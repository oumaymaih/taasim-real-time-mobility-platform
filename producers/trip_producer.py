"""
trip_producer.py — Producteur de demandes de trajets — TaaSim
==============================================================
Génère des événements de réservation vers raw.trips en suivant
la courbe de demande horaire réelle du dataset Porto/Casablanca.

Conformité cahier des charges :
    ✅ Courbe de demande horaire (aggregate par heure → normalize)
    ✅ Heures de pointe 7-9h / 17-19h → 3-5× le taux nominal
    ✅ Vendredi 12-14h → taux réduit (×0.5)
    ✅ Payload complet : trip_id (UUID), rider_id, origin_zone,
                        destination_zone, requested_at, call_type
    ✅ Diagnostics de démarrage complets
    ✅ Protection contre cycles vides

Usage :
    python trip_producer.py
    python trip_producer.py --speed 5.0 --max-trips 200
"""

import argparse
import json
import logging
import math
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

import config

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("taasim.trip_producer")


# ---------------------------------------------------------------------------
# Validation polyline
# ---------------------------------------------------------------------------

def is_valid_polyline(p) -> bool:
    try:
        return hasattr(p, "__len__") and len(p) >= 2
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Chargement dataset
# ---------------------------------------------------------------------------

def load_dataset(parquet_dir: str) -> pd.DataFrame:
    logger.info(f"Chargement du dataset depuis : {parquet_dir}")
    try:
        df = pd.read_parquet(parquet_dir)
    except FileNotFoundError:
        logger.error(f"Dossier Parquet introuvable : {parquet_dir}")
        sys.exit(1)

    logger.info(f"Dataset chargé : {len(df)} trajets | colonnes : {df.columns.tolist()}")

    if config.POLYLINE_COLUMN not in df.columns:
        logger.error(f"Colonne '{config.POLYLINE_COLUMN}' introuvable.")
        sys.exit(1)

    sample = df[config.POLYLINE_COLUMN].dropna().iloc[0] if len(df) > 0 else None
    logger.info(f"[DIAG] Type polyline : {type(sample)}")

    before = len(df)
    df = df[df[config.POLYLINE_COLUMN].apply(is_valid_polyline)].reset_index(drop=True)
    logger.info(f"{len(df)} trajets valides ({before - len(df)} ignorés).")

    if len(df) == 0:
        logger.error("ERREUR CRITIQUE : 0 trajets valides après filtrage.")
        sys.exit(1)

    return df


# ---------------------------------------------------------------------------
# Courbe de demande horaire
# ---------------------------------------------------------------------------

def get_demand_multiplier() -> float:
    """
    Retourne le multiplicateur de demande pour l'heure actuelle.

    Logique :
      1. On lit l'heure locale actuelle (0-23).
      2. On lit le multiplier de base dans DEMAND_CURVE_HOURLY.
         Ex : 08h → 2.5 (heure de pointe matin)
      3. Si c'est vendredi et entre 12h-14h, on applique en plus
         FRIDAY_LUNCH_REDUCTION (×0.5) → taux réduit prière du vendredi.

    Exemple de valeurs résultantes :
      - 08h lundi   → 2.5
      - 18h mardi   → 2.8
      - 13h vendredi → 0.6 × 0.5 = 0.30
      - 03h dimanche → 0.10
    """
    now = datetime.now()
    hour = now.hour
    multiplier = config.DEMAND_CURVE_HOURLY[hour]

    # Réduction vendredi midi (vendredi = weekday() == 4)
    fri_start, fri_end = config.FRIDAY_LUNCH_HOURS
    if now.weekday() == 4 and fri_start <= hour < fri_end:
        multiplier *= config.FRIDAY_LUNCH_REDUCTION
        logger.debug(
            f"[DEMANDE] Vendredi {hour}h → réduction ×{config.FRIDAY_LUNCH_REDUCTION} "
            f"→ multiplier={multiplier:.2f}"
        )

    return multiplier


def compute_inter_trip_delay(base_interval_s: float, speed: float) -> float:
    """
    Calcule le délai entre deux demandes de trajet en tenant compte
    de la courbe de demande horaire.

    Formule :
        délai = (base_interval / speed) / demand_multiplier

    Un multiplier de 3.0 → le délai est divisé par 3 → 3× plus de demandes.
    Un multiplier de 0.3 → le délai est multiplié par ~3.3 → moins de demandes.
    """
    multiplier = get_demand_multiplier()
    if multiplier <= 0:
        multiplier = 0.01  # sécurité division par zéro
    delay = (base_interval_s / speed) / multiplier
    return max(0.05, delay)


# ---------------------------------------------------------------------------
# Géographie
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin(math.radians(lat2 - lat1) / 2) ** 2
        + math.cos(phi1) * math.cos(phi2)
        * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(max(0.0, a)))


def estimate_duration_min(distance_km: float, avg_speed_kmh: float = 25.0) -> float:
    return max(2.0, (distance_km / avg_speed_kmh) * 60.0) if distance_km > 0 else 5.0


def estimate_fare(distance_km: float) -> float:
    fare = config.BASE_FARE_MAD + distance_km * config.PRICE_PER_KM_MAD
    return round(fare * random.uniform(0.90, 1.10), 2)


# ---------------------------------------------------------------------------
# Construction des événements
# ---------------------------------------------------------------------------

def build_trip_request(row: pd.Series) -> Dict[str, Any]:
    """
    Payload du cahier des charges :
      - trip_id      : UUID unique de cette réservation
      - rider_id     : identifiant simulé du passager
      - origin_zone  : arrondissement de départ (du dataset)
      - destination_zone : arrondissement d'arrivée (du dataset)
      - requested_at : heure réelle de la demande (event time)
      - call_type    : A/B/C du dataset Porto
    """
    polyline = row[config.POLYLINE_COLUMN]

    # Normalisation numpy → list
    if not isinstance(polyline, list):
        try:
            polyline = [list(p) for p in polyline]
        except Exception as e:
            raise ValueError(f"Polyline invalide trajet {row['trip_id']} : {e}")

    origin_lon, origin_lat = float(polyline[0][0]),  float(polyline[0][1])
    dest_lon,   dest_lat   = float(polyline[-1][0]), float(polyline[-1][1])

    distance_km  = haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
    duration_min = estimate_duration_min(distance_km)
    fare         = estimate_fare(distance_km)

    payment_type = random.choices(
        config.PAYMENT_TYPES,
        weights=config.PAYMENT_DISTRIBUTION,
        k=1,
    )[0]

    num_passengers = random.randint(
        config.PASSENGER_COUNT_MIN,
        config.PASSENGER_COUNT_MAX,
    )

    # trip_id = UUID unique pour cette réservation (pas le trip_id du dataset)
    trip_uuid  = str(uuid.uuid4())

    # rider_id simulé : RIDER-XXXXX
    rider_id   = f"RIDER-{random.randint(1, 99999):05d}"

    vehicle_id = (
        f"{config.VEHICLE_ID_PREFIX}"
        f"-{random.randint(1, config.NUM_SIMULATED_VEHICLES):03d}"
    )

    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        # Champs requis cahier des charges
        "trip_id":            trip_uuid,           # UUID unique par réservation
        "rider_id":           rider_id,            # passager simulé
        "origin_zone":        int(row["origin_zone"]),
        "destination_zone":   int(row["destination_zone"]),
        "requested_at":       datetime.now(timezone.utc).isoformat(),  # event time
        "timestamp_utc":      now_utc,
        "call_type":          str(row["CALL_TYPE"]),  # A/B/C du dataset

        # Champs supplémentaires
        "event_id":           str(uuid.uuid4()),
        "event_type":         "trip_request",
        "dataset_trip_id":    int(row["trip_id"]),   # id original du dataset
        "taxi_id":            int(row["TAXI_ID"]),
        "day_type":           str(row["DAY_TYPE"]),
        "vehicle_id":         vehicle_id,
        "passenger_count":    num_passengers,
        "origin": {
            "lat": round(origin_lat, 7),
            "lon": round(origin_lon, 7),
        },
        "destination": {
            "lat": round(dest_lat, 7),
            "lon": round(dest_lon, 7),
        },
        "estimated_distance_km":  round(distance_km, 3),
        "estimated_duration_min": round(duration_min, 1),
        "estimated_fare_mad":     fare,
        "payment_type":           payment_type,
        "num_gps_points":         len(polyline),
        "status":                 "requested",
        "demand_multiplier":      get_demand_multiplier(),  # pour debug/monitoring
        "simulation":             True,
    }


def build_status_update(request: Dict[str, Any], new_status: str) -> Dict[str, Any]:
    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      "trip_status_update",
        "trip_id":         request["trip_id"],
        "rider_id":        request["rider_id"],
        "vehicle_id":      request["vehicle_id"],
        "origin_zone":     request["origin_zone"],
        "destination_zone": request["destination_zone"],
        "status":          new_status,
        "timestamp_utc":   datetime.now(timezone.utc).isoformat(),
        "simulation":      True,
    }


# ---------------------------------------------------------------------------
# Producteur Kafka
# ---------------------------------------------------------------------------

def create_kafka_producer() -> KafkaProducer:
    for attempt in range(1, config.KAFKA_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                linger_ms=config.KAFKA_LINGER_MS,
                batch_size=config.KAFKA_BATCH_SIZE,
                compression_type=config.KAFKA_COMPRESSION_TYPE,
                retries=3,
            )
            logger.info(f"Kafka connecté : {config.KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            logger.warning(f"Tentative {attempt}/{config.KAFKA_RETRIES} : {e}")
            time.sleep(2 ** attempt)

    logger.error("Impossible de se connecter à Kafka.")
    sys.exit(1)


def kafka_send(producer: KafkaProducer, topic: str, event: Dict[str, Any],
               key: str = None) -> bool:
    try:
        producer.send(topic, key=key, value=event).get(timeout=10)
        return True
    except KafkaError as e:
        logger.error(f"Erreur Kafka (topic={topic}) : {e}")
        return False


# ---------------------------------------------------------------------------
# Boucle de simulation
# ---------------------------------------------------------------------------

def run_simulation(producer: KafkaProducer, df: pd.DataFrame, max_trips: int) -> None:
    if len(df) == 0:
        logger.error("DataFrame vide. Abandon.")
        sys.exit(1)
    if max_trips <= 0:
        raise ValueError(f"max_trips doit être > 0, reçu : {max_trips}")

    total_sent   = 0
    total_errors = 0
    last_stats   = time.time()
    speed        = config.SIMULATION_SPEED_FACTOR
    cycle        = 0

    while True:
        cycle += 1
        sample = df.sample(frac=1).reset_index(drop=True).iloc[:max_trips]

        hour = datetime.now().hour
        multiplier = get_demand_multiplier()
        logger.info(
            f"Cycle trajets #{cycle} — "
            f"dataset : {len(df)} | max_trips : {max_trips} | "
            f"ce cycle : {len(sample)} | heure : {hour}h | "
            f"multiplier demande : {multiplier:.2f}x"
        )

        if len(sample) == 0:
            logger.error("Cycle vide. Arrêt.")
            break

        for _, row in sample.iterrows():
            try:
                request = build_trip_request(row)
            except (ValueError, IndexError, KeyError) as e:
                logger.warning(f"Trajet ignoré : {e}")
                continue

            if request["estimated_duration_min"] > config.MAX_TRIP_DURATION_MINUTES:
                logger.debug(f"Trajet {request['dataset_trip_id']} ignoré (trop long).")
                continue

            # Clé = origin_zone pour regrouper les demandes par zone
            key = str(request["origin_zone"])

            # 1. Demande
            if kafka_send(producer, config.KAFKA_TOPIC_TRIPS, request, key=key):
                total_sent += 1
                logger.debug(
                    f"[REQUEST] rider={request['rider_id']} | "
                    f"{request['estimated_distance_km']} km | "
                    f"{request['estimated_fare_mad']} MAD | "
                    f"call_type={request['call_type']}"
                )
            else:
                total_errors += 1

            time.sleep(random.uniform(2, 8) / speed)

            # 2. Accepté
            if kafka_send(producer, config.KAFKA_TOPIC_TRIP_STATUS,
                          build_status_update(request, "accepted"), key=key):
                total_sent += 1

            time.sleep(random.uniform(1, 3) / speed)

            # 3. En cours
            if kafka_send(producer, config.KAFKA_TOPIC_TRIP_STATUS,
                          build_status_update(request, "in_progress"), key=key):
                total_sent += 1

            time.sleep((request["estimated_duration_min"] * 60) / speed)

            # 4. Terminé
            if kafka_send(producer, config.KAFKA_TOPIC_TRIP_STATUS,
                          build_status_update(request, "completed"), key=key):
                total_sent += 1
                logger.debug(f"[COMPLETED] rider={request['rider_id']}")

            if time.time() - last_stats >= config.STATS_INTERVAL_SECONDS:
                logger.info(
                    f"[TRIPS] Envoyés : {total_sent} | Erreurs : {total_errors} | "
                    f"Multiplier : {get_demand_multiplier():.2f}x"
                )
                last_stats = time.time()

            # Délai entre deux demandes modulé par la courbe horaire
            inter_delay = compute_inter_trip_delay(
                config.TRIP_REQUEST_INTERVAL_SECONDS, speed
            )
            time.sleep(inter_delay)

        logger.info(f"Cycle trajets #{cycle} terminé — total envoyés : {total_sent}")
        if not config.LOOP_INFINITE:
            break

    producer.flush()
    logger.info(f"Producteur trajets arrêté — {total_sent} envoyés | {total_errors} erreurs.")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Producteur trajets TaaSim.")
    parser.add_argument("--speed", type=float, default=None)
    parser.add_argument("--max-trips", type=int, default=config.MAX_TRIPS_PER_CYCLE)
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(f"[DIAG] Script    : {os.path.abspath(__file__)}")
    logger.info(f"[DIAG] config.py : {os.path.abspath(config.__file__)}")
    logger.info(f"[DIAG] Parquet   : {config.TRIPS_PARQUET_PATH}")
    logger.info(f"[DIAG] max_trips : {args.max_trips}")
    logger.info(f"[DIAG] speed     : {config.SIMULATION_SPEED_FACTOR}x")

    # Affichage de la courbe de demande au démarrage
    logger.info("[DIAG] Courbe de demande horaire :")
    for h, m in enumerate(config.DEMAND_CURVE_HOURLY):
        bar = "█" * int(m * 10)
        logger.info(f"  {h:02d}h : {m:.1f}x {bar}")

    if args.max_trips <= 0:
        logger.error(f"--max-trips doit être > 0, reçu : {args.max_trips}")
        sys.exit(1)

    if args.speed is not None:
        config.SIMULATION_SPEED_FACTOR = args.speed

    df       = load_dataset(config.TRIPS_PARQUET_PATH)
    producer = create_kafka_producer()

    logger.info(
        f"Démarrage trajets | vitesse={config.SIMULATION_SPEED_FACTOR}x | "
        f"topics={config.KAFKA_TOPIC_TRIPS},{config.KAFKA_TOPIC_TRIP_STATUS} | "
        f"trajets={len(df)} | cycle={args.max_trips}"
    )

    try:
        run_simulation(producer, df, args.max_trips)
    except KeyboardInterrupt:
        logger.info("Arrêt (Ctrl+C).")
    except ValueError as e:
        logger.error(f"Config : {e}")
        sys.exit(1)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
