"""
gps_producer.py — Producteur GPS simulé — TaaSim
=================================================
Rejoue les polylines du dataset casablanca_dataset.parquet vers raw.gps.

Conformité cahier des charges :
    ✅ Vitesse configurable (ex : 10×)
    ✅ Bruit gaussien σ=0.0002° (~20 m)
    ✅ Blackout GPS 5% probabilité → délai 60-180 s par événement
    ✅ Clé Kafka = taxi_id (partitionnement cohérent par véhicule)
    ✅ Payload complet : taxi_id, timestamp, lat, lon, speed, status
    ✅ Diagnostics de démarrage complets
    ✅ Protection contre cycles vides

Usage :
    python gps_producer.py
    python gps_producer.py --speed 5.0 --max-trips 200
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
from typing import Any, Dict, Generator

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
logger = logging.getLogger("taasim.gps_producer")


# ---------------------------------------------------------------------------
# Validation polyline
# ---------------------------------------------------------------------------

def is_valid_polyline(p) -> bool:
    """Accepte list, numpy.ndarray ou tout itérable de longueur >= 2."""
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
        logger.error(f"Colonne '{config.POLYLINE_COLUMN}' absente.")
        sys.exit(1)

    sample = df[config.POLYLINE_COLUMN].dropna().iloc[0] if len(df) > 0 else None
    logger.info(f"[DIAG] Type polyline       : {type(sample)}")
    if sample is not None and hasattr(sample, "__len__") and len(sample) > 0:
        logger.info(f"[DIAG] Type d'un point     : {type(sample[0])}")
        logger.info(f"[DIAG] Exemple points[0:2] : {[list(p) for p in list(sample[:2])]}")

    before = len(df)
    df = df[df[config.POLYLINE_COLUMN].apply(is_valid_polyline)].reset_index(drop=True)
    logger.info(f"{len(df)} trajets valides ({before - len(df)} ignorés).")

    if len(df) == 0:
        logger.error(
            "ERREUR CRITIQUE : 0 trajets valides après filtrage. "
            f"Type polyline détecté : {type(sample)}."
        )
        sys.exit(1)

    return df


# ---------------------------------------------------------------------------
# Blackout GPS
# ---------------------------------------------------------------------------

def maybe_blackout_delay() -> float:
    """
    Avec une probabilité GPS_BLACKOUT_PROBABILITY (5%), simule une coupure GPS
    en retournant un délai entre 60 et 180 secondes (divisé par la vitesse).
    Sinon retourne 0.

    Pourquoi : dans la réalité, les taxis perdent parfois le signal GPS
    (tunnel, garage, zone dense). On simule ce comportement pour tester
    la robustesse des consommateurs Kafka.
    """
    if random.random() < config.GPS_BLACKOUT_PROBABILITY:
        raw = random.uniform(
            config.GPS_BLACKOUT_MIN_SECONDS,
            config.GPS_BLACKOUT_MAX_SECONDS,
        )
        return raw / config.SIMULATION_SPEED_FACTOR
    return 0.0


# ---------------------------------------------------------------------------
# Vitesse instantanée
# ---------------------------------------------------------------------------

def compute_speed_kmh(
    prev_lat: float, prev_lon: float,
    curr_lat: float, curr_lon: float,
    interval_s: float,
) -> float:
    """
    Calcule la vitesse en km/h entre deux points GPS consécutifs
    via la formule de Haversine.

    Pourquoi : le dataset ne fournit pas la vitesse directement.
    On la déduit de la distance entre deux points et du temps entre
    deux captures (GPS_INTERVAL_SECONDS = 15 s dans le dataset Porto).
    """
    R = 6371.0
    phi1, phi2 = math.radians(prev_lat), math.radians(curr_lat)
    dphi = math.radians(curr_lat - prev_lat)
    dlam = math.radians(curr_lon - prev_lon)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    dist_km = R * 2 * math.asin(math.sqrt(max(0.0, a)))
    if interval_s <= 0:
        return 0.0
    return round((dist_km / interval_s) * 3600, 1)


# ---------------------------------------------------------------------------
# Producteur Kafka
# ---------------------------------------------------------------------------

def create_kafka_producer() -> KafkaProducer:
    """
    key_serializer encode le taxi_id en bytes UTF-8.
    Kafka utilise cette clé pour partitionner → tous les points
    d'un même taxi arrivent dans la même partition, dans l'ordre.
    Sans clé, les points d'un taxi seraient dispersés sur 3 partitions
    et perdre leur ordre temporel.
    """
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


# ---------------------------------------------------------------------------
# Construction de l'événement GPS
# ---------------------------------------------------------------------------

def build_gps_event(
    row: pd.Series,
    vehicle_id: str,
    lon: float,
    lat: float,
    point_index: int,
    speed_kmh: float,
    status: str,
) -> Dict[str, Any]:
    """
    Payload GPS selon le cahier des charges :
      - taxi_id    : identifiant du véhicule (clé de partitionnement Kafka)
      - timestamp  : heure réelle de l'événement (event time)
      - lat / lon  : coordonnées avec bruit gaussien appliqué
      - speed      : vitesse estimée en km/h (calculée entre deux points)
      - status     : "moving" | "idle" | "blackout_recovered"

    Champs supplémentaires pour le traitement aval (Spark, Flink...) :
      origin_zone, destination_zone, call_type, day_type, etc.
    """
    noisy_lat = lat + np.random.normal(0, config.GPS_NOISE_STD_DEG) if config.ADD_GPS_NOISE else lat
    noisy_lon = lon + np.random.normal(0, config.GPS_NOISE_STD_DEG) if config.ADD_GPS_NOISE else lon

    return {
        # Champs requis cahier des charges
        "taxi_id":   int(row["TAXI_ID"]),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "lat":       round(noisy_lat, 7),
        "lon":       round(noisy_lon, 7),
        "speed":     speed_kmh,
        "status":    status,

        # Métadonnées trajet
        "event_id":         str(uuid.uuid4()),
        "event_type":       "gps_position",
        "trip_id":          int(row["trip_id"]),
        "vehicle_id":       vehicle_id,
        "point_index":      point_index,
        "origin_zone":      int(row["origin_zone"]),
        "destination_zone": int(row["destination_zone"]),
        "call_type":        str(row["CALL_TYPE"]),
        "day_type":         str(row["DAY_TYPE"]),
        "simulation":       True,
    }


# ---------------------------------------------------------------------------
# Générateur de points GPS
# ---------------------------------------------------------------------------

def gps_point_generator(df: pd.DataFrame, max_trips: int) -> Generator:
    """
    Pour chaque trajet, itère sur les points de la polyline et produit
    (kafka_key, event, delay).

    La clé Kafka est le taxi_id (string) pour garantir que tous les
    points d'un même taxi arrivent dans la même partition Kafka.
    """
    indices = df.index.tolist()
    random.shuffle(indices)
    if max_trips > 0:
        indices = indices[:max_trips]

    logger.info(f"[GPS] Trajets retenus : {len(indices)} (max_trips={max_trips})")

    base_delay = config.GPS_INTERVAL_SECONDS / config.SIMULATION_SPEED_FACTOR

    for idx in indices:
        row = df.loc[idx]
        polyline = row[config.POLYLINE_COLUMN]

        # Normalisation numpy.ndarray → list Python
        if not isinstance(polyline, list):
            try:
                polyline = [list(p) for p in polyline]
            except Exception as e:
                logger.warning(f"Trajet {row['trip_id']} — polyline invalide : {e}")
                continue

        vehicle_id = (
            f"{config.VEHICLE_ID_PREFIX}"
            f"-{random.randint(1, config.NUM_SIMULATED_VEHICLES):03d}"
        )

        prev_lat, prev_lon = None, None

        for point_index, point in enumerate(polyline):
            lon, lat = float(point[0]), float(point[1])

            # Calcul vitesse (0 pour le premier point du trajet)
            speed_kmh = (
                compute_speed_kmh(prev_lat, prev_lon, lat, lon, config.GPS_INTERVAL_SECONDS)
                if prev_lat is not None else 0.0
            )

            status = "moving" if speed_kmh > 2.0 else "idle"

            # Blackout : délai simulé + marquage de l'événement suivant
            blackout = maybe_blackout_delay()
            if blackout > 0:
                logger.debug(
                    f"[BLACKOUT] taxi={row['TAXI_ID']} point={point_index} "
                    f"délai={blackout * config.SIMULATION_SPEED_FACTOR:.0f}s simulées"
                )
                time.sleep(blackout)
                status = "blackout_recovered"

            event = build_gps_event(
                row, vehicle_id, lon, lat, point_index, speed_kmh, status
            )

            # Délai entre deux points avec jitter temporel optionnel
            delay = base_delay
            if config.ADD_TEMPORAL_JITTER:
                jitter = random.uniform(
                    -config.TEMPORAL_JITTER_MAX_SECONDS,
                    config.TEMPORAL_JITTER_MAX_SECONDS,
                ) / config.SIMULATION_SPEED_FACTOR
                delay = max(0.01, delay + jitter)

            prev_lat, prev_lon = lat, lon
            yield str(row["TAXI_ID"]), event, delay


# ---------------------------------------------------------------------------
# Boucle principale
# ---------------------------------------------------------------------------

def run_simulation(producer: KafkaProducer, df: pd.DataFrame, max_trips: int) -> None:
    if len(df) == 0:
        logger.error("DataFrame vide. Abandon.")
        sys.exit(1)
    if max_trips <= 0:
        raise ValueError(f"max_trips doit être > 0, reçu : {max_trips}")

    total_sent      = 0
    total_errors    = 0
    total_blackouts = 0
    last_stats      = time.time()
    cycle           = 0

    while True:
        cycle += 1
        logger.info(
            f"Cycle GPS #{cycle} — "
            f"dataset : {len(df)} | max_trips : {max_trips} | "
            f"ce cycle : {min(max_trips, len(df))}"
        )

        for kafka_key, event, delay in gps_point_generator(df, max_trips):
            if event.get("status") == "blackout_recovered":
                total_blackouts += 1

            try:
                producer.send(
                    config.KAFKA_TOPIC_GPS,
                    key=kafka_key,
                    value=event,
                ).get(timeout=10)
                total_sent += 1
            except KafkaError as e:
                logger.error(f"Erreur Kafka : {e}")
                total_errors += 1

            if time.time() - last_stats >= config.STATS_INTERVAL_SECONDS:
                logger.info(
                    f"[GPS] Envoyés : {total_sent} | Erreurs : {total_errors} | "
                    f"Blackouts : {total_blackouts} | Topic : {config.KAFKA_TOPIC_GPS}"
                )
                last_stats = time.time()

            time.sleep(delay)

        logger.info(f"Cycle GPS #{cycle} terminé — total envoyés : {total_sent}")
        if not config.LOOP_INFINITE:
            break

    producer.flush()
    logger.info(
        f"Producteur GPS arrêté — {total_sent} envoyés | "
        f"{total_errors} erreurs | {total_blackouts} blackouts simulés"
    )


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="Producteur GPS TaaSim.")
    parser.add_argument("--speed", type=float, default=None,
                        help="Facteur d'accélération (écrase config.py)")
    parser.add_argument("--max-trips", type=int, default=config.MAX_TRIPS_PER_CYCLE,
                        help=f"Trajets max par cycle (défaut : {config.MAX_TRIPS_PER_CYCLE})")
    return parser.parse_args()


def main():
    args = parse_args()

    logger.info(f"[DIAG] Script        : {os.path.abspath(__file__)}")
    logger.info(f"[DIAG] config.py     : {os.path.abspath(config.__file__)}")
    logger.info(f"[DIAG] Parquet       : {config.TRIPS_PARQUET_PATH}")
    logger.info(f"[DIAG] max_trips     : {args.max_trips}")
    logger.info(f"[DIAG] speed         : {config.SIMULATION_SPEED_FACTOR}x")
    logger.info(f"[DIAG] GPS noise σ   : {config.GPS_NOISE_STD_DEG}° "
                f"(~{config.GPS_NOISE_STD_DEG * 111000:.0f} m)")
    logger.info(f"[DIAG] Blackout prob : {config.GPS_BLACKOUT_PROBABILITY * 100:.0f}%")

    if args.max_trips <= 0:
        logger.error(f"--max-trips doit être > 0, reçu : {args.max_trips}")
        sys.exit(1)

    if args.speed is not None:
        config.SIMULATION_SPEED_FACTOR = args.speed

    df       = load_dataset(config.TRIPS_PARQUET_PATH)
    producer = create_kafka_producer()

    logger.info(
        f"Démarrage GPS | vitesse={config.SIMULATION_SPEED_FACTOR}x | "
        f"topic={config.KAFKA_TOPIC_GPS} | trajets={len(df)} | cycle={args.max_trips}"
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
