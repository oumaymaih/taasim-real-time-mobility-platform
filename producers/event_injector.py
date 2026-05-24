"""
event_injector.py — Injecteur d'anomalies pour démo live — TaaSim
==================================================================
Script standalone qui publie des événements synthétiques d'anomalie
directement sur les mêmes topics Kafka que les producteurs normaux.

3 types d'anomalies supportées :

  1. SPIKE  — Demande soudaine dans une zone (ex: stade, gare)
              Multiplie le taux d'émission de trip_requests par un
              facteur configurable (défaut ×3) pendant N secondes.

  2. BLACKOUT — Coupure GPS pour un ensemble de véhicules
                Envoie des événements GPS avec status="blackout" pour
                les taxis ciblés pendant N secondes.

  3. RAIN   — Pluie : augmentation globale des demandes ×1.4
              Injecte des trip_requests supplémentaires pendant N secondes.

Usage :
    # Spike de demande dans la zone 3, facteur ×3, pendant 5 minutes
    python event_injector.py spike --zone 3 --factor 3.0 --duration 300

    # Blackout GPS pour les taxis 12, 47, 88 pendant 2 minutes
    python event_injector.py blackout --taxis 12,47,88 --duration 120

    # Événement pluie global pendant 10 minutes
    python event_injector.py rain --duration 600

    # Enchaîner plusieurs événements
    python event_injector.py spike --zone 5 --factor 5.0 --duration 120
    python event_injector.py rain --duration 300
"""

import argparse
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
logger = logging.getLogger("taasim.event_injector")


# ---------------------------------------------------------------------------
# Coordonnées des zones de Casablanca (pour les événements synthétiques)
# Zone = arrondissement, coordonnées centrales approximatives
# ---------------------------------------------------------------------------

ZONE_COORDS = {
    1:  (33.5893, -7.6114),   # Ain Chock
    2:  (33.5731, -7.5898),   # Ain Sebaa
    3:  (33.5950, -7.6311),   # Al Fida
    4:  (33.5800, -7.6200),   # Ben M'Sick
    5:  (33.6050, -7.5500),   # Hay Hassani
    6:  (33.5480, -7.6830),   # Hay Mohammadi
    7:  (33.5200, -7.6100),   # Nouaceur
    8:  (33.6150, -7.5750),   # Sidi Bernoussi
    9:  (33.5650, -7.6550),   # Sidi Moumen
    10: (33.5900, -7.6600),   # Centre-ville / Maarif
}

# Zones par défaut si la zone demandée n'est pas dans le dictionnaire
DEFAULT_ZONE_LAT = 33.5731
DEFAULT_ZONE_LON = -7.5898


def get_zone_coords(zone_id: int):
    return ZONE_COORDS.get(zone_id, (DEFAULT_ZONE_LAT, DEFAULT_ZONE_LON))


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
# ANOMALIE 1 : SPIKE DE DEMANDE
# ---------------------------------------------------------------------------

def build_spike_trip_request(zone_id: int) -> Dict[str, Any]:
    """
    Construit un trip_request synthétique centré sur la zone du spike.

    Les coordonnées sont générées autour du centroïde de la zone avec
    un rayon aléatoire de ~1 km pour simuler des demandes dispersées
    (ex: spectateurs quittant un stade).
    """
    lat, lon = get_zone_coords(zone_id)

    # Rayon ~1 km en degrés (~0.009°)
    origin_lat = lat + random.uniform(-0.009, 0.009)
    origin_lon = lon + random.uniform(-0.009, 0.009)

    # Destination aléatoire dans Casablanca
    dest_zone = random.choice(list(ZONE_COORDS.keys()))
    dest_lat, dest_lon = get_zone_coords(dest_zone)
    dest_lat += random.uniform(-0.005, 0.005)
    dest_lon += random.uniform(-0.005, 0.005)

    rider_id = f"RIDER-SPIKE-{random.randint(1, 99999):05d}"
    trip_id  = str(uuid.uuid4())
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        # Champs requis cahier des charges
        "trip_id":          trip_id,
        "rider_id":         rider_id,
        "origin_zone":      zone_id,
        "destination_zone": dest_zone,
        "requested_at": now_utc,
        "timestamp_utc": now_utc,
        "requested_at":     datetime.now(timezone.utc).isoformat(),
        "call_type":        random.choice(["A", "B", "C"]),

        # Métadonnées anomalie
        "event_id":         str(uuid.uuid4()),
        "event_type":       "trip_request",
        "anomaly_type":     "demand_spike",
        "origin": {
            "lat": round(origin_lat, 7),
            "lon": round(origin_lon, 7),
        },
        "destination": {
            "lat": round(dest_lat, 7),
            "lon": round(dest_lon, 7),
        },
        "estimated_distance_km":  round(random.uniform(2, 15), 2),
        "estimated_duration_min": round(random.uniform(5, 40), 1),
        "estimated_fare_mad":     round(random.uniform(15, 80), 2),
        "payment_type":           random.choices(
            config.PAYMENT_TYPES, weights=config.PAYMENT_DISTRIBUTION
        )[0],
        "passenger_count": random.randint(1, 4),
        "status":          "requested",
        "simulation":      True,
        "injected":        True,
    }


def run_spike(
    producer: KafkaProducer,
    zone_id: int,
    factor: float,
    duration_s: float,
) -> None:
    """
    Injecte un spike de demande sur la zone `zone_id` pendant `duration_s` secondes.

    Comment ça marche :
      - En régime normal, un trip_request est émis toutes ~30 s (config de base).
      - Avec factor=3.0, on divise ce délai par 3 → un événement toutes ~10 s.
      - Les événements sont publiés sur raw.trips avec anomaly_type="demand_spike".
      - Les consommateurs peuvent détecter ce spike via le champ anomaly_type
        ou en mesurant le débit sur la zone.

    Exemple réel : sortie de stade Mohamed V après un match → 50 000 personnes
    cherchent un taxi en même temps → les demandes explosent dans la zone 3.
    """
    logger.info(
        f"[SPIKE] Démarrage | zone={zone_id} | facteur=×{factor} | "
        f"durée={duration_s:.0f}s | topic={config.KAFKA_TOPIC_TRIPS}"
    )

    # Délai de base en secondes réelles (pas de facteur d'accélération ici,
    # l'injecteur tourne en temps réel pour la démo)
    base_delay = config.TRIP_REQUEST_INTERVAL_SECONDS / factor
    end_time   = time.time() + duration_s
    sent       = 0

    while time.time() < end_time:
        event = build_spike_trip_request(zone_id)
        key   = str(zone_id)

        if kafka_send(producer, config.KAFKA_TOPIC_TRIPS, event, key=key):
            sent += 1
            remaining = end_time - time.time()
            logger.info(
                f"[SPIKE] Envoyé #{sent} | zone={zone_id} | "
                f"rider={event['rider_id']} | restant={remaining:.0f}s"
            )
        else:
            logger.warning("[SPIKE] Échec envoi Kafka.")

        time.sleep(base_delay)

    logger.info(f"[SPIKE] Terminé — {sent} événements injectés sur zone {zone_id}.")


# ---------------------------------------------------------------------------
# ANOMALIE 2 : BLACKOUT GPS
# ---------------------------------------------------------------------------

def build_blackout_gps_event(taxi_id: int) -> Dict[str, Any]:
    """
    Construit un événement GPS avec status="blackout" pour un taxi donné.

    Cet événement signale que le véhicule a perdu le signal GPS.
    Le consommateur peut l'utiliser pour :
      - marquer le véhicule comme "hors ligne" dans le dashboard
      - déclencher une alerte si le blackout dure trop longtemps
      - tester la robustesse du pipeline face aux données manquantes

    Les coordonnées sont (0, 0) car inconnues pendant le blackout.
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        # Champs requis cahier des charges
        "taxi_id":   taxi_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat":       0.0,    # coordonnées inconnues pendant blackout
        "lon":       0.0,
        "speed":     0.0,
        "timestamp": now_utc,
        "timestamp_utc": now_utc,
        "status":    "blackout",

        # Métadonnées
        "event_id":     str(uuid.uuid4()),
        "event_type":   "gps_position",
        "anomaly_type": "gps_blackout",
        "vehicle_id":   f"{config.VEHICLE_ID_PREFIX}-{taxi_id:03d}",
        "simulation":   True,
        "injected":     True,
    }


def build_blackout_recovered_event(taxi_id: int, lat: float, lon: float) -> Dict[str, Any]:
    """Événement de retour du signal GPS après le blackout."""
    now_utc = datetime.now(timezone.utc).isoformat()

    return {
        "taxi_id":   taxi_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat":       lat,
        "lon":       lon,
        "speed":     0.0,
        "status":    "blackout_recovered",
        "event_id":     str(uuid.uuid4()),
        "event_type":   "gps_position",
        "anomaly_type": "gps_blackout_recovered",
        "vehicle_id":   f"{config.VEHICLE_ID_PREFIX}-{taxi_id:03d}",
        "timestamp": now_utc,
        "timestamp_utc": now_utc,
        "simulation":   True,
        "injected":     True,
    }


def run_blackout(
    producer: KafkaProducer,
    taxi_ids: List[int],
    duration_s: float,
) -> None:
    """
    Simule une coupure GPS pour les taxis listés pendant `duration_s` secondes.

    Phase 1 (pendant duration_s) :
      - Envoie un événement "blackout" toutes les 5 secondes pour chaque taxi.
      - status="blackout", lat=0, lon=0.

    Phase 2 (fin du blackout) :
      - Envoie un événement "blackout_recovered" avec des coordonnées
        aléatoires dans Casablanca pour simuler le retour du signal.

    Exemple réel : un taxi entre dans le parking souterrain de Morocco Mall
    → perd le GPS pendant 2 minutes.
    """
    logger.info(
        f"[BLACKOUT] Démarrage | taxis={taxi_ids} | durée={duration_s:.0f}s | "
        f"topic={config.KAFKA_TOPIC_GPS}"
    )

    end_time  = time.time() + duration_s
    ping_interval = 5.0  # envoyer un événement blackout toutes les 5 s
    total_sent = 0

    while time.time() < end_time:
        for taxi_id in taxi_ids:
            event = build_blackout_gps_event(taxi_id)
            key   = str(taxi_id)
            if kafka_send(producer, config.KAFKA_TOPIC_GPS, event, key=str(taxi_id)):
                total_sent += 1
                logger.info(
                    f"[BLACKOUT] taxi={taxi_id} | status=blackout | "
                    f"restant={end_time - time.time():.0f}s"
                )
        time.sleep(ping_interval)

    # Fin du blackout : envoyer un événement "recovered" pour chaque taxi
    logger.info("[BLACKOUT] Fin — envoi des événements de récupération.")
    for taxi_id in taxi_ids:
        # Coordonnées de retour aléatoires dans Casablanca
        lat = random.uniform(33.50, 33.65)
        lon = random.uniform(-7.70, -7.55)
        event = build_blackout_recovered_event(taxi_id, lat, lon)
        if kafka_send(producer, config.KAFKA_TOPIC_GPS, str(taxi_id), event):
            total_sent += 1
        logger.info(f"[BLACKOUT] taxi={taxi_id} | status=blackout_recovered")

    logger.info(f"[BLACKOUT] Terminé — {total_sent} événements injectés.")


# ---------------------------------------------------------------------------
# ANOMALIE 3 : RAIN EVENT
# ---------------------------------------------------------------------------

def build_rain_trip_request() -> Dict[str, Any]:
    """
    Construit un trip_request supplémentaire lié à la pluie.

    Les demandes liées à la pluie sont réparties uniformément dans Casablanca
    (pas concentrées sur une zone) car la pluie affecte toute la ville.
    Le taux d'émission est multiplié par INJECTOR_RAIN_FACTOR (1.4).
    """
    zone_id  = random.choice(list(ZONE_COORDS.keys()))
    lat, lon = get_zone_coords(zone_id)

    origin_lat = lat + random.uniform(-0.02, 0.02)
    origin_lon = lon + random.uniform(-0.02, 0.02)

    dest_zone   = random.choice(list(ZONE_COORDS.keys()))
    dest_lat, dest_lon = get_zone_coords(dest_zone)
    dest_lat += random.uniform(-0.01, 0.01)
    dest_lon += random.uniform(-0.01, 0.01)
    now_utc = datetime.now(timezone.utc).isoformat()


    return {
        # Champs requis cahier des charges
        "trip_id":          str(uuid.uuid4()),
        "rider_id":         f"RIDER-RAIN-{random.randint(1, 99999):05d}",
        "origin_zone":      zone_id,
        "destination_zone": dest_zone, 
        "requested_at": now_utc,
        "timestamp_utc": now_utc,
        "requested_at":     datetime.now(timezone.utc).isoformat(),
        "call_type":        random.choice(["A", "B", "C"]),

        # Métadonnées anomalie
        "event_id":         str(uuid.uuid4()),
        "event_type":       "trip_request",
        "anomaly_type":     "rain_event",
        "origin": {
            "lat": round(origin_lat, 7),
            "lon": round(origin_lon, 7),
        },
        "destination": {
            "lat": round(dest_lat, 7),
            "lon": round(dest_lon, 7),
        },
        "estimated_distance_km":  round(random.uniform(1, 12), 2),
        "estimated_duration_min": round(random.uniform(5, 35), 1),
        "estimated_fare_mad":     round(random.uniform(10, 60), 2),
        "payment_type":           random.choices(
            config.PAYMENT_TYPES, weights=config.PAYMENT_DISTRIBUTION
        )[0],
        "passenger_count": random.randint(1, 3),
        "status":          "requested",
        "simulation":      True,
        "injected":        True,
    }


def run_rain(
    producer: KafkaProducer,
    duration_s: float,
    rain_factor: float = None,
) -> None:
    """
    Simule un événement pluie pendant `duration_s` secondes.

    Comment ça marche :
      - En régime normal : 1 demande toutes les ~30 s.
      - Avec rain_factor=1.4 : 1 demande toutes les ~21 s (30 / 1.4).
      - Les demandes sont distribuées sur toutes les zones (pluie globale).
      - Le champ anomaly_type="rain_event" permet aux consommateurs
        de distinguer ces demandes des demandes normales.

    Exemple réel : pluie soudaine à Casablanca un soir de semaine →
    toutes les personnes dans la rue cherchent un taxi en même temps.
    """
    if rain_factor is None:
        rain_factor = config.INJECTOR_RAIN_FACTOR

    logger.info(
        f"[RAIN] Démarrage | facteur=×{rain_factor} | durée={duration_s:.0f}s | "
        f"topic={config.KAFKA_TOPIC_TRIPS}"
    )

    base_delay = config.TRIP_REQUEST_INTERVAL_SECONDS / rain_factor
    end_time   = time.time() + duration_s
    sent       = 0

    while time.time() < end_time:
        event = build_rain_trip_request()
        key   = str(event["origin_zone"])

        if kafka_send(producer, config.KAFKA_TOPIC_TRIPS, event, key=key):
            sent += 1
            logger.info(
                f"[RAIN] Envoyé #{sent} | zone={event['origin_zone']} | "
                f"rider={event['rider_id']} | restant={end_time - time.time():.0f}s"
            )

        time.sleep(base_delay)

    logger.info(f"[RAIN] Terminé — {sent} événements injectés.")


# ---------------------------------------------------------------------------
# Parser CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="TaaSim Event Injector — injecte des anomalies pour démo live.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python event_injector.py spike --zone 3 --factor 3.0 --duration 300
  python event_injector.py blackout --taxis 12,47,88 --duration 120
  python event_injector.py rain --duration 600
  python event_injector.py rain --factor 2.0 --duration 300
        """,
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # --- spike ---
    sp = sub.add_parser("spike", help="Spike de demande dans une zone")
    sp.add_argument("--zone", type=int, required=True,
                    help="ID de la zone ciblée (1-10)")
    sp.add_argument("--factor", type=float,
                    default=config.INJECTOR_SPIKE_DEFAULT_FACTOR,
                    help=f"Facteur multiplicateur (défaut : {config.INJECTOR_SPIKE_DEFAULT_FACTOR})")
    sp.add_argument("--duration", type=float,
                    default=config.INJECTOR_SPIKE_DEFAULT_DURATION,
                    help=f"Durée en secondes (défaut : {config.INJECTOR_SPIKE_DEFAULT_DURATION})")

    # --- blackout ---
    bp = sub.add_parser("blackout", help="Coupure GPS pour des taxis")
    bp.add_argument("--taxis", type=str, required=True,
                    help="IDs des taxis séparés par virgule (ex: 12,47,88)")
    bp.add_argument("--duration", type=float,
                    default=config.INJECTOR_BLACKOUT_DEFAULT_DURATION,
                    help=f"Durée en secondes (défaut : {config.INJECTOR_BLACKOUT_DEFAULT_DURATION})")

    # --- rain ---
    rp = sub.add_parser("rain", help="Événement pluie global")
    rp.add_argument("--factor", type=float,
                    default=config.INJECTOR_RAIN_FACTOR,
                    help=f"Facteur multiplicateur (défaut : {config.INJECTOR_RAIN_FACTOR})")
    rp.add_argument("--duration", type=float,
                    default=config.INJECTOR_RAIN_DEFAULT_DURATION,
                    help=f"Durée en secondes (défaut : {config.INJECTOR_RAIN_DEFAULT_DURATION})")

    return parser.parse_args()


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    logger.info("=" * 60)
    logger.info(f"TaaSim Event Injector — commande : {args.command.upper()}")
    logger.info(f"Kafka : {config.KAFKA_BOOTSTRAP_SERVERS}")
    logger.info("=" * 60)

    producer = create_kafka_producer()

    try:
        if args.command == "spike":
            logger.info(
                f"Injection SPIKE | zone={args.zone} | "
                f"×{args.factor} | {args.duration}s"
            )
            run_spike(producer, args.zone, args.factor, args.duration)

        elif args.command == "blackout":
            taxi_ids = [int(t.strip()) for t in args.taxis.split(",")]
            logger.info(
                f"Injection BLACKOUT | taxis={taxi_ids} | {args.duration}s"
            )
            run_blackout(producer, taxi_ids, args.duration)

        elif args.command == "rain":
            logger.info(
                f"Injection RAIN | ×{args.factor} | {args.duration}s"
            )
            run_rain(producer, args.duration, args.factor)

    except KeyboardInterrupt:
        logger.info("Injection interrompue (Ctrl+C).")
    finally:
        producer.flush()
        producer.close()
        logger.info("Injecteur arrêté proprement.")


if __name__ == "__main__":
    main()
