"""
flink/tests/inject_late_event.py  —  TaaSim  W3
================================================
Injecte 4 messages GPS dans Kafka pour tester le watermark Flink.

Cas testés :
    Cas 1 — offset  0 min  →  événement normal          → écrit dans Cassandra
    Cas 2 — offset -2 min  →  légèrement tardif (<3min) → écrit dans Cassandra
    Cas 3 — offset -4 min  →  tardif (>3min)            → late event, non écrit
    Cas 4 — offset -10 min →  très tardif               → late event, non écrit

Après injection, observer les logs du TaskManager :
    docker logs taasim-flink-tm --follow --tail=50

Usage :
    python flink/tests/inject_late_event.py
    python flink/tests/inject_late_event.py --case 3
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Broker externe (script lancé depuis la machine hôte)
KAFKA_BOOTSTRAP = "localhost:29092"
KAFKA_TOPIC     = "taasim.gps.positions"

# Coordonnées valides dans la bbox Casablanca
TEST_LAT =  33.5731
TEST_LON = -7.5898

# Cas de test
TEST_CASES = [
    {
        "id":       1,
        "label":    "NORMAL — offset 0 min",
        "offset":   0,
        "expected": "✅ Écrit dans Cassandra (dans les délais)",
    },
    {
        "id":       2,
        "label":    "LÉGÈREMENT TARDIF — offset -2 min",
        "offset":   -2,
        "expected": "✅ Écrit dans Cassandra (retard < 3 min, dans la tolérance)",
    },
    {
        "id":       3,
        "label":    "TARDIF — offset -4 min",
        "offset":   -4,
        "expected": "⚠  Late event — non écrit dans Cassandra (retard > 3 min)",
    },
    {
        "id":       4,
        "label":    "TRÈS TARDIF — offset -10 min",
        "offset":   -10,
        "expected": "⚠  Late event — non écrit dans Cassandra (retard = 10 min)",
    },
]


def make_event(offset_minutes: int, label: str) -> dict:
    """Construit un événement GPS de test avec le décalage temporel donné."""
    event_time = datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)
    return {
        "event_id":         str(uuid.uuid4()),
        "event_type":       "gps_position",
        "trip_id":          99000 + abs(offset_minutes),
        "vehicle_id":       f"TAXI-TEST-{abs(offset_minutes):02d}min",
        "taxi_id":          f"T{abs(offset_minutes):03d}",
        "point_index":      0,
        "latitude":         TEST_LAT,
        "longitude":        TEST_LON,
        "origin_zone":      1,
        "destination_zone": 10,
        "call_type":        "A",
        "day_type":         "A",
        "timestamp_utc":    event_time.isoformat(),
        "simulation":       True,
        "_test_label":      label,
    }


def run(cases: list) -> None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
        )
    except KafkaError as e:
        print(f"Impossible de se connecter à Kafka ({KAFKA_BOOTSTRAP}) : {e}")
        return

    print("\n" + "═" * 60)
    print("  TaaSim W3 — Test watermark Flink (late events)")
    print(f"  Topic : {KAFKA_TOPIC}  |  Broker : {KAFKA_BOOTSTRAP}")
    print("═" * 60 + "\n")

    for case in cases:
        event = make_event(case["offset"], case["label"])

        print(f"── Cas {case['id']} : {case['label']}")
        print(f"   timestamp_utc : {event['timestamp_utc']}")
        print(f"   vehicle_id    : {event['vehicle_id']}")
        print(f"   Attendu       : {case['expected']}")

        try:
            rec = producer.send(KAFKA_TOPIC, value=event).get(timeout=10)
            print(f"   Kafka         : partition={rec.partition}, offset={rec.offset} ✓")
        except KafkaError as e:
            print(f"   ERREUR Kafka  : {e}")

        print()
        # Pause pour que Flink traite le message et mette à jour le watermark
        time.sleep(3)

    producer.flush()
    producer.close()

    print("═" * 60)
    print("  Injection terminée.")
    print("\n  Observer les logs Flink :")
    print("    docker logs taasim-flink-tm --follow --tail=50")
    print("\n  Vérifier dans Cassandra (cas 1 et 2 uniquement) :")
    print("    docker exec -it taasim-cassandra cqlsh")
    print("    USE taasim;")
    print("    SELECT taxi_id, event_time, lat, lon FROM vehicle_positions")
    print("    WHERE city='Casablanca' AND zone_id=10 LIMIT 10;")
    print("═" * 60 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Injection de late events pour test du watermark Flink W3"
    )
    parser.add_argument(
        "--case", type=int, choices=[1, 2, 3, 4], default=None,
        help="Injecter uniquement le cas N. Sans argument : tous les cas."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    selected = [c for c in TEST_CASES if c["id"] == args.case] if args.case else TEST_CASES
    run(selected)
