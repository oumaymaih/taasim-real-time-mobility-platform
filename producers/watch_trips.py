
# watch_topics.py
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "raw.trips",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

COLORS = {
    "rain_event":    "\033[94m",   # bleu
    "demand_spike":  "\033[91m",   # rouge
    "normal":        "\033[92m",   # vert
    "RESET":         "\033[0m",
}

for msg in consumer:
    e = msg.value
    anomaly = e.get("anomaly_type", "normal") or "normal"
    color   = COLORS.get(anomaly, COLORS["normal"])
    reset   = COLORS["RESET"]

    label = f"[{anomaly.upper()}]" if anomaly != "normal" else "[NORMAL]"

    print(f"\n{color}{'='*60}")
    print(f"  {label}  rider={e.get('rider_id')}  zone {e.get('origin_zone')} → {e.get('destination_zone')}")
    print(f"  dist={e.get('estimated_distance_km')} km  |  fare={e.get('estimated_fare_mad')} MAD  |  pay={e.get('payment_type')}")
    print(f"  time={e.get('timestamp_utc')}  |  injected={e.get('injected', False)}")
    print(f"{'='*60}{reset}")