# watch_gps.py
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "raw.gps",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

COLORS = {
    "moving":             "\033[92m",   # vert
    "idle":               "\033[93m",   # jaune
    "blackout_recovered": "\033[91m",   # rouge
    "blackout":           "\033[91m",   # rouge
    "RESET":              "\033[0m",
}

STATUS_ICONS = {
    "moving":             "🚕 MOVING",
    "idle":               "🅿️  IDLE",
    "blackout_recovered": "⚠️  BLACKOUT RECOVERED",
    "blackout":           "🔴 BLACKOUT",
}

for msg in consumer:
    e = msg.value
    status  = e.get("status", "unknown")
    color   = COLORS.get(status, "\033[97m")
    reset   = COLORS["RESET"]
    label   = STATUS_ICONS.get(status, f"[{status.upper()}]")

    print(f"\n{color}{'='*60}")
    print(f"  {label}")
    print(f"  taxi={e.get('taxi_id')}  |  vehicle={e.get('vehicle_id')}")
    print(f"  lat={e.get('lat')}  lon={e.get('lon')}")
    print(f"  speed={e.get('speed')} km/h  |  point_index={e.get('point_index')}")
    print(f"  zone {e.get('origin_zone')} → {e.get('destination_zone')}  |  call_type={e.get('call_type')}")
    print(f"  time={e.get('timestamp_utc')}")
    print(f"{'='*60}{reset}")