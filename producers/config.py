"""
config.py — Configuration centralisée du projet TaaSim
=======================================================
Dataset : casablanca_dataset.parquet (dossier partitionné Spark)

Structure du dataset :
    - trip_id             : long
    - origin_zone         : integer
    - destination_zone    : integer
    - timestamp           : integer (epoch Unix)
    - CALL_TYPE           : string  (A=centrale, B=stand, C=rue)
    - TAXI_ID             : integer
    - DAY_TYPE            : string  (A=normal, B=férié, C=weekend)
    - Polyline transformé : array<array<double>> -> [[lon, lat], ...]
"""

import os

# ---------------------------------------------------------------------------
# Chemins
# ---------------------------------------------------------------------------

DATA_DIR = os.environ.get(
    "TAASIM_DATA_DIR",
    os.path.join(os.path.dirname(__file__), "..", "datasets")
)

TRIPS_PARQUET_PATH = os.path.join(DATA_DIR, "casablanca_dataset.parquet")
POLYLINE_COLUMN    = "Polyline transformé"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# Topics
KAFKA_TOPIC_GPS         = "raw.gps"
KAFKA_TOPIC_TRIPS       = "raw.trips"
KAFKA_TOPIC_TRIP_STATUS = "taasim.trip.status"

# Producer tuning
KAFKA_RETRIES          = 5
KAFKA_LINGER_MS        = 10
KAFKA_BATCH_SIZE       = 16384
KAFKA_COMPRESSION_TYPE = "gzip"

# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

# 1.0 = temps réel, 10.0 = 10× plus vite
SIMULATION_SPEED_FACTOR = float(os.environ.get("SIMULATION_SPEED_FACTOR", "10.0"))

# Un point GPS toutes les 15 secondes dans le dataset Porto/Casablanca
GPS_INTERVAL_SECONDS = 15.0

# Intervalle entre deux demandes de trajets successives (secondes réelles)
TRIP_REQUEST_INTERVAL_SECONDS = 30.0

# Filtre : trajets de plus de 60 min ignorés
MAX_TRIP_DURATION_MINUTES = 60

# Trajets max par cycle de simulation
MAX_TRIPS_PER_CYCLE = 500

# Rejouer en boucle infinie
LOOP_INFINITE = True

# ---------------------------------------------------------------------------
# Bruit GPS  (cahier des charges : σ ≈ 0.0002° ≈ 20 m)
# ---------------------------------------------------------------------------

ADD_GPS_NOISE     = True
GPS_NOISE_STD_DEG = 0.0002          # ~20 m à la latitude de Casablanca

# Blackout GPS : probabilité par véhicule par événement d'un délai 60-180 s
GPS_BLACKOUT_PROBABILITY  = 0.05    # 5 % par événement
GPS_BLACKOUT_MIN_SECONDS  = 60
GPS_BLACKOUT_MAX_SECONDS  = 180

ADD_TEMPORAL_JITTER         = True
TEMPORAL_JITTER_MAX_SECONDS = 3.0

# ---------------------------------------------------------------------------
# Courbe de demande horaire
# (multiplier normalisé — base 1.0 = taux nominal)
# Heures de pointe 7-9h et 17-19h → ×3-5
# Vendredi 12-14h → ×0.5
# ---------------------------------------------------------------------------

# Multiplier par heure (index 0=minuit … 23=23h)
DEMAND_CURVE_HOURLY = [
    0.2,   # 00h
    0.15,  # 01h
    0.1,   # 02h
    0.1,   # 03h
    0.15,  # 04h
    0.3,   # 05h
    0.7,   # 06h
    1.5,   # 07h  ← matin peak début
    2.5,   # 08h  ← matin peak
    1.8,   # 09h  ← matin peak fin
    1.0,   # 10h
    0.8,   # 11h
    0.7,   # 12h
    0.6,   # 13h
    0.7,   # 14h
    0.9,   # 15h
    1.2,   # 16h
    2.0,   # 17h  ← soir peak début
    2.8,   # 18h  ← soir peak
    2.0,   # 19h  ← soir peak fin
    1.3,   # 20h
    1.0,   # 21h
    0.7,   # 22h
    0.4,   # 23h
]

# Réduction vendredi midi (12h-14h) — multiplie le multiplier horaire
FRIDAY_LUNCH_REDUCTION = 0.5     # × 0.5 → taux réduit
FRIDAY_LUNCH_HOURS     = (12, 14)  # [12h, 14h[

# ---------------------------------------------------------------------------
# Métier trajet
# ---------------------------------------------------------------------------

PAYMENT_TYPES        = ["cash", "card", "mobile"]
PAYMENT_DISTRIBUTION = [0.55, 0.30, 0.15]

PASSENGER_COUNT_MIN = 1
PASSENGER_COUNT_MAX = 4

BASE_FARE_MAD    = 2.50
PRICE_PER_KM_MAD = 3.20

# ---------------------------------------------------------------------------
# Véhicules simulés
# ---------------------------------------------------------------------------

NUM_SIMULATED_VEHICLES = 20
VEHICLE_ID_PREFIX      = "TAXI-CASA"

# ---------------------------------------------------------------------------
# Event injector
# ---------------------------------------------------------------------------

# Spike de demande
INJECTOR_SPIKE_DEFAULT_FACTOR   = 3.0   # ×3 le taux normal
INJECTOR_SPIKE_DEFAULT_DURATION = 300   # 5 minutes

# Blackout GPS
INJECTOR_BLACKOUT_DEFAULT_DURATION = 120  # secondes

# Rain event
INJECTOR_RAIN_FACTOR          = 1.4   # ×1.4 globalement
INJECTOR_RAIN_DEFAULT_DURATION = 600  # 10 minutes

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

LOG_LEVEL              = os.environ.get("LOG_LEVEL", "INFO")
STATS_INTERVAL_SECONDS = 60
