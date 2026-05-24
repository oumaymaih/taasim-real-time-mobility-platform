import json
import logging
import os
import random
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyflink


# =============================================================
# Logging
# =============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

logger = logging.getLogger("taasim.gps_normalizer")


# =============================================================
# MinIO config
# =============================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "taasim")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "taasim2024")

CHECKPOINT_DIR = os.getenv(
    "FLINK_CHECKPOINT_DIR",
    "s3a://taasim-checkpoints/flink/checkpoints",
)

CHECKPOINT_INTERVAL_MS = 60_000


# =============================================================
# GeoJSON zones config
# =============================================================

CITY = "Casablanca"

# Bounding box is computed from the GeoJSON polygons.
# The real zone_id is computed using point-in-polygon over arrondissements.
GEOJSON_BBOX_MARGIN = float(os.getenv("GEOJSON_BBOX_MARGIN", "0.01"))

# Put your Casablanca arrondissements GeoJSON here, or override with env var:
# $env:CASA_ZONES_GEOJSON="data/casablanca_arrondissements.geojson"
ZONES_GEOJSON_PATH = Path(
    os.getenv("CASA_ZONES_GEOJSON", "datasets/Arrondissements.geojson")
)

# If a GPS point is inside the Casablanca bbox but does not fall exactly inside
# a polygon due to GPS noise / polygon gaps, assign the nearest arrondissement.
ASSIGN_NEAREST_ZONE_IF_OUTSIDE = (
    os.getenv("ASSIGN_NEAREST_ZONE_IF_OUTSIDE", "true").lower() == "true"
)

# Your GeoJSON contains 17 features because it includes Mechouar.
# To stay aligned with the project requirement of 16 Casablanca arrondissements,
# we ignore Mechouar by default. Remove this env value if you want 17 zones.
IGNORE_ARRONDISSEMENTS = {
    name.strip().lower()
    for name in os.getenv("IGNORE_ARRONDISSEMENTS", "Mechouar Casablanca").split(",")
    if name.strip()
}

MAX_SPEED_KMH = 70
MIN_REALISTIC_SPEED_KMH = 20


# =============================================================
# S3 / Hadoop setup BEFORE PyFlink datastream imports
# =============================================================

_PYFLINK_HOME = Path(pyflink.__file__).parent
_PLUGINS_DIR = _PYFLINK_HOME / "plugins"
_PLUGIN_SUBDIR = _PLUGINS_DIR / "s3-fs-hadoop"
_PYFLINK_LIB_DIR = _PYFLINK_HOME / "lib"
_HADOOP_CONF_DIR = Path("flink/hadoop-conf").resolve()

os.environ["FLINK_HOME"] = str(_PYFLINK_HOME)
os.environ["FLINK_PLUGINS_DIR"] = str(_PLUGINS_DIR)
os.environ["HADOOP_CONF_DIR"] = str(_HADOOP_CONF_DIR)

# Force credentials for Hadoop / AWS SDK
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_ACCESS_KEY"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_REGION"] = "us-east-1"

S3_PLUGIN_JAR_SRC = Path("flink/jars/flink-s3-fs-hadoop-1.18.0.jar")
S3_PLUGIN_JAR_DST_PLUGIN = _PLUGIN_SUBDIR / "flink-s3-fs-hadoop-1.18.0.jar"
S3_PLUGIN_JAR_DST_LIB = _PYFLINK_LIB_DIR / "flink-s3-fs-hadoop-1.18.0.jar"


def ensure_s3_plugin_before_flink_starts():
    if not S3_PLUGIN_JAR_SRC.exists():
        raise FileNotFoundError(
            f"JAR S3 introuvable: {S3_PLUGIN_JAR_SRC.resolve()}\n"
            "Télécharge d'abord flink-s3-fs-hadoop-1.18.0.jar dans flink/jars/"
        )

    _PLUGIN_SUBDIR.mkdir(parents=True, exist_ok=True)
    _PYFLINK_LIB_DIR.mkdir(parents=True, exist_ok=True)

    shutil.copy2(S3_PLUGIN_JAR_SRC, S3_PLUGIN_JAR_DST_PLUGIN)
    shutil.copy2(S3_PLUGIN_JAR_SRC, S3_PLUGIN_JAR_DST_LIB)

    logger.info(f"FLINK_HOME = {os.environ['FLINK_HOME']}")
    logger.info(f"FLINK_PLUGINS_DIR = {os.environ['FLINK_PLUGINS_DIR']}")
    logger.info(f"HADOOP_CONF_DIR = {os.environ['HADOOP_CONF_DIR']}")
    logger.info(f"Plugin S3 copié dans plugins: {S3_PLUGIN_JAR_DST_PLUGIN}")
    logger.info(f"Plugin S3 copié dans lib: {S3_PLUGIN_JAR_DST_LIB}")


def ensure_hadoop_core_site():
    _HADOOP_CONF_DIR.mkdir(parents=True, exist_ok=True)
    core_site_path = _HADOOP_CONF_DIR / "core-site.xml"

    core_site_content = f"""<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>{MINIO_ENDPOINT}</value>
    </property>

    <property>
        <name>fs.s3a.bucket.taasim-checkpoints.endpoint</name>
        <value>{MINIO_ENDPOINT}</value>
    </property>

    <property>
        <name>fs.s3a.access.key</name>
        <value>{MINIO_ACCESS_KEY}</value>
    </property>

    <property>
        <name>fs.s3a.secret.key</name>
        <value>{MINIO_SECRET_KEY}</value>
    </property>

    <property>
        <name>fs.s3a.bucket.taasim-checkpoints.access.key</name>
        <value>{MINIO_ACCESS_KEY}</value>
    </property>

    <property>
        <name>fs.s3a.bucket.taasim-checkpoints.secret.key</name>
        <value>{MINIO_SECRET_KEY}</value>
    </property>

    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>

    <property>
        <name>fs.s3a.connection.ssl.enabled</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>

    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>

    <property>
        <name>fs.s3a.endpoint.region</name>
        <value>us-east-1</value>
    </property>

    <property>
        <name>fs.s3a.change.detection.mode</name>
        <value>none</value>
    </property>

    <property>
        <name>fs.s3a.change.detection.version.required</name>
        <value>false</value>
    </property>
</configuration>
"""

    core_site_path.write_text(core_site_content, encoding="utf-8")
    logger.info(f"core-site.xml généré: {core_site_path}")


ensure_s3_plugin_before_flink_starts()
ensure_hadoop_core_site()


from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import ProcessFunction


# =============================================================
# Kafka
# =============================================================

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
RAW_GPS_TOPIC = "raw.gps"
PROCESSED_GPS_TOPIC = "processed.gps"
KAFKA_GROUP_ID = "flink-gps-normalizer-v1"


# =============================================================
# Cassandra
# =============================================================

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taasim")


# =============================================================
# Flink JARs
# =============================================================

KAFKA_JAR_PATH = "flink/jars/flink-sql-connector-kafka-3.0.2-1.18.jar"


# =============================================================
# Helpers: time + GeoJSON point-in-polygon
# =============================================================

def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def extract_property(props: Dict[str, Any], names: List[str], default=None):
    for name in names:
        if name in props and props[name] not in (None, ""):
            return props[name]
    lower = {str(k).lower(): v for k, v in props.items()}
    for name in names:
        key = name.lower()
        if key in lower and lower[key] not in (None, ""):
            return lower[key]
    return default


def point_in_ring(lon: float, lat: float, ring: List[List[float]]) -> bool:
    """Ray casting. Ring coordinates are [lon, lat]."""
    inside = False
    n = len(ring)
    if n < 3:
        return False

    j = n - 1
    for i in range(n):
        xi, yi = float(ring[i][0]), float(ring[i][1])
        xj, yj = float(ring[j][0]), float(ring[j][1])

        intersects = ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i

    return inside


def point_in_polygon(lon: float, lat: float, polygon: List[List[List[float]]]) -> bool:
    """Polygon = [outer_ring, hole1, hole2, ...]."""
    if not polygon:
        return False

    outer = polygon[0]
    if not point_in_ring(lon, lat, outer):
        return False

    # Exclude holes
    for hole in polygon[1:]:
        if point_in_ring(lon, lat, hole):
            return False

    return True


def simple_centroid_from_geometry(geometry: Dict[str, Any]) -> Tuple[float, float]:
    """Approximate centroid as average of all coordinates. Returns (lon, lat)."""
    coords = []

    def collect(obj):
        if isinstance(obj, list):
            if len(obj) >= 2 and isinstance(obj[0], (int, float)) and isinstance(obj[1], (int, float)):
                coords.append((float(obj[0]), float(obj[1])))
            else:
                for item in obj:
                    collect(item)

    collect(geometry.get("coordinates", []))

    if not coords:
        return 0.0, 0.0

    lon = sum(c[0] for c in coords) / len(coords)
    lat = sum(c[1] for c in coords) / len(coords)
    return lon, lat


def geometry_points(geometry: Dict[str, Any]):
    """Yield every (lon, lat) coordinate from Polygon/MultiPolygon geometry."""
    def collect(obj):
        if isinstance(obj, list):
            if len(obj) >= 2 and isinstance(obj[0], (int, float)) and isinstance(obj[1], (int, float)):
                yield float(obj[0]), float(obj[1])
            else:
                for item in obj:
                    yield from collect(item)

    yield from collect(geometry.get("coordinates", []))


class ArrondissementZoneLookup:
    """Loads Casablanca arrondissement polygons from a GeoJSON file."""

    def __init__(self, geojson_path: Path):
        self.geojson_path = geojson_path
        self.zones = []
        self.min_lon = None
        self.max_lon = None
        self.min_lat = None
        self.max_lat = None
        self.load()

    def load(self):
        if not self.geojson_path.exists():
            raise FileNotFoundError(
                f"GeoJSON introuvable: {self.geojson_path.resolve()}\n"
                "Crée le dossier data/ et mets le fichier sous le nom: "
                "data/casablanca_arrondissements.geojson\n"
                "Ou définis la variable CASA_ZONES_GEOJSON."
            )

        data = json.loads(self.geojson_path.read_text(encoding="utf-8"))
        features = data.get("features", [])
        if not features:
            raise ValueError(f"GeoJSON sans features: {self.geojson_path}")

        seq_id = 1
        for feature in features:
            props = feature.get("properties", {}) or {}
            geometry = feature.get("geometry") or {}
            geom_type = geometry.get("type")
            coords = geometry.get("coordinates")

            if geom_type not in ("Polygon", "MultiPolygon") or not coords:
                continue

            name = extract_property(
                props,
                [
                    "Arrondissement",
                    "arrondissement",
                    "ARRONDISSEMENT",
                    "name",
                    "Name",
                    "NOM",
                    "nom",
                ],
                default=f"zone_{seq_id}",
            )

            if str(name).strip().lower() in IGNORE_ARRONDISSEMENTS:
                logger.info(f"Arrondissement ignoré depuis GeoJSON: {name}")
                continue

            raw_zone_id = extract_property(
                props,
                [
                    "zone_id",
                    "ZONE_ID",
                    "Zone_ID",
                    "zone",
                    "ZONE",
                    "id_zone",
                    "ID_ZONE",
                    "id",
                    "ID",
                ],
                default=None,
            )

            try:
                zone_id = int(raw_zone_id) if raw_zone_id is not None else seq_id
            except Exception:
                zone_id = seq_id

            centroid_lon, centroid_lat = simple_centroid_from_geometry(geometry)

            zone_record = {
                "zone_id": zone_id,
                "arrondissement": str(name),
                "prefecture": str(extract_property(props, ["Prefecture", "PREFECTURE", "prefecture"], default="")),
                "population": extract_property(props, ["Population", "POPULATION", "population"], default=None),
                "geometry": geometry,
                "centroid_lon": centroid_lon,
                "centroid_lat": centroid_lat,
            }
            self.zones.append(zone_record)

            for gx, gy in geometry_points(geometry):
                self.min_lon = gx if self.min_lon is None else min(self.min_lon, gx)
                self.max_lon = gx if self.max_lon is None else max(self.max_lon, gx)
                self.min_lat = gy if self.min_lat is None else min(self.min_lat, gy)
                self.max_lat = gy if self.max_lat is None else max(self.max_lat, gy)

            seq_id += 1

        if not self.zones:
            raise ValueError("Aucun Polygon/MultiPolygon valide trouvé dans le GeoJSON.")

        logger.info(
            f"GeoJSON chargé: {self.geojson_path} — {len(self.zones)} zones"
        )
        logger.info(
            "Zones: "
            + ", ".join(
                f"{z['zone_id']}={z['arrondissement']}" for z in self.zones
            )
        )
        logger.info(
            f"GeoJSON bbox: lon=[{self.min_lon}, {self.max_lon}], "
            f"lat=[{self.min_lat}, {self.max_lat}], margin={GEOJSON_BBOX_MARGIN}"
        )

    def is_inside_bbox(self, lat: float, lon: float) -> bool:
        if None in (self.min_lon, self.max_lon, self.min_lat, self.max_lat):
            return True
        return (
            self.min_lat - GEOJSON_BBOX_MARGIN <= lat <= self.max_lat + GEOJSON_BBOX_MARGIN
            and self.min_lon - GEOJSON_BBOX_MARGIN <= lon <= self.max_lon + GEOJSON_BBOX_MARGIN
        )

    def find_zone(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        for zone in self.zones:
            geometry = zone["geometry"]
            geom_type = geometry["type"]
            coords = geometry["coordinates"]

            if geom_type == "Polygon":
                if point_in_polygon(lon, lat, coords):
                    return zone

            elif geom_type == "MultiPolygon":
                for polygon in coords:
                    if point_in_polygon(lon, lat, polygon):
                        return zone

        if not ASSIGN_NEAREST_ZONE_IF_OUTSIDE:
            return None

        # Fallback for small GPS noise or tiny gaps between polygons.
        nearest = min(
            self.zones,
            key=lambda z: (lon - z["centroid_lon"]) ** 2 + (lat - z["centroid_lat"]) ** 2,
        )
        return nearest


# =============================================================
# Processing functions
# =============================================================

class NormalizeGpsEvent(ProcessFunction):
    def open(self, runtime_context):
        self.zone_lookup = ArrondissementZoneLookup(ZONES_GEOJSON_PATH)

    def process_element(self, raw_message: str, ctx: ProcessFunction.Context):
        try:
            event = json.loads(raw_message)

            taxi_id = str(event.get("taxi_id", event.get("vehicle_id", "unknown")))
            timestamp_utc = event.get("timestamp_utc", event.get("timestamp"))
            if not timestamp_utc:
                raise ValueError("timestamp_utc manquant")

            lat_value = event.get("lat", event.get("latitude"))
            lon_value = event.get("lon", event.get("longitude"))
            lat = float(lat_value)
            lon = float(lon_value)

            speed = float(event.get("speed", 0.0))
            status = str(event.get("status", "unknown"))

            if speed < 0:
                return

            if speed > MAX_SPEED_KMH:
                speed = round(
                    random.uniform(MIN_REALISTIC_SPEED_KMH, MAX_SPEED_KMH),
                    1,
                )

            # Bounding box = validation only. Zone mapping = GeoJSON polygons.
            if not self.zone_lookup.is_inside_bbox(lat=lat, lon=lon):
                logger.warning(f"GPS hors bbox GeoJSON Casablanca ignoré: lat={lat}, lon={lon}")
                return

            zone = self.zone_lookup.find_zone(lat=lat, lon=lon)
            if zone is None:
                logger.warning(f"Aucune zone GeoJSON trouvée: lat={lat}, lon={lon}")
                return

            event_time = parse_datetime(timestamp_utc)

            normalized_event = {
                "city": CITY,
                "zone_id": int(zone["zone_id"]),
                "arrondissement": zone["arrondissement"],
                "prefecture": zone.get("prefecture"),
                "zone_population": zone.get("population"),
                "event_time": event_time.isoformat(),
                "taxi_id": taxi_id,
                "lat": lat,
                "lon": lon,
                "speed": speed,
                "status": status,
                "trip_id": event.get("trip_id"),
                "vehicle_id": event.get("vehicle_id"),
                "origin_zone": event.get("origin_zone"),
                "destination_zone": event.get("destination_zone"),
            }

            yield json.dumps(normalized_event, ensure_ascii=False)

        except Exception as e:
            logger.warning(f"GPS event ignoré: {e}")


class GpsTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            event = json.loads(value)
            dt = parse_datetime(event["event_time"])
            return int(dt.timestamp() * 1000)
        except Exception:
            return record_timestamp


class DropLateEvents(ProcessFunction):
    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            event = json.loads(value)
            event_ts_ms = int(parse_datetime(event["event_time"]).timestamp() * 1000)
            watermark_ms = ctx.timer_service().current_watermark()

            if watermark_ms <= 0 or event_ts_ms >= watermark_ms:
                yield value
            else:
                logger.warning(
                    f"Late GPS ignoré: taxi_id={event.get('taxi_id')} "
                    f"zone_id={event.get('zone_id')} "
                    f"retard={(watermark_ms - event_ts_ms) // 1000}s"
                )

        except Exception as e:
            logger.warning(f"Erreur late-event filter: {e}")


class CassandraSink(ProcessFunction):
    def __init__(self):
        self.session = None
        self.insert_stmt = None

    def open(self, runtime_context):
        from cassandra.cluster import Cluster

        cluster = Cluster(
            contact_points=[CASSANDRA_HOST],
            port=CASSANDRA_PORT,
        )

        self.session = cluster.connect(CASSANDRA_KEYSPACE)

        self.insert_stmt = self.session.prepare(
            """
            INSERT INTO vehicle_positions
                (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """
        )

        logger.info(
            f"Cassandra connecté: {CASSANDRA_HOST}:{CASSANDRA_PORT}/{CASSANDRA_KEYSPACE}"
        )

    def process_element(self, value: str, ctx: ProcessFunction.Context):
        try:
            event = json.loads(value)

            self.session.execute(
                self.insert_stmt,
                (
                    event["city"],
                    int(event["zone_id"]),
                    parse_datetime(event["event_time"]),
                    str(event["taxi_id"]),
                    float(event["lat"]),
                    float(event["lon"]),
                    float(event["speed"]),
                    str(event["status"]),
                ),
            )

        except Exception as e:
            logger.error(f"Erreur CassandraSink: {e}")

        return
        yield


# =============================================================
# Builders
# =============================================================

def build_kafka_source():
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(RAW_GPS_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink():
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(PROCESSED_GPS_TOPIC)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )

    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(serializer)
        .build()
    )


def build_flink_configuration() -> Configuration:
    config = Configuration()

    config.set_string("plugin.dir", str(_PLUGINS_DIR))
    config.set_string("fs.hdfs.hadoopconf", str(_HADOOP_CONF_DIR))

    # Flink S3 plugin config
    config.set_string("s3.endpoint", MINIO_ENDPOINT)
    config.set_string("s3.access-key", MINIO_ACCESS_KEY)
    config.set_string("s3.secret-key", MINIO_SECRET_KEY)
    config.set_string("s3.path.style.access", "true")
    config.set_string("s3.connection.ssl.enabled", "false")
    config.set_string("s3.region", "us-east-1")

    # Hadoop S3A config
    config.set_string("fs.s3a.endpoint", MINIO_ENDPOINT)
    config.set_string("fs.s3a.bucket.taasim-checkpoints.endpoint", MINIO_ENDPOINT)
    config.set_string("fs.s3a.endpoint.region", "us-east-1")
    config.set_string("fs.s3a.access.key", MINIO_ACCESS_KEY)
    config.set_string("fs.s3a.secret.key", MINIO_SECRET_KEY)
    config.set_string("fs.s3a.bucket.taasim-checkpoints.access.key", MINIO_ACCESS_KEY)
    config.set_string("fs.s3a.bucket.taasim-checkpoints.secret.key", MINIO_SECRET_KEY)
    config.set_string(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    config.set_string("fs.s3a.path.style.access", "true")
    config.set_string("fs.s3a.connection.ssl.enabled", "false")
    config.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    config.set_string("fs.s3a.change.detection.mode", "none")
    config.set_string("fs.s3a.change.detection.version.required", "false")

    return config


def add_required_jars(env: StreamExecutionEnvironment):
    kafka_jar = Path(KAFKA_JAR_PATH).resolve()
    s3_jar = S3_PLUGIN_JAR_SRC.resolve()

    if not kafka_jar.exists():
        raise FileNotFoundError(f"Kafka connector JAR introuvable: {kafka_jar}")

    if not s3_jar.exists():
        raise FileNotFoundError(f"S3 connector JAR introuvable: {s3_jar}")

    env.add_jars(kafka_jar.as_uri())
    env.add_jars(s3_jar.as_uri())


def configure_checkpointing(env: StreamExecutionEnvironment):
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)

    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    checkpoint_config.set_checkpoint_storage(
        FileSystemCheckpointStorage(CHECKPOINT_DIR)
    )

    checkpoint_config.set_min_pause_between_checkpoints(30_000)
    checkpoint_config.set_checkpoint_timeout(120_000)

    logger.info(
        f"Checkpointing activé toutes les {CHECKPOINT_INTERVAL_MS // 1000}s"
    )
    logger.info(f"Checkpoint storage: {CHECKPOINT_DIR}")


# =============================================================
# Main
# =============================================================

def main():
    flink_config = build_flink_configuration()

    env = StreamExecutionEnvironment.get_execution_environment(flink_config)
    env.set_parallelism(1)

    add_required_jars(env)
    configure_checkpointing(env)

    kafka_source = build_kafka_source()
    kafka_sink = build_kafka_sink()

    raw_stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaSource[raw.gps]",
    )

    normalized_stream = raw_stream.process(
        NormalizeGpsEvent(),
        output_type=Types.STRING(),
    ).name("NormalizeGpsEventWithArrondissementGeoJSON")

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(3))
        .with_timestamp_assigner(GpsTimestampAssigner())
        .with_idleness(Duration.of_seconds(30))
    )

    clean_stream = (
        normalized_stream
        .assign_timestamps_and_watermarks(watermark_strategy)
        .process(DropLateEvents(), output_type=Types.STRING())
        .name("WatermarkAndLateEventFilter")
    )

    clean_stream.print().name("DebugPrint[processed.gps]")

    clean_stream.sink_to(kafka_sink).name("KafkaSink[processed.gps]")

    clean_stream.process(
        CassandraSink(),
        output_type=Types.STRING(),
    ).name("CassandraSink[vehicle_positions]")

    env.execute("TaaSim - Flink Job 1 - GPS Normalizer GeoJSON Arrondissements")


if __name__ == "__main__":
    main()
