import json
import logging
import os
import random
import shutil
from datetime import datetime
from pathlib import Path

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
# Casablanca coordinates
# =============================================================

CITY = "Casablanca"

LAT_MIN, LAT_MAX = 33.47, 33.63
LON_MIN, LON_MAX = -7.73, -7.51

MAX_SPEED_KMH = 70
MIN_REALISTIC_SPEED_KMH = 20


# =============================================================
# Flink JARs
# =============================================================

KAFKA_JAR_PATH = "flink/jars/flink-sql-connector-kafka-3.0.2-1.18.jar"


# =============================================================
# Helpers
# =============================================================

def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def coords_to_zone(lat: float, lon: float) -> int:
    col = int((lon - LON_MIN) / (LON_MAX - LON_MIN) * 4)
    row = int((LAT_MAX - lat) / (LAT_MAX - LAT_MIN) * 4)

    col = max(0, min(col, 3))
    row = max(0, min(row, 3))

    return row * 4 + col + 1


# =============================================================
# Processing functions
# =============================================================

class NormalizeGpsEvent(ProcessFunction):
    def process_element(self, raw_message: str, ctx: ProcessFunction.Context):
        try:
            event = json.loads(raw_message)

            taxi_id = str(event["taxi_id"])
            timestamp_utc = event["timestamp_utc"]
            lat = float(event["lat"])
            lon = float(event["lon"])
            speed = float(event.get("speed", 0.0))
            status = str(event.get("status", "unknown"))

            if speed < 0:
                return

            if speed > MAX_SPEED_KMH:
                speed = round(
                    random.uniform(MIN_REALISTIC_SPEED_KMH, MAX_SPEED_KMH),
                    1,
                )

            if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
                return

            event_time = parse_datetime(timestamp_utc)
            zone_id = coords_to_zone(lat, lon)

            normalized_event = {
                "city": CITY,
                "zone_id": zone_id,
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

            yield json.dumps(normalized_event)

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
    ).name("NormalizeGpsEvent")

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

    env.execute("TaaSim - Flink Job 1 - GPS Normalizer")


if __name__ == "__main__":
    main()