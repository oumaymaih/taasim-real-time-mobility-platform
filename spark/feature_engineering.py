#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("TaaSim-FeatureEngineering")
    .master("spark://spark-master:7077")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "taasim")
    .config("spark.hadoop.fs.s3a.secret.key", "taasim2024")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("FEATURE ENGINEERING - START")
print("=" * 60)

# ============================================================
# LIRE LES DONNÉES PORTO NETTOYÉES
# ============================================================

print("\n📂 Lecture des données Porto depuis curated...")
porto = spark.read.parquet("s3a://taasim/curated/porto/")
print(f"✅ Porto chargé : {porto.count()} lignes")

# ============================================================
# CRÉER DES CRÉNEAUX DE 30 MINUTES
# ============================================================

print("\n🕐 Agrégation par zone et créneau de 30 minutes...")

# Utiliser TIMESTAMP directement (c'est un entier UNIX)
porto = porto.withColumn(
    "slot_30min",
    expr("from_unixtime(floor(TIMESTAMP / 1800) * 1800)")
)

# Grouper par zone et slot
demand_by_slot = (
    porto.groupBy("zone_id", "slot_30min")
    .agg(count("*").alias("demand"))
)

print(f"✅ {demand_by_slot.count()} créneaux générés")

# ============================================================
# CARACTÉRISTIQUES TEMPORELLES
# ============================================================

print("\n📊 Ajout des caractéristiques temporelles...")

demand_by_slot = (
    demand_by_slot
    .withColumn("timestamp", col("slot_30min").cast("timestamp"))
    .withColumn("hour", hour("timestamp"))
    .withColumn("day_of_week", dayofweek("timestamp"))  # 1=dimanche, 7=samedi
    .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), 1).otherwise(0))
    .withColumn("is_friday", when(col("day_of_week") == 6, 1).otherwise(0))
    .withColumn("date", to_date("timestamp"))
)

# ============================================================
# LAGS (J-1, J-7) ET MOYENNE MOBILE
# ============================================================

print("\n🔄 Calcul des lags et moyenne mobile...")

window_spec = Window.partitionBy("zone_id").orderBy("timestamp")

# Lag 1 jour (même heure, jour précédent) – 48 slots de 30 min = 24h
demand_by_slot = (
    demand_by_slot
    .withColumn("demand_lag_1d", lag("demand", 48).over(window_spec))
)

# Lag 7 jours – 336 slots = 7j
demand_by_slot = (
    demand_by_slot
    .withColumn("demand_lag_7d", lag("demand", 336).over(window_spec))
)

# Moyenne mobile 7 jours (les 336 slots précédents)
window_7d = Window.partitionBy("zone_id").orderBy("timestamp").rowsBetween(-336, -1)
demand_by_slot = (
    demand_by_slot
    .withColumn("rolling_7d_mean", avg("demand").over(window_7d))
)

# ============================================================
# NETTOYAGE (supprimer les lignes avec des NULL)
# ============================================================

print("\n🧹 Suppression des lignes avec valeurs manquantes...")

demand_by_slot = demand_by_slot.filter(
    col("demand_lag_1d").isNotNull() &
    col("demand_lag_7d").isNotNull() &
    col("rolling_7d_mean").isNotNull()
)

print(f"✅ Après nettoyage : {demand_by_slot.count()} lignes")

# ============================================================
# AJOUTER LA COLONNE TARGET
# ============================================================

demand_by_slot = demand_by_slot.withColumnRenamed("demand", "target")

# ============================================================
# SAUVEGARDE DANS MINIO
# ============================================================

print("\n💾 Sauvegarde du dataset dans MinIO...")

OUTPUT_PATH = "s3a://taasim/ml/features/"

demand_by_slot.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet(OUTPUT_PATH)

print(f"✅ Dataset sauvegardé dans {OUTPUT_PATH}")

# ============================================================
# VÉRIFICATION RAPIDE
# ============================================================

print("\n📊 Échantillon des données :")
demand_by_slot.select(
    "zone_id", "timestamp", "hour", "day_of_week", "is_weekend", "is_friday",
    "target", "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean"
).show(10, truncate=False)

print(f"\n✅ Nombre total de lignes : {demand_by_slot.count()}")
print("=" * 60)
print("FEATURE ENGINEERING - TERMINÉ")
print("=" * 60)

spark.stop()