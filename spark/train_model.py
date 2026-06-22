#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, when, lit, abs, sqrt
from pyspark.sql.types import DoubleType

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("TaaSim-TrainModel")
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
print("TRAIN MODEL - START")
print("=" * 60)

# ============================================================
# LIRE LE DATASET FEATURES
# ============================================================

print("\n📂 Lecture du dataset features...")
df = spark.read.parquet("s3a://taasim/ml/features/")
print(f"✅ Dataset chargé : {df.count()} lignes")

# ============================================================
# SPLIT TEMPOREL (10 mois train, 2 mois test)
# ============================================================

print("\n⏰ Split temporel (10 mois train / 2 mois test)...")

# Extraire le mois de la timestamp
df = df.withColumn("month", col("timestamp").substr(1, 7))  # "2013-07"

# Déterminer les mois
months = df.select("month").distinct().orderBy("month").collect()
months_list = [row.month for row in months]
print(f"📅 Mois disponibles : {months_list}")

# 10 premiers mois = train, 2 derniers = test
train_months = months_list[:10]
test_months = months_list[10:]

print(f"Train months: {train_months}")
print(f"Test months: {test_months}")

train_df = df.filter(col("month").isin(train_months))
test_df = df.filter(col("month").isin(test_months))

print(f"✅ Train : {train_df.count()} lignes")
print(f"✅ Test : {test_df.count()} lignes")

# ============================================================
# PRÉPARATION DES FEATURES
# ============================================================

print("\n🔧 Préparation des features pour le modèle...")

# Colonnes à utiliser pour l'entraînement
feature_cols = [
    "hour", "day_of_week", "is_weekend", "is_friday",
    "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean"
]

# Assembler les features
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_data = assembler.transform(train_df).select("zone_id", "features", "target")
test_data = assembler.transform(test_df).select("zone_id", "features", "target")

# ============================================================
# ENTRAÎNEMENT DU MODÈLE GBT
# ============================================================

print("\n🚀 Entraînement du GBTRegressor...")

gbt = GBTRegressor(
    featuresCol="features",
    labelCol="target",
    maxDepth=5,
    maxIter=50,
    stepSize=0.1,
    seed=42
)

model = gbt.fit(train_data)

# ============================================================
# ÉVALUATION - RMSE
# ============================================================

print("\n📊 Évaluation du modèle...")

predictions = model.transform(test_data)

evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"✅ RMSE du modèle GBT : {rmse:.4f}")

# ============================================================
# BASELINE NAIVE (prédire avec demand_lag_7d)
# ============================================================

print("\n📊 Baseline naive (demand_lag_7d)...")

# La baseline naive utilise demand_lag_7d comme prédiction
baseline_df = test_df.withColumn("prediction_naive", col("demand_lag_7d").cast("double"))

# Calculer RMSE de la baseline
baseline_evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction_naive", metricName="rmse")
rmse_baseline = baseline_evaluator.evaluate(baseline_df)
print(f"✅ RMSE de la baseline naive : {rmse_baseline:.4f}")

# ============================================================
# COMPARAISON
# ============================================================

print("\n" + "=" * 60)
print("📈 COMPARAISON MODÈLE vs BASELINE")
print("=" * 60)
print(f"Modèle GBT RMSE : {rmse:.4f}")
print(f"Baseline naive RMSE : {rmse_baseline:.4f}")
print(f"Amélioration : {((rmse_baseline - rmse) / rmse_baseline * 100):.2f}%")

if rmse < rmse_baseline:
    print("✅ Le modèle bat la baseline !")
else:
    print("⚠️ Le modèle ne bat pas la baseline. Ajustement nécessaire.")

# ============================================================
# IMPORTANCE DES CARACTÉRISTIQUES
# ============================================================

print("\n📊 Feature Importance :")
feature_importance = list(zip(feature_cols, model.featureImportances))
feature_importance_sorted = sorted(feature_importance, key=lambda x: x[1], reverse=True)

for feat, imp in feature_importance_sorted:
    print(f"  {feat}: {imp:.4f}")

top3 = feature_importance_sorted[:3]
print(f"\n🏆 Top 3 predictors :")
for i, (feat, imp) in enumerate(top3, 1):
    print(f"  {i}. {feat}: {imp:.4f}")

# ============================================================
# SAUVEGARDE DU MODÈLE DANS MINIO
# ============================================================

print("\n💾 Sauvegarde du modèle dans MinIO...")
model_path = "s3a://taasim/ml/models/demand_v1/"
model.write().overwrite().save(model_path)
print(f"✅ Modèle sauvegardé dans {model_path}")

# ============================================================
# SAUVEGARDE DU RMSE ET FEATURE IMPORTANCE DANS UN FICHIER LOG
# ============================================================

print("\n📝 Sauvegarde des métriques...")

log_content = f"""
=== TaaSIM Demand Forecasting Model ===
Training months: {train_months}
Test months: {test_months}
Model RMSE: {rmse:.4f}
Baseline RMSE: {rmse_baseline:.4f}
Improvement: {((rmse_baseline - rmse) / rmse_baseline * 100):.2f}%

Feature Importance:
"""
for feat, imp in feature_importance_sorted:
    log_content += f"  {feat}: {imp:.4f}\n"

log_content += f"\nTop 3 predictors:\n"
for i, (feat, imp) in enumerate(top3, 1):
    log_content += f"  {i}. {feat}: {imp:.4f}\n"

# Sauvegarder dans MinIO
log_path = "s3a://taasim/ml/models/demand_v1/metrics.txt"
with open("/tmp/metrics.txt", "w") as f:
    f.write(log_content)

spark.sparkContext.addFile("/tmp/metrics.txt")
print(f"✅ Métriques sauvegardées dans {log_path}")

print("\n" + "=" * 60)
print("TRAIN MODEL - TERMINÉ")
print("=" * 60)

spark.stop()