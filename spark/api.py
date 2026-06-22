#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from datetime import datetime
import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressionModel

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("TaaSim-API") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "taasim") \
    .config("spark.hadoop.fs.s3a.secret.key", "taasim2024") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ============================================================
# CHARGER LE MODÈLE
# ============================================================
print("📂 Chargement du modèle depuis MinIO...")
model = GBTRegressionModel.load("s3a://taasim/ml/models/demand_v1/")
print("✅ Modèle chargé avec succès.")
print("📋 Nombre de features :", model.numFeatures)
print("📋 Features :", model.featureImportances)

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(title="TaaSim Demand Forecast API")

class ForecastRequest(BaseModel):
    zone_id: int
    datetime: str

class ForecastResponse(BaseModel):
    zone_id: int
    datetime: str
    predicted_demand: float

from pyspark.sql import Row
from pyspark.ml.linalg import Vectors

@app.post("/api/demand/forecast", response_model=ForecastResponse)
async def forecast(request: ForecastRequest):

    start_time = time.time()

    dt = datetime.strptime(
        request.datetime,
        "%Y-%m-%d %H:%M:%S"
    )

    hour = dt.hour
    day_of_week = dt.isoweekday()

    is_weekend = 1 if day_of_week in [6, 7] else 0
    is_friday = 1 if day_of_week == 5 else 0

    features = Vectors.dense([
        float(hour),
        float(day_of_week),
        float(is_weekend),
        float(is_friday),
        100.0,
        100.0,
        100.0
    ])

    spark_df = spark.createDataFrame(
        [Row(features=features)]
    )

    prediction = model.transform(spark_df)

    predicted = prediction.first()["prediction"]

    elapsed = (time.time() - start_time) * 1000

    print(f"Prediction time: {elapsed:.2f} ms")

    return ForecastResponse(
        zone_id=request.zone_id,
        datetime=request.datetime,
        predicted_demand=float(predicted)
    )

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)