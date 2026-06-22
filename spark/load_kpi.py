#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("TaaSim-W5-KPI")
        .master("spark://spark-master:7077")
        .config("spark.cassandra.connection.host", "cassandra")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "taasim")
        .config("spark.hadoop.fs.s3a.secret.key", "taasim2024")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def main():
    spark = create_spark_session()
    try:
        # Lire Porto depuis curated
        logger.info("Lecture Porto depuis curated...")
        porto_df = spark.read.parquet("s3a://taasim/curated/porto/")
        logger.info(f"Porto chargé : {porto_df.count()} lignes")

        # Lire NYC brut pour les heures de pointe
        logger.info("Lecture NYC brut depuis raw...")
        nyc_brut = spark.read.parquet("s3a://taasim/raw/nyc/")
        nyc_brut = nyc_brut.withColumn("hour", hour("tpep_pickup_datetime"))
        logger.info(f"NYC chargé : {nyc_brut.count()} lignes")

        # Créer les vues temporaires
        porto_df.createOrReplaceTempView("porto")
        nyc_brut.createOrReplaceTempView("nyc")

        # KPI 1
        logger.info("KPI Trips par zone :")
        spark.sql("SELECT zone_id, COUNT(*) AS trips FROM porto GROUP BY zone_id ORDER BY zone_id").show(truncate=False)

        # KPI 2
        logger.info("KPI Durée moyenne :")
        spark.sql("SELECT AVG(trip_duration_sec)/60 AS avg_minutes FROM porto").show()

        # KPI 3
        logger.info("KPI Heures de pointe :")
        spark.sql("SELECT hour, COUNT(*) AS demand FROM nyc GROUP BY hour ORDER BY demand DESC").show(truncate=False)

        # KPI 4
        coverage_gap = (
            porto_df.groupBy("zone_id")
                    .agg(count("*").alias("demand"))
                    .withColumn("vehicles", lit(1))
                    .filter(col("vehicles") < 2)
        )
        logger.info("KPI Coverage gap :")
        coverage_gap.show()

        # KPI final pour Cassandra
        kpi_final = (
            porto_df.groupBy("zone_id")
                    .agg(
                        count("*").alias("trips"),
                        avg("trip_duration_sec").alias("avg_duration")
                    )
        )
        logger.info("KPI final (pour Cassandra) :")
        kpi_final.show()

        # Chargement dans Cassandra
        logger.info("Chargement dans Cassandra...")
        from cassandra.cluster import Cluster

        cluster = Cluster(["cassandra"], protocol_version=4)  # version 4 pour compatibilité
        session = cluster.connect()

        # Créer keyspace et table si besoin
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS taasim
            WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS taasim.demand_zones (
                zone_id int PRIMARY KEY,
                trips bigint,
                avg_duration double
            )
        """)

        # Écriture en mode append (pas overwrite)
        kpi_final.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="demand_zones", keyspace="taasim") \
            .save()
        logger.info("Données insérées dans Cassandra (mode append)")

        # Vérification rapide
        rows = session.execute("SELECT * FROM taasim.demand_zones LIMIT 10")
        for row in rows:
            logger.info(f"zone_id={row.zone_id}, trips={row.trips}, avg_duration={row.avg_duration}")

        session.shutdown()
        cluster.shutdown()

        logger.info("KPIs terminés avec succès.")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()