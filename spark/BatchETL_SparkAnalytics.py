#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TaaSim - Week 5 ETL Job
Spark ETL sur Porto et NYC, calcul des KPI, chargement dans Cassandra.
À lancer avec spark-submit, par exemple :
spark-submit --master spark://spark-master:7077 \
             --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0 \
             etl_week5.py
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import h3

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------
def create_spark_session():
    """Crée et retourne la SparkSession avec les configurations nécessaires."""
    spark = (
        SparkSession.builder
        .appName("TaaSim-W5")
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


# -------------------------------------------------------------------
# 2. Traitement Porto
# -------------------------------------------------------------------
def process_porto(spark):
    """
    Lit le fichier CSV Porto, nettoie, ajoute zone_id (simplifié) et H3,
    écrit le résultat en Parquet dans le bucket curated.
    """
    logger.info("Début du traitement Porto")

    PORTO_PATH = "s3a://taasim/raw/porto/porto.csv"
    CURATED_PORTO = "s3a://taasim/curated/porto"

    # Lecture
    porto = (
        spark.read
        .option("header", True)
        .csv(PORTO_PATH)
    )
    logger.info(f"Porto lu : {porto.count()} lignes brutes")

    # Nettoyage
    porto = porto.dropDuplicates(["TRIP_ID"])
    porto = porto.filter(col("POLYLINE").isNotNull())
    porto = porto.filter(length(col("POLYLINE")) > 5)
    logger.info(f"Après nettoyage : {porto.count()} lignes")

    # Durée estimée (15s par point GPS)
    porto = porto.withColumn(
        "trip_duration_sec",
        size(split(col("POLYLINE"), "\\],\\[")) * 15
    )

    # Zone Casablanca simplifiée (hash de TRIP_ID -> 1..16)
    porto = porto.withColumn(
        "zone_id",
        (abs(hash(col("TRIP_ID"))) % 16) + 1
    )

    # H3 (fake mais réel) : chaque zone_id donne une cellule H3 niveau 8
    @udf(StringType())
    def fake_h3(zone):
        # On génère un point représentatif de la zone (décalage arbitraire)
        lat = 33.5 + zone * 0.01
        lon = -7.6 + zone * 0.01
        return h3.latlng_to_cell(lat, lon, 8)

    porto = porto.withColumn(
        "h3_zone",
        fake_h3(col("zone_id"))
    )

    # Écriture en Parquet
    porto.write \
        .mode("overwrite") \
        .parquet(CURATED_PORTO)

    porto.limit(1000).write \
    .mode("overwrite") \
    .parquet("s3a://taasim/curated/porto_sample")
    
    logger.info(f"Porto traité et écrit dans {CURATED_PORTO}")

    # On retourne le DataFrame pour usage ultérieur (KPIs)
    return porto


# -------------------------------------------------------------------
# 3. Traitement NYC
# -------------------------------------------------------------------
def process_nyc(spark):
    """
    Lit les fichiers Parquet NYC, calcule des agrégats par zone de pick-up,
    écrit le résultat en Parquet dans curated.
    """
    logger.info("Début du traitement NYC")

    NYC_PATH = "s3a://taasim/raw/nyc/"
    CURATED_NYC = "s3a://taasim/curated/nyc"

    # Lecture
    nyc = spark.read.parquet(NYC_PATH)
    logger.info(f"NYC lu : {nyc.count()} lignes")

    # Durée du trajet en minutes
    nyc = nyc.withColumn(
        "trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 60
    )

    # Agrégation par zone de départ (PULocationID)
    zone_demand = (
        nyc.groupBy("PULocationID")
           .agg(
               count("*").alias("trip_count"),
               avg("trip_duration_min").alias("avg_duration_min")
           )
    )

    # Écriture
    zone_demand.write \
        .mode("overwrite") \
        .parquet(CURATED_NYC)
    logger.info(f"NYC aggrégé et écrit dans {CURATED_NYC}")

    # On conserve le DataFrame pour les KPIs éventuels (ici on ne l'utilise pas directement)
    return zone_demand


# -------------------------------------------------------------------
# 4. Calcul des KPI avec Spark SQL
# -------------------------------------------------------------------
def compute_kpis(spark, porto_df, nyc_df):
    """
    Utilise Spark SQL pour calculer les KPI demandés :
    - trips par zone, durée moyenne, heures de pointe, coverage gap.
    Retourne un DataFrame agrégé final (trips, avg_duration par zone).
    """
    logger.info("Calcul des KPI")

    # Création des vues temporaires
    porto_df.createOrReplaceTempView("porto")
    nyc_df.createOrReplaceTempView("nyc")  # nyc_df est le DataFrame brut avant agrégation (mais on a aussi un agrégé)

    # Pour les heures de pointe, on a besoin de l'heure de pick-up ; on ajoute la colonne si pas présente
    # On travaille sur le DataFrame NYC brut (qu'on a conservé dans nyc_df)
    # Mais on a déjà un DataFrame nyc_brut ? Dans le notebook on a créé une nouvelle colonne 'hour' sur nyc.
    # Pour éviter de modifier le DataFrame original, on clone ou on crée une vue avec la colonne calculée.
    # On va recréer une vue avec la colonne hour.
    nyc_with_hour = nyc_df.withColumn("hour", hour("tpep_pickup_datetime"))
    nyc_with_hour.createOrReplaceTempView("nyc_with_hour")

    # KPI 1 : Trips par zone
    kpi_trips = spark.sql("""
        SELECT zone_id, COUNT(*) AS trips
        FROM porto
        GROUP BY zone_id
        ORDER BY zone_id
    """)
    logger.info("KPI Trips par zone :")
    kpi_trips.show(truncate=False)

    # KPI 2 : Durée moyenne
    kpi_duration = spark.sql("""
        SELECT AVG(trip_duration_sec)/60 AS avg_minutes
        FROM porto
    """)
    logger.info("KPI Durée moyenne :")
    kpi_duration.show()

    # KPI 3 : Heures de pointe (basé sur NYC)
    peak_hours = spark.sql("""
        SELECT hour, COUNT(*) AS demand
        FROM nyc_with_hour
        GROUP BY hour
        ORDER BY demand DESC
    """)
    logger.info("KPI Heures de pointe :")
    peak_hours.show(truncate=False)

    # KPI 4 : Coverage gap (zones avec demande mais < 2 véhicules)
    # Ici on simule un seul véhicule par zone pour l'exemple, comme dans le notebook
    coverage_gap = (
        porto_df.groupBy("zone_id")
                .agg(count("*").alias("demand"))
                .withColumn("vehicles", lit(1))  # simulation : 1 véhicule par zone
                .filter(col("vehicles") < 2)
    )
    logger.info("KPI Coverage gap :")
    coverage_gap.show()

    # KPI final : agrégat par zone (trips, avg_duration) pour Cassandra
    kpi_final = (
        porto_df.groupBy("zone_id")
                .agg(
                    count("*").alias("trips"),
                    avg("trip_duration_sec").alias("avg_duration")
                )
    )
    logger.info("KPI final (pour Cassandra) :")
    kpi_final.show()

    return kpi_final


# -------------------------------------------------------------------
# 5. Chargement dans Cassandra
# -------------------------------------------------------------------
def load_to_cassandra(kpi_final_df):
    """
    Crée le keyspace et la table demand_zones dans Cassandra,
    puis insère les données du DataFrame kpi_final.
    """
    logger.info("Chargement dans Cassandra")

    from cassandra.cluster import Cluster

    # Connexion à Cassandra
    cluster = Cluster(["cassandra"])
    session = cluster.connect()

    # Création du keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS taasim
        WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}
    """)

    # Création de la table
    session.execute("""
        CREATE TABLE IF NOT EXISTS taasim.demand_zones (
            zone_id int PRIMARY KEY,
            trips bigint,
            avg_duration double
        )
    """)
    logger.info("Keyspace et table créés (ou déjà existants)")

    # Écriture via Spark Cassandra Connector
    kpi_final_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="demand_zones", keyspace="taasim") \
        .save()
    logger.info("Données insérées dans Cassandra")

    # Vérification rapide
    rows = session.execute("SELECT * FROM taasim.demand_zones LIMIT 10")
    logger.info("Échantillon des données dans Cassandra :")
    for row in rows:
        logger.info(f"zone_id={row.zone_id}, trips={row.trips}, avg_duration={row.avg_duration}")

    session.shutdown()
    cluster.shutdown()


# -------------------------------------------------------------------
# 6. Fonction principale
# -------------------------------------------------------------------
def main():
    spark = create_spark_session()
    try:
        # Étape 1 : Porto
        porto_df = process_porto(spark)

        # Étape 2 : NYC
        nyc_df = process_nyc(spark)

        # Étape 3 : KPI
        kpi_final = compute_kpis(spark, porto_df, nyc_df)

        # Étape 4 : Chargement Cassandra
        load_to_cassandra(kpi_final)

        logger.info("ETL Week 5 terminé avec succès.")

    except Exception as e:
        logger.error(f"Erreur lors de l'exécution : {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()