from pyspark.sql import SparkSession
from pyspark.sql.functions import hour

spark = SparkSession.builder \
    .appName("Load Peak Hours") \
    .master("spark://spark-master:7077") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "taasim") \
    .config("spark.hadoop.fs.s3a.secret.key", "taasim2024") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

nyc = spark.read.parquet("s3a://taasim/raw/nyc/")

peak_hours = nyc.withColumn("hour", hour("tpep_pickup_datetime")) \
    .groupBy("hour") \
    .count() \
    .withColumnRenamed("count", "trips") \
    .orderBy("trips", ascending=False)

peak_hours.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("overwrite") \
    .option("confirm.truncate", "true") \
    .options(table="peak_hours", keyspace="taasim") \
    .save()

print("✅ Peak hours loaded successfully!")
spark.stop()