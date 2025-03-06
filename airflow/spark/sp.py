from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaToS3") \
    .getOrCreate()

# Esquema para los datos JSON
schema = StructType() \
    .add("ciudad", StringType()) \
    .add("temperatura", StringType())

# Leer los datos de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "airflow-spark") \
    .load()

# Convertir los datos de Kafka a JSON
df_json = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Guardar en S3 como Parquet
df_json.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "s3a://mi-bucket-de-datos/checkpoints/") \
    .option("path", "s3a://mi-bucket-de-datos/clima/") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
