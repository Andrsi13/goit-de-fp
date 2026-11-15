import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    JDBC_URL, JDBC_DRIVER, MYSQL_USER, MYSQL_PASSWORD,
    KAFKA_BOOTSTRAP, INPUT_TOPIC, SPARK_PACKAGES,
)

spark = (
    SparkSession.builder
    .appName("MySQLToKafka")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Stage 3.1 читаємо результати змагань з MySQL
events_df = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("driver", JDBC_DRIVER)
    .option("dbtable", "athlete_event_results")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)
print(f"[Stage 3.1] athlete_event_results з MySQL: {events_df.count()} рядків")

kafka_df = events_df.select(
    F.col("athlete_id").cast("string").alias("key"),
    F.to_json(F.struct(*events_df.columns)).alias("value"),
)
kafka_df.show(3, truncate=False)

(
    kafka_df.write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", INPUT_TOPIC)
    .save()
)
print(f"[Stage 3.1] Дані записано в Kafka-топік '{INPUT_TOPIC}'")

spark.stop()