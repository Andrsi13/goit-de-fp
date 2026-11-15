import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from config import (
    JDBC_URL, JDBC_DRIVER, MYSQL_USER, MYSQL_PASSWORD,
    KAFKA_BOOTSTRAP, INPUT_TOPIC, OUTPUT_TOPIC, SPARK_PACKAGES,
)

OUTPUT_TABLE = "athlete_enriched_agg"
CHECKPOINT_DIR = "/opt/project/checkpoints/streaming_pipeline"


spark = (
    SparkSession.builder
    .appName("AthleteStreamingPipeline")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


# Stage 1: зчитування біологічних даних атлетів з MySQL (olympic_dataset.athlete_bio)
athlete_bio_df = (
    spark.read.format("jdbc")
    .option("url", JDBC_URL)
    .option("driver", JDBC_DRIVER)
    .option("dbtable", "athlete_bio")
    .option("user", MYSQL_USER)
    .option("password", MYSQL_PASSWORD)
    .load()
)

print(f"[STAGE 1] athlete_bio: {athlete_bio_df.count()} рядків")
athlete_bio_df.printSchema()


# Stage 2: фільтрація — відкидаємо рядки, де height / weight порожні або не числа
# (cast("double") повертає null для порожніх і нечислових значень)
athlete_bio_clean_df = (
    athlete_bio_df
    .withColumn("height", F.col("height").cast("double"))
    .withColumn("weight", F.col("weight").cast("double"))
    .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())
    .select("athlete_id", "sex", "country_noc", "height", "weight")
    .cache()   # статична таблиця використовується в кожному мікробатчі
)

print(f"[STAGE 2] athlete_bio після фільтрації: {athlete_bio_clean_df.count()} рядків")
athlete_bio_clean_df.show(10, truncate=False)


# Stage 3: читання результатів змагань з Kafka-топіку та перетворення JSON у колонки
# (запис з MySQL у Kafka-топік — Stage 3.1 — виконується в mysql_to_kafka.py)
event_fields = [
    "edition", "edition_id", "country_noc", "sport", "event",
    "result_id", "athlete", "athlete_id", "pos", "medal", "isTeamSport",
]
event_schema = StructType([StructField(name, StringType(), True) for name in event_fields])

events_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)   # ~7 мікробатчів на весь топік
    .option("failOnDataLoss", "false")
    .load()
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("data"))
    .select("data.*")
    .drop("country_noc")   # country_noc беремо з біоданих атлета
)


# Stage 4: об'єднання результатів змагань з біоданими за ключем athlete_id
enriched_stream_df = events_stream_df.join(athlete_bio_clean_df, on="athlete_id", how="inner")


# Stage 5: середній зріст і вага для кожної комбінації sport / medal / sex / country_noc
# + timestamp розрахунку; відсутність медалі позначаємо як "nan"
aggregated_stream_df = (
    enriched_stream_df
    .withColumn("medal", F.coalesce(F.col("medal"), F.lit("nan")))
    .groupBy("sport", "medal", "sex", "country_noc")
    .agg(
        F.avg("height").alias("avg_height"),
        F.avg("weight").alias("avg_weight"),
    )
    .withColumn("timestamp", F.current_timestamp())
)


# Stage 6: стрим даних за допомогою forEachBatch у вихідний Kafka-топік і базу даних
def foreach_batch_function(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df.persist()   # батч пишемо у два місця — не рахуємо його двічі
    print(f"[STAGE 6] Мікробатч {batch_id}: {batch_df.count()} агрегованих рядків")
    batch_df.show(10, truncate=False)

    # Stage 6.a: запис у вихідний Kafka-топік (кожен рядок -> JSON)
    (
        batch_df
        .select(F.to_json(F.struct(*batch_df.columns)).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", OUTPUT_TOPIC)
        .save()
    )

    # Stage 6.b: запис у базу даних MySQL
    (
        batch_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", OUTPUT_TABLE)
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .mode("append")
        .save()
    )

    batch_df.unpersist()


query = (
    aggregated_stream_df.writeStream
    .foreachBatch(foreach_batch_function)
    .outputMode("update")   # у кожному батчі — тільки змінені агрегати
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

query.awaitTermination()