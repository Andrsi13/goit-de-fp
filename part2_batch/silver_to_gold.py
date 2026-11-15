import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.getenv("DATALAKE_DIR", "/opt/project/datalake")
SILVER_DIR = os.path.join(BASE_DIR, "silver")
GOLD_DIR = os.path.join(BASE_DIR, "gold")


def main():
    os.makedirs(GOLD_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Stage 3.a: зчитування двох таблиць з silver
    athlete_bio_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_bio"))
    athlete_event_results_df = spark.read.parquet(os.path.join(SILVER_DIR, "athlete_event_results"))

    # Колонки для математичних перетворень мають бути числовими:
    athlete_bio_df = (
        athlete_bio_df
        .withColumn("height", F.col("height").cast("double"))
        .withColumn("weight", F.col("weight").cast("double"))
        .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())
        .select("athlete_id", "sex", "country_noc", "height", "weight")
    )

    # country_noc є в обох таблицях - беремо його з біоданих атлета
    athlete_event_results_df = athlete_event_results_df.drop("country_noc")

    # Stage 3.b: join за колонкою athlete_id
    joined_df = athlete_event_results_df.join(athlete_bio_df, on="athlete_id", how="inner")

    # Stage 3.c: середні weight і height для кожної комбінації sport, medal, sex, country_noc
    # Stage 3.d: колонка timestamp з часовою міткою виконання програми
    avg_stats_df = (
        joined_df
        .withColumn("medal", F.coalesce(F.col("medal"), F.lit("nan"))) 
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            F.avg("weight").alias("avg_weight"),
            F.avg("height").alias("avg_height"),
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    # Stage 3.e: запис даних у gold/avg_stats
    output_path = os.path.join(GOLD_DIR, "avg_stats")
    avg_stats_df.write.mode("overwrite").parquet(output_path)

    df = spark.read.parquet(output_path)
    print(f"[STAGE 3] avg_stats: {df.count()} рядків збережено в {output_path}")
    df.show()  

    spark.stop()


if __name__ == "__main__":
    main()
