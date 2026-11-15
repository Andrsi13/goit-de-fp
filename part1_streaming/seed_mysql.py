"""
Етап 0 (підготовка): завантажуємо локальні CSV у MySQL,
щоб далі працювати з таблицями olympic_dataset.athlete_bio
та olympic_dataset.athlete_event_results, як в умові проєкту.

Запуск:  docker compose exec spark python part1_streaming/seed_mysql.py
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from config import JDBC_URL, JDBC_PROPS, SPARK_PACKAGES

DATA_DIR = "/opt/project/data"
TABLES = ["athlete_bio", "athlete_event_results"]

spark = (
    SparkSession.builder
    .appName("SeedMySQL")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

for table in TABLES:
    path = f"{DATA_DIR}/{table}.csv"
    # Без inferSchema: залишаємо колонки рядками, щоб на Етапі 2
    # чесно відфільтрувати "не числа" у height / weight
    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)   # в описах атлетів бувають переноси рядків
        .option("escape", '"')
        .csv(path)
    )
    print(f"{table}: {df.count()} рядків")
    df.printSchema()

    df.write.jdbc(url=JDBC_URL, table=table, mode="overwrite", properties=JDBC_PROPS)
    print(f"{table} записано в MySQL")

spark.stop()