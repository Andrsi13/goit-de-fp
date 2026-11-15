"""Спільні налаштування для Частини 1 (значення беруться з docker-compose)."""
import os

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_DB = os.getenv("MYSQL_DB", "olympic_dataset")
MYSQL_USER = os.getenv("MYSQL_USER", "de")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "de_pass")

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:3306/{MYSQL_DB}"
JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
JDBC_PROPS = {"user": MYSQL_USER, "password": MYSQL_PASSWORD, "driver": JDBC_DRIVER}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = "athlete_event_results"    # вхідний топік з результатами змагань
OUTPUT_TOPIC = "athlete_enriched_agg"    # вихідний топік з агрегатами (Етап 6.a)

# MySQL JDBC-драйвер (той самий 8.0.32, що в умові) + Kafka-конектор для Spark 3.5.3
SPARK_PACKAGES = ",".join([
    "com.mysql:mysql-connector-j:8.0.32",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
])