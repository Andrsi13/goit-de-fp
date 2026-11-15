import os

import requests
from pyspark.sql import SparkSession

BASE_DIR = os.getenv("DATALAKE_DIR", "/opt/project/datalake")
LANDING_DIR = os.path.join(BASE_DIR, "landing")
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")

TABLES = ["athlete_bio", "athlete_event_results"]


# Stage 1.a: завантаження файлу з FTP-сервера в оригінальному форматі csv
def download_data(table):
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + table + ".csv"
    local_file_path = os.path.join(LANDING_DIR, table + ".csv")
    print(f"Downloading from {downloading_url}")

    response = requests.get(downloading_url, timeout=300)

    # Перевіряємо, чи запит успішний
    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")

    return local_file_path


def main():
    os.makedirs(LANDING_DIR, exist_ok=True)
    os.makedirs(BRONZE_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        csv_path = download_data(table)

        # Stage 1.b: читання csv за допомогою Spark
        df = (
            spark.read
            .option("header", True)
            .option("multiLine", True)   
            .option("escape", '"')
            .csv(csv_path)
        )

        # Stage 1.c: збереження у форматі parquet у bronze/{table}
        output_path = os.path.join(BRONZE_DIR, table)
        df.write.mode("overwrite").parquet(output_path)

        print(f"[STAGE 1] {table}: {df.count()} рядків збережено в {output_path}")
        df.show()   # фінальний DataFrame для скріншоту з логів Airflow

    spark.stop()


if __name__ == "__main__":
    main()
