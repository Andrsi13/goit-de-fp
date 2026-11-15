import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

BASE_DIR = os.getenv("DATALAKE_DIR", "/opt/project/datalake")
BRONZE_DIR = os.path.join(BASE_DIR, "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "silver")

TABLES = ["athlete_bio", "athlete_event_results"]

NUMERIC_COLUMNS = ["height", "weight"]


# функція очистки тексту.
def clean_text(text):
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())


def main():
    os.makedirs(SILVER_DIR, exist_ok=True)

    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for table in TABLES:
        # Stage 2.a: зчитування таблиці з bronze
        df = spark.read.parquet(os.path.join(BRONZE_DIR, table))
        rows_before = df.count()

        # Stage 2.b: числові колонки
        for col_name in NUMERIC_COLUMNS:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast("double"))

        # Stage 2.c: чистка тексту для всіх текстових колонок
        text_columns = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
        for col_name in text_columns:
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

        # Stage 2.d: дедуплікація рядків
        df = df.dropDuplicates()

        # Stage 2.e: запис таблиці в silver/{table}
        output_path = os.path.join(SILVER_DIR, table)
        df.write.mode("overwrite").parquet(output_path)

        df = spark.read.parquet(output_path)
        print(
            f"[STAGE 2] {table}: {rows_before} рядків у bronze -> "
            f"{df.count()} після дедуплікації, збережено в {output_path}"
        )
        df.show() 

    spark.stop()


if __name__ == "__main__":
    main()
