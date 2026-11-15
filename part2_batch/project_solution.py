"""
Фінальний проєкт, Частина 2 — Stage 4: Airflow DAG,
який послідовно запускає три Spark job:
landing_to_bronze -> bronze_to_silver -> silver_to_gold
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JOBS_DIR = "/opt/project/part2_batch"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 11, 1),
}

with DAG(
    dag_id="project_solution",
    default_args=default_args,
    schedule=None,          # запуск вручну з UI
    catchup=False,
    tags=["goit-de-fp"],
) as dag:

    # Stage 4.a: FTP -> landing -> bronze
    landing_to_bronze = SparkSubmitOperator(
        application=f"{JOBS_DIR}/landing_to_bronze.py",
        task_id="landing_to_bronze",
        conn_id="spark-default",
        verbose=1,
    )

    # Stage 4.b: bronze -> silver
    bronze_to_silver = SparkSubmitOperator(
        application=f"{JOBS_DIR}/bronze_to_silver.py",
        task_id="bronze_to_silver",
        conn_id="spark-default",
        verbose=1,
    )

    # Stage 4.c: silver -> gold
    silver_to_gold = SparkSubmitOperator(
        application=f"{JOBS_DIR}/silver_to_gold.py",
        task_id="silver_to_gold",
        conn_id="spark-default",
        verbose=1,
    )

    # Послідовний запуск
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
