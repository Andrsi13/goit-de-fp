FROM apache/airflow:2.10.5-python3.11

# Java 17 для Spark
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Spark-провайдер (SparkSubmitOperator) + PySpark, версії узгоджені з Airflow 2.10.5
USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    apache-airflow-providers-apache-spark \
    pyspark \
    requests \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"
