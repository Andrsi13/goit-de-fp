FROM python:3.11-slim-bookworm

# Java 17 для Spark 3.5
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir pyspark==3.5.3 requests