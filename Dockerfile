# Airflow 3 image with Java (for Spark), PySpark, the imp package, and dbt in its own virtualenv
# (dbt and Airflow pin conflicting libraries, so they are kept apart).
FROM apache/airflow:3.1.0-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# dbt in an isolated venv
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.10" "dbt-postgres~=1.9" \
 && chown -R airflow: /opt/dbt-venv
ENV DBT_BIN=/opt/dbt-venv/bin/dbt

USER airflow
COPY --chown=airflow:root pyproject.toml /opt/imp/pyproject.toml
COPY --chown=airflow:root pipeline /opt/imp/pipeline
RUN pip install --no-cache-dir "pyspark==4.0.1" -e /opt/imp
