# Airflow 3 image with the imp package and dbt (in its own venv: dbt and Airflow pin conflicting libraries).
#
# WITH_SPARK=true  (default) adds Java 17 + PySpark   -> Silver built by Spark      (~+700 MB)
# WITH_SPARK=false skips both                           -> Silver built by pandas    (identical output, smaller image)
FROM apache/airflow:3.1.0-python3.12

ARG WITH_SPARK=true

USER root
RUN if [ "$WITH_SPARK" = "true" ]; then \
      apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
      && apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.10" "dbt-postgres~=1.9" \
 && find /opt/dbt-venv -name "__pycache__" -prune -exec rm -rf {} + \
 && chown -R airflow: /opt/dbt-venv
ENV DBT_BIN=/opt/dbt-venv/bin/dbt

USER airflow
COPY --chown=airflow:root pyproject.toml /opt/imp/pyproject.toml
COPY --chown=airflow:root pipeline /opt/imp/pipeline
# without PySpark installed, imp automatically uses the pandas Silver engine
RUN pip install --no-cache-dir -e /opt/imp \
 && if [ "$WITH_SPARK" = "true" ]; then pip install --no-cache-dir "pyspark==4.0.1"; fi
