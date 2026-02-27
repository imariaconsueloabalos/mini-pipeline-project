FROM apache/airflow:2.8.1

USER root
# 1. Install system dependencies as root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# 2. Install dbt as the airflow user
# This puts dbt in /home/airflow/.local/bin/dbt
RUN pip install --no-cache-dir dbt-core==1.8.2 dbt-postgres==1.8.2

# 3. Add other requirements
ENV _PIP_ADDITIONAL_REQUIREMENTS="pandas sqlalchemy psycopg2-binary"