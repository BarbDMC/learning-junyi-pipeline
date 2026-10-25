FROM apache/airflow:2.10.5

COPY requirements-airflow.txt /requirements-airflow.txt

# Pinning apache-airflow stops pip from upgrading/downgrading it while resolving dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt

# dbt gets its own virtualenv because its dependencies can conflict with Airflow's.
# It lives in the user's home because /opt/airflow is covered by the airflow_home volume.
RUN python -m venv /home/airflow/dbt_venv \
    && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.11.0
