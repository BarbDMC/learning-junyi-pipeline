FROM apache/airflow:2.10.5

COPY requirements.txt /requirements.txt

# Pinning apache-airflow stops pip from upgrading/downgrading it while resolving dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
