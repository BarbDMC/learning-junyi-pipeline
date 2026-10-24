FROM apache/airflow:2.10.5

COPY requirements.txt /requirements.txt

# Fijar apache-airflow evita que pip lo actualice/degrade al resolver dependencias
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
