import os
import io
from datetime import datetime
import psycopg2
from minio import Minio
from airflow import DAG
from airflow.operators.python import PythonOperator

# Docker internal connection parameters
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "junyi-raw"

PG_HOST = "postgres"
PG_PORT = 5432
PG_DB = "warehouse"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

DATA_DIR = "/opt/airflow/data"
MIGRATIONS_DIR = "/opt/airflow/migrations"

FILES_AND_TABLES = [
    ("Info_UserData.csv", "raw.info_user_data"),
    ("Info_Content.csv", "raw.info_content"),
    ("Log_Problem.csv", "raw.log_problem"),
]

def get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def run_db_migrations():
    """Find and execute pending .sql migration files in alphabetical order."""
    conn = get_pg_connection()
    cursor = conn.cursor()
    try:
        # Ensure schema migration tracking table exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS public.schema_migrations (
                version VARCHAR(50) PRIMARY KEY,
                applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

        # Fetch already applied migrations
        cursor.execute("SELECT version FROM public.schema_migrations;")
        applied = {row[0] for row in cursor.fetchall()}

        # Read migration files sorted alphabetically
        migration_files = sorted(
            [f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql")]
        )

        for filename in migration_files:
            if filename not in applied:
                print(f"Applying migration: {filename}...")
                filepath = os.path.join(MIGRATIONS_DIR, filename)
                with open(filepath, "r", encoding="utf-8") as f:
                    sql_script = f.read()
                
                cursor.execute(sql_script)
                cursor.execute(
                    "INSERT INTO public.schema_migrations (version) VALUES (%s);",
                    (filename,)
                )
                conn.commit()
                print(f"Migration {filename} applied successfully.")
            else:
                print(f"Migration {filename} already applied. Skipping.")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def upload_local_to_minio():
    client = get_minio_client()
    for file_name, _ in FILES_AND_TABLES:
        file_path = os.path.join(DATA_DIR, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found in container: {file_path}")
        
        print(f"Uploading {file_name} to MinIO bucket '{BUCKET_NAME}'...")
        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=f"landing/{file_name}",
            file_path=file_path,
        )

def load_minio_to_postgres():
    client = get_minio_client()
    conn = get_pg_connection()
    cursor = conn.cursor()

    try:
        for file_name, table_name in FILES_AND_TABLES:
            object_name = f"landing/{file_name}"
            print(f"Processing {object_name} -> {table_name}...")

            # 1. Truncate table to ensure idempotent loads
            cursor.execute(f"TRUNCATE TABLE {table_name};")

            # 2. Stream data directly from MinIO without saving to disk
            response = client.get_object(BUCKET_NAME, object_name)
            # urllib3 2.x closes the response at EOF; TextIOWrapper then fails on its final read()
            response.auto_close = False
            try:
                # 3. Wrap binary stream into an in-memory text stream
                text_stream = io.TextIOWrapper(response, encoding='utf-8', errors='replace')

                # 4. Use native PostgreSQL COPY for high-performance bulk loading
                cursor.copy_expert(
                    sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'",
                    file=text_stream
                )
            finally:
                response.close()
                response.release_conn()
            print(f"Data loading complete for {table_name}.")

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

with DAG(
    dag_id="ingest_junyi_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger on demand
    catchup=False,
    tags=["junyi", "ingestion", "raw"],
) as dag:

    task_migrate_db = PythonOperator(
        task_id="run_db_migrations",
        python_callable=run_db_migrations,
    )

    task_upload_to_minio = PythonOperator(
        task_id="upload_local_to_minio",
        python_callable=upload_local_to_minio,
    )

    task_load_to_postgres = PythonOperator(
        task_id="load_minio_to_postgres",
        python_callable=load_minio_to_postgres,
    )

    # Pipeline task dependencies
    task_migrate_db >> task_upload_to_minio >> task_load_to_postgres
