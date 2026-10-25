import os
import io
import itertools
import subprocess
from datetime import datetime
import psycopg2
from minio import Minio
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

# Credentials come from .env, injected by docker-compose; MinIO is Docker-internal
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
BUCKET_NAME = os.environ["MINIO_BUCKET"]

def _row_limit(var):
    return int(os.environ[var]) if os.environ.get(var) else None

# Warehouses the DAG can target, chosen per run with the `target` param
WAREHOUSES = {
    "local": {
        "host": "postgres",
        "port": 5432,
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
        "sslmode": "disable",
        "log_problem_row_limit": _row_limit("LOCAL_LOG_PROBLEM_ROW_LIMIT"),
    },
    "supabase": {
        "host": os.environ["SUPABASE_HOST"],
        "port": int(os.environ["SUPABASE_PORT"]),
        "dbname": os.environ["SUPABASE_DB"],
        "user": os.environ["SUPABASE_USER"],
        "password": os.environ["SUPABASE_PASSWORD"],
        "sslmode": os.environ["SUPABASE_SSLMODE"],
        "log_problem_row_limit": _row_limit("SUPABASE_LOG_PROBLEM_ROW_LIMIT"),
    },
}
DEFAULT_TARGET = os.environ.get("WAREHOUSE_TARGET") or "local"

DATA_DIR = "/opt/airflow/data"
MIGRATIONS_DIR = "/opt/airflow/migrations"
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_BIN = "/home/airflow/dbt_venv/bin/dbt"

# (file, table, whether the target's Log_Problem row limit applies)
FILES_AND_TABLES = [
    ("Info_UserData.csv", "raw.info_user_data", False),
    ("Info_Content.csv", "raw.info_content", False),
    ("Log_Problem.csv", "raw.log_problem", True),
]

class LineLimitedReader:
    """File-like object that yields the CSV header plus at most `max_rows` data lines.

    Only safe for files without multi-line quoted fields, which holds for Log_Problem.csv.
    """

    def __init__(self, stream, max_rows):
        self._lines = itertools.islice(stream, max_rows + 1)
        self._buffer = ""

    def read(self, size=-1):
        while size < 0 or len(self._buffer) < size:
            line = next(self._lines, None)
            if line is None:
                break
            self._buffer += line
        if size < 0:
            size = len(self._buffer)
        chunk, self._buffer = self._buffer[:size], self._buffer[size:]
        return chunk

def get_warehouse(params):
    target = params["target"]
    warehouse = WAREHOUSES[target]
    missing = [key for key in ("host", "user", "password") if not warehouse[key]]
    if missing:
        raise ValueError(
            f"Warehouse '{target}' is missing {', '.join(missing)}. "
            "Set them in .env and run `docker compose up -d airflow`."
        )
    print(f"Target warehouse: {target} ({warehouse['host']}:{warehouse['port']}/{warehouse['dbname']})")
    return warehouse

def get_pg_connection(warehouse):
    return psycopg2.connect(
        host=warehouse["host"],
        port=warehouse["port"],
        dbname=warehouse["dbname"],
        user=warehouse["user"],
        password=warehouse["password"],
        sslmode=warehouse["sslmode"],
    )

def run_db_migrations(params):
    """Find and execute pending .sql migration files in alphabetical order."""
    conn = get_pg_connection(get_warehouse(params))
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
    for file_name, _, _ in FILES_AND_TABLES:
        file_path = os.path.join(DATA_DIR, file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found in container: {file_path}")
        
        print(f"Uploading {file_name} to MinIO bucket '{BUCKET_NAME}'...")
        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=f"landing/{file_name}",
            file_path=file_path,
        )

def load_minio_to_postgres(params):
    client = get_minio_client()
    warehouse = get_warehouse(params)
    row_limit = warehouse["log_problem_row_limit"]
    conn = get_pg_connection(warehouse)
    cursor = conn.cursor()

    try:
        for file_name, table_name, is_limited in FILES_AND_TABLES:
            max_rows = row_limit if is_limited else None
            object_name = f"landing/{file_name}"
            print(f"Processing {object_name} -> {table_name} (row limit: {max_rows or 'none'})...")

            # 1. Truncate table to ensure idempotent loads
            cursor.execute(f"TRUNCATE TABLE {table_name};")

            # 2. Stream data directly from MinIO without saving to disk
            response = client.get_object(BUCKET_NAME, object_name)
            # urllib3 2.x closes the response at EOF; TextIOWrapper then fails on its final read()
            response.auto_close = False
            try:
                # 3. Wrap binary stream into an in-memory text stream
                text_stream = io.TextIOWrapper(response, encoding='utf-8', errors='replace')
                if max_rows is not None:
                    text_stream = LineLimitedReader(text_stream, max_rows)

                # 4. Use native PostgreSQL COPY for high-performance bulk loading
                cursor.copy_expert(
                    sql=f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'",
                    file=text_stream
                )
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                print(f"Loaded {cursor.fetchone()[0]:,} rows into {table_name}.")
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

def run_dbt(params):
    """Run `dbt run` against the target warehouse; profiles.yml reads the WAREHOUSE_* vars.

    Credentials go through the subprocess env, not a templated BashOperator field,
    so they never show up in the UI's rendered templates.
    """
    warehouse = get_warehouse(params)
    env = {
        **os.environ,
        "WAREHOUSE_HOST": warehouse["host"],
        "WAREHOUSE_PORT": str(warehouse["port"]),
        "WAREHOUSE_DB": warehouse["dbname"],
        "WAREHOUSE_USER": warehouse["user"],
        "WAREHOUSE_PASSWORD": warehouse["password"],
        "WAREHOUSE_SSLMODE": warehouse["sslmode"],
    }
    # target/ and logs go to /tmp, not the mounted project
    work_dir = f"/tmp/dbt/{params['target']}"
    command = [
        DBT_BIN, "run",
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROJECT_DIR,
        "--target-path", f"{work_dir}/target",
        "--log-path", f"{work_dir}/logs",
    ]
    # Print dbt's output line by line so it lands in the Airflow task log
    with subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) as process:
        for line in process.stdout:
            print(line, end="")
    if process.returncode != 0:
        raise RuntimeError(f"dbt run failed with exit code {process.returncode}")

with DAG(
    dag_id="ingest_junyi_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger on demand
    catchup=False,
    tags=["junyi", "ingestion", "raw", "dbt"],
    params={
        "target": Param(
            DEFAULT_TARGET,
            type="string",
            enum=list(WAREHOUSES),
            description="Warehouse to load and transform: local Postgres container or Supabase",
        ),
    },
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

    # Builds the staging and marts schemas
    task_dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
    )

    # Pipeline task dependencies
    task_migrate_db >> task_upload_to_minio >> task_load_to_postgres >> task_dbt_run
