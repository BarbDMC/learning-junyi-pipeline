# Junyi Learning Pipeline

A local, end-to-end data engineering project built to learn the full path from raw files to a dashboard: object storage, orchestration, a warehouse, dbt transformations and a BI layer, all running on a laptop with Docker.

It uses the public [Junyi Academy Online Learning Activity Dataset](https://www.kaggle.com/datasets/junyiacademy/learning-activity-public-dataset-by-junyi-academy): about 16 million problem attempts from roughly 72,000 students on a Taiwanese online learning platform (August 2018 to July 2019).

## The problem

The dataset ships as three CSV files, one of them about 3 GB. In that form it can't answer simple questions without loading everything into memory:

- How often do students get a problem right on the first try?
- How much effort (attempts, hints) does each difficulty level take?
- At what time of day are students most active?
- How does performance change across learning stages?

The goal is to turn those files into a queryable star schema in a warehouse, with a repeatable, orchestrated load, and put a dashboard on top. Each layer uses the kind of tool you'd find in a real data stack.

## Architecture

```
  data/*.csv
      │   Airflow DAG: ingest_junyi_data
      ▼
  ┌─────────────────┐   upload_local_to_minio
  │ MinIO           │   s3://junyi-raw/landing/*.csv
  │ (data lake)     │
  └────────┬────────┘
           │   load_minio_to_postgres (streamed COPY)
           ▼
  ┌─────────────────┐
  │ PostgreSQL 15   │   raw.*      ← exact copy of the CSVs (all TEXT)
  │ "warehouse"     │   staging.*  ← dbt views: clean, typed, renamed
  │                 │   marts.*    ← dbt tables: star schema
  └────────┬────────┘
           ▼
  ┌─────────────────┐
  │ Streamlit app   │   app.py
  └─────────────────┘
```

The DAG runs four tasks in order:

1. **`run_db_migrations`** applies any pending `.sql` file in `migrations/` and records it in `public.schema_migrations`.
2. **`upload_local_to_minio`** uploads the CSVs from `data/` to the `junyi-raw` bucket under `landing/`.
3. **`load_minio_to_postgres`** truncates each `raw.*` table and streams the matching file from MinIO straight into Postgres with `COPY`, without writing it to disk. Each warehouse can cap how many `Log_Problem` rows are loaded.
4. **`dbt_run`** runs `dbt run` inside the Airflow container, creating the `staging` and `marts` schemas and every model.

Each run picks its warehouse with the DAG's `target` parameter: `local` for the Postgres container, or `supabase` for a cloud database (see [Using Supabase](#using-supabase-as-the-warehouse)).

## Tech stack

| Layer | Tool | Notes |
|---|---|---|
| Containers | Docker Compose (Colima on macOS) | One network, `de-network`, for all services |
| Data lake | MinIO via [`pgsty/silo`](https://hub.docker.com/r/pgsty/silo) | Community fork; official MinIO images are gone (see FAQ) |
| Orchestration | Apache Airflow 2.10.5, `standalone` | Custom image with `minio`, `psycopg2-binary` and dbt |
| Warehouse | PostgreSQL 15 (local) or Supabase | Chosen per run with the DAG's `target` parameter |
| Transformations | dbt-core 1.12 + dbt-postgres 1.11 | Own virtualenv inside the Airflow image |
| Dashboard | Streamlit 1.50 + Plotly | Reads `marts.*` directly |

## Data model

Staging models (`staging` schema, views) clean the raw tables: they cast types, turn empty strings into `NULL`, convert `'True'/'False'` text into booleans and give columns clear names.

Mart models (`marts` schema, tables) form a star schema:

| Table | Grain | Rows |
|---|---|---|
| `fct_student_problem_attempts` | One row per problem a student worked on | 16,217,311 |
| `dim_user` | One row per student | 72,758 |
| `dim_content` | One row per exercise | 1,330 |
| `dim_date` | One row per calendar day (2015 to 2025) | 4,018 |
| `dim_time` | One row per hour of the day | 24 |

The fact table joins to the dimensions through surrogate keys: `MD5()` of the natural ID for users and content, `YYYYMMDD` for dates, and the hour (0 to 23) for time.

## Getting started

### Prerequisites

- Docker with the Compose v2 plugin (`docker compose version` must work). On macOS with Colima, give the VM at least 4 CPUs and 8 GB of RAM.
- Python 3.9+ for dbt and Streamlit.
- About 10 GB of free disk: 3.4 GB of CSVs, plus a copy in MinIO and in Postgres.

### 1. Get the data

Download the dataset from [Kaggle](https://www.kaggle.com/datasets/junyiacademy/learning-activity-public-dataset-by-junyi-academy) and put these three files in `data/`:

```
data/Info_UserData.csv
data/Info_Content.csv
data/Log_Problem.csv
```

`data/` is in `.gitignore`, so the files never end up in the repo.

### 2. Start the stack

```bash
cp .env.example .env
docker compose up -d --build
```

On first start:

- `minio-init` creates the `junyi-raw` bucket and exits.
- Airflow migrates its metadata database and creates the admin user from `.env`.

| Service | URL / address | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| MinIO S3 API | http://localhost:9000 | `minioadmin` / `minioadmin` |
| PostgreSQL | `localhost:5433`, database `warehouse` | `postgres` / `postgres` |

### 3. Run the pipeline

In the Airflow UI, turn on the `ingest_junyi_data` DAG and click **Trigger DAG**. The form asks for a `target`: keep `local` for the Postgres container. When all four tasks are green, the `raw`, `staging` and `marts` schemas are built.

`WAREHOUSE_TARGET` in `.env` sets the form's default. The local warehouse loads all 16M `Log_Problem` rows unless `LOCAL_LOG_PROBLEM_ROW_LIMIT` is set.

`dbt_project/profiles.yml` reads its connection from `WAREHOUSE_*` variables, which the DAG sets for the chosen target, so it contains no credentials. Without them it falls back to the local container on `localhost:5433`, which lets you also run `dbt run` from `dbt_project/` on the host.

### 4. Run the dashboard

Create a virtualenv, install the dashboard's packages and start it from the project root:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

With no secrets file, the app reads the local Docker warehouse with the default credentials from `.env.example`. To change that or add Supabase, copy the example and edit it. `secrets.toml` is in `.gitignore`:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Each `[connections.<name>]` section is a data source. With both `local` and `supabase` defined, a **Data source** selector appears in the sidebar; `local` is the default.

### Using Supabase as the warehouse

1. In Supabase, open **Connect** and copy the **Session pooler** details. The direct `db.<ref>.supabase.co` host is IPv6-only, which Docker usually can't reach.
2. Fill in the Supabase block in `.env`:

   ```bash
   SUPABASE_HOST=aws-0-<region>.pooler.supabase.com
   SUPABASE_PORT=5432
   SUPABASE_DB=postgres
   SUPABASE_USER=postgres.<project-ref>
   SUPABASE_PASSWORD=<your database password>
   SUPABASE_SSLMODE=require
   SUPABASE_LOG_PROBLEM_ROW_LIMIT=500000
   ```

3. Recreate Airflow so it picks up the new values:

   ```bash
   docker compose up -d airflow
   ```

4. Trigger the DAG and choose `supabase` as the `target`. To make it the default, set `WAREHOUSE_TARGET=supabase`.
5. To see Supabase in the dashboard, add a `[connections.supabase]` section to `.streamlit/secrets.toml` with the same values. The commented block in `secrets.toml.example` shows the format, including `query = { sslmode = "require" }`.

Both warehouses stay available: switching is just a different `target` on the next run. If the Supabase values are missing, a `supabase` run fails on its first task with a message saying which ones.

The free plan caps the database at 500 MB, and the full dataset needs about 6 GB. With `SUPABASE_LOG_PROBLEM_ROW_LIMIT=500000` the whole warehouse is about 225 MB. The file isn't sorted by date, so the first rows are spread across the year.

### Publishing the dashboard on Streamlit Community Cloud

The public app reads Supabase only, so it needs no Docker.

1. **Create a read-only role** for it. The app's secrets should never hold the `postgres` password. Run this once in the Supabase **SQL Editor**, with a strong password of your own:

   ```sql
   CREATE ROLE junyi_reader WITH LOGIN PASSWORD '<strong password>';
   GRANT USAGE ON SCHEMA marts TO junyi_reader;
   GRANT SELECT ON ALL TABLES IN SCHEMA marts TO junyi_reader;
   -- dbt recreates the tables on every run; this keeps new ones readable
   ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA marts GRANT SELECT ON TABLES TO junyi_reader;
   ```

2. **Deploy:** on [share.streamlit.io](https://share.streamlit.io), click **Create app** and pick this repository, branch `main` and file `app.py`. The root `requirements.txt` lists the dashboard's packages.
3. **Add the secrets:** under **Advanced settings → Secrets**, paste only a Supabase section:

   ```toml
   [connections.supabase]
   dialect = "postgresql"
   host = "aws-0-<region>.pooler.supabase.com"
   port = 5432
   database = "postgres"
   username = "junyi_reader.<project-ref>"
   password = "<junyi_reader password>"
   query = { sslmode = "require" }
   ```

   The app also accepts the same names as `.env`, as top-level secrets (TOML needs the quotes) or as environment variables:

   ```toml
   SUPABASE_HOST = "aws-0-<region>.pooler.supabase.com"
   SUPABASE_USER = "junyi_reader.<project-ref>"
   SUPABASE_PASSWORD = "<junyi_reader password>"
   ```

   `SUPABASE_PORT`, `SUPABASE_DB` and `SUPABASE_SSLMODE` default to `5432`, `postgres` and `require`.

With a single source there's no selector, and the page shows "Data source: supabase". Query results are cached until the app restarts, so reboot it from the Streamlit dashboard after a new DAG run.

## Project structure

```
.
├── app.py                     # Streamlit dashboard
├── dags/
│   └── ingestion_junyi_dag.py # Migrations → MinIO upload → Postgres COPY
├── migrations/
│   └── 001_create_raw_schema_and_tables.sql
├── dbt_project/
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/           # sources.yml + stg_* views
│       └── marts/             # dim_* and fct_* tables
├── data/                      # CSVs go here (git-ignored)
├── docker-compose.yml
├── Dockerfile                 # Airflow image with extra Python packages and dbt
├── requirements.txt           # Dashboard packages (also used by Streamlit Cloud)
├── requirements-airflow.txt   # Packages installed into the Airflow image
├── .streamlit/
│   └── secrets.toml.example   # Data sources for the dashboard
└── .env.example
```

## What the dashboard shows

- **Top KPIs:** total interactions, average time per problem, average hints and the share of problems answered correctly on the first attempt.
- **Effort by difficulty:** average attempts and hints per difficulty level.
- **Activity by hour of day:** how many problems are worked on at each hour.
- **Performance by learning stage:** a table of volume, attempts, time and first-attempt success per stage.
- **First-attempt success vs. retries:** how many problems are correct on the first try, and how many attempts students make when they aren't.

## FAQ

### Why the Junyi dataset?

- **It's real telemetry, not a toy example.** It has 16 million rows, a 3 GB file and messy fields: empty strings, booleans stored as text and nullable flags. That makes streaming loads, typing and data quality real problems rather than exercises.
- **It's public and free to download.** Anyone can reproduce the project end to end.
- **It fits a star schema naturally.** There's a clear event table (problem attempts) and clear dimensions (students, exercises, time), which is the textbook shape for dimensional modeling.
- **It's big enough to matter, small enough for a laptop.** Choices like `COPY` instead of row-by-row inserts make a visible difference, yet the whole pipeline still runs locally.

### Why upload to MinIO first instead of loading the CSVs straight into Postgres?

The pipeline copies a common production pattern: land raw files in an object store (S3, GCS or similar), then load the warehouse from there. The landing zone keeps an untouched copy of the source. Reloads don't depend on the original files on someone's laptop, and the warehouse load becomes "read from S3", which is how it would work in the cloud. MinIO speaks the S3 API, so the same code would work against AWS S3.

### Why is every column in the `raw` schema `TEXT`?

The raw layer is meant to be an exact copy of the source, so it should never reject a row. If a column were typed `INTEGER` and the file had an empty string or a bad value, `COPY` would fail the whole load. Casting and cleaning happen in dbt staging models, where problems are visible and fixable in SQL.

### Why `COPY` instead of pandas or `INSERT`?

`COPY ... FROM STDIN` is Postgres's native bulk loader and is far faster than inserting rows one by one. The DAG streams each file from MinIO into `COPY` without saving it to disk or holding it in memory, which matters for the 3 GB `Log_Problem.csv`.

### Why `pgsty/silo` instead of the official MinIO image?

MinIO stopped publishing community Docker images in 2025, and the project was later archived. `minio/minio` and `minio/mc` no longer pull from Docker Hub, and `quay.io` requires login. `pgsty/silo` is an actively maintained community fork and a drop-in replacement: same environment variables, same `server` command, and it ships the `mc` client used to create the bucket. The image is pinned to a release tag so upgrades are deliberate.


### Why are the dbt schemas `staging` and `marts`, not `public_staging` and `public_marts`?

By default dbt prefixes a model's `+schema` with the profile's schema, so `+schema: marts` builds into `public_marts`. The dashboard queries `marts.*`, so the project overrides this with `macros/generate_schema_name.sql`, a standard dbt pattern that uses `+schema` exactly as written.

### Why are staging models views and marts tables?

Staging models are light, one-to-one cleanups of the raw tables. Views keep them cheap and always up to date. Marts are what the dashboard queries, and the fact table has 16 million rows, so they're materialized as tables to keep queries fast.

### Why doesn't the dashboard show "how many attempts it takes to pass a problem"?

The dataset can't answer that. In Junyi, `is_correct` means **correct on the first attempt without hints**, not "eventually solved". Almost every row with `is_correct = True` has exactly one attempt, and there is no field that says whether a problem was solved in the end. A chart of "attempts to pass" built on `is_correct` would show 1 for everyone. The last dashboard section shows what the data does support: the share of problems correct on the first try, and how many attempts students make when they aren't.

