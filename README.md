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

The DAG runs three tasks in order:

1. **`run_db_migrations`** applies any pending `.sql` file in `migrations/` and records it in `public.schema_migrations`.
2. **`upload_local_to_minio`** uploads the CSVs from `data/` to the `junyi-raw` bucket under `landing/`.
3. **`load_minio_to_postgres`** truncates each `raw.*` table and streams the matching file from MinIO straight into Postgres with `COPY`, without writing it to disk.

dbt is run by hand from the host after the DAG finishes (see [Getting started](#getting-started)).

## Tech stack

| Layer | Tool | Notes |
|---|---|---|
| Containers | Docker Compose (Colima on macOS) | One network, `de-network`, for all services |
| Data lake | MinIO via [`pgsty/silo`](https://hub.docker.com/r/pgsty/silo) | Community fork; official MinIO images are gone (see FAQ) |
| Orchestration | Apache Airflow 2.10.5, `standalone` | Custom image with `minio` and `psycopg2-binary` |
| Warehouse | PostgreSQL 15 | Database `warehouse` |
| Transformations | dbt-core 1.10 + dbt-postgres 1.9 | Run from a local virtualenv |
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

### 3. Load the raw data

In the Airflow UI, turn on and trigger the `ingest_junyi_data` DAG. When all three tasks are green, `raw.info_user_data`, `raw.info_content` and `raw.log_problem` are populated.

### 4. Build the models with dbt

Create a virtualenv and install the local tools:

```bash
python3 -m venv venv
source venv/bin/activate
pip install dbt-postgres streamlit plotly pandas sqlalchemy psycopg2-binary
```

Add a `junyi_warehouse` profile to `~/.dbt/profiles.yml`. It lives outside the repo because it contains credentials:

```yaml
junyi_warehouse:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5433
      user: postgres
      pass: postgres
      dbname: warehouse
      schema: public
      threads: 4
```

Then build:

```bash
cd dbt_project
dbt run
```

### 5. Run the dashboard

Create `.streamlit/secrets.toml`. It's in `.gitignore`:

```toml
[connections.postgresql]
dialect = "postgresql"
host = "localhost"
port = 5433
database = "warehouse"
username = "postgres"
password = "postgres"
```

Then, from the project root:

```bash
streamlit run app.py
```

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
├── Dockerfile                 # Airflow image with extra Python packages
├── requirements.txt           # Packages installed into the Airflow image
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

