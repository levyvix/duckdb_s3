# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data lake pipeline project built on the Medallion Architecture (Bronze → Silver → Gold) that ingests and processes GitHub events from GH Archive using DuckDB, S3, and orchestration frameworks (Dagster/Airflow).

**Core Pipeline Flow:**
1. **Bronze (Ingest)**: Download hourly GitHub events from `data.gharchive.org` → Upload raw `.json.gz` to S3
2. **Silver (Transform)**: Read from Bronze → Clean/serialize data → Write `.parquet` to S3
3. **Gold (Aggregate)**: Read from Silver → Aggregate daily metrics → Write consolidated `.parquet` to S3

## Project Structure

```
duckdb_s3/
├── scripts/                    # Standalone Python scripts for pipeline execution
│   ├── data_lake_ingester.py   # DataLakeIngester class - handles Bronze layer
│   ├── data_lake_transformer.py # DataLakeTransformer class - handles Silver/Gold layers
│   ├── main_ingest.py          # CLI entry point for ingestion
│   ├── main_transform.py       # CLI entry point for transformation
│   └── main_agg.py             # CLI entry point for aggregation
├── dagster_project/            # Dagster orchestration (separate pyproject.toml)
│   └── dagster_project/
│       ├── assets/duckdb_s3.py # Dagster assets: ingest_data, transform_data, aggregate_data
│       ├── resources/          # Dagster resource wrappers for ingester/transformer
│       └── definitions.py      # Job and schedule definitions
├── airflow_prj/                # Airflow orchestration (Astronomer CLI project)
│   └── dags/                   # Airflow DAGs
└── config.ini                  # AWS credentials and bucket names (gitignored)
```

## Development Commands

### Running the Pipeline (Standalone Scripts)

Use `uv run` to execute pipeline scripts:

```bash
# Ingest data (Bronze layer)
uv run scripts/main_ingest.py

# Transform data (Silver layer)
uv run scripts/main_transform.py

# Aggregate data (Gold layer)
uv run scripts/main_agg.py
```

### Running Dagster

```bash
# Start Dagster web server
z dagster_project  # Navigate using z
dagster dev

# Access UI at localhost:3000
# Run the `duckdb_s3_job` job from Deployments tab
```

### Running Airflow

```bash
# Navigate to Airflow project
z airflow_prj

# Start Airflow using Astronomer CLI
astro dev start

# Access UI at localhost:8080
```

**IMPORTANT**: Before running Airflow DAGs, you must configure Airflow Variables in the UI (Admin → Variables):

Required variables:
- `s3_access_key_id` - AWS access key
- `s3_secret_access_key` - AWS secret key
- `s3_region_name` - AWS region (e.g., us-east-1)
- `bronze_bucket` - S3 bucket name for Bronze layer
- `silver_bucket` - S3 bucket name for Silver layer
- `gold_bucket` - S3 bucket name for Gold layer

### Linting

Always run linting after code changes:

```bash
uvx ruff check .
```

### Dependency Management

Use `uv` for all dependency operations:

```bash
# Add dependency
uv add <package>

# Sync dependencies
uv sync

# NEVER manually edit pyproject.toml
```

## Configuration

Before running the pipeline:

1. Copy `config.ini.template` to `config.ini`
2. Fill in AWS credentials under `[aws]` section:
   - `s3_access_key_id`
   - `s3_secret_access_key`
   - `s3_region_name`
3. Set bucket names under `[datalake]` section:
   - `bronze_bucket` (raw data)
   - `silver_bucket` (cleaned/transformed)
   - `gold_bucket` (aggregated)

## Core Classes Architecture

### DataLakeIngester (`scripts/data_lake_ingester.py`)

**Purpose**: Manages Bronze layer ingestion from GH Archive to S3

**Key Methods**:
- `ingest_hourly_gharchive(process_date)` - Downloads hourly data from `https://data.gharchive.org/{YYYY-MM-DD-HH}.json.gz` and uploads to S3

**S3 Path Pattern**: `s3://{bronze_bucket}/gharchive/events/{YYYY-MM-DD}/{HH}/{YYYY-MM-DD-HH}.json.gz`

### DataLakeTransformer (`scripts/data_lake_transformer.py`)

**Purpose**: Manages Silver and Gold layer transformations using DuckDB

**Key Methods**:
- `transform(process_date)` - Bronze → Silver: Deserialize JSON, clean data, write Parquet
  - Creates temporary DuckDB tables: `gharchive_raw` → `gharchive_clean`
  - Extracts: event_id, user_id, user_name, event_type, repo_id, repo_name, event_date
- `aggregate_silver_data(process_date)` - Silver → Gold: Daily aggregation by event type and repo
  - Groups by: event_type, repo_id, repo_name, event_date (day-level)
  - Computes: event_count

**S3 Path Patterns**:
- Silver: `s3://{silver_bucket}/gharchive/events/{YYYY-MM-DD}/{HH}/{YYYY-MM-DD-HH}.parquet`
- Gold: `s3://{gold_bucket}/gharchive/events/{YYYY-MM-DD}/{YYYY-MM-DD}.parquet`

**DuckDB Configuration**:
- Uses `httpfs` extension for S3 access
- Credentials set via `SET s3_access_key_id` and `SET s3_secret_access_key`

## Dagster Integration

The Dagster pipeline (`dagster_project/dagster_project/assets/duckdb_s3.py`) wraps the core classes as Dagster assets:

- **Partitions**: Hourly partitions starting from `2024-10-10 00:00:00`
- **Time Offset**: Processes data with a 4-hour delay (e.g., partition `2024-10-10-05:00` processes data for `2024-10-10-01:00`)
- **Assets**:
  - `ingest_data` → calls `DataLakeIngester.ingest_hourly_gharchive()`
  - `transform_data` → calls `DataLakeTransformer.transform()`
  - `aggregate_data` → calls `DataLakeTransformer.aggregate_silver_data()`
- **Schedule**: Runs hourly via `duckdb_s3_schedule` (cron: `0 * * * *`)

## Airflow Integration

The Airflow DAG (`airflow_prj/dags/duckdb_s3.py`) uses the same core classes but with Airflow Variables for configuration:

- **DAG ID**: `duckdb_s3`
- **Time Offset**: Processes data with a 3-hour delay using `logical_date`
- **Tasks**:
  - `ingest_hourly_gharchive` → calls `DataLakeIngester.ingest_hourly_gharchive()`
  - `transform` → calls `DataLakeTransformer.transform()`
  - `aggregate` → calls `DataLakeTransformer.aggregate_silver_data()`
- **Configuration**: Uses Airflow Variables (`Variable.get()`) instead of `config.ini`
  - Airflow resources are in `airflow_prj/include/resources/duckdb_s3.py`

## Logging

Both ingester and transformer use `loguru` for structured logging:
- Logs written to `scripts/logs/ingest.log` and `scripts/logs/transform.log`
- Rotation: Daily, 7-day retention
- Console output enabled with colorization

## Key Dependencies

- **duckdb**: In-memory SQL database with S3 support (httpfs extension)
- **boto3**: AWS S3 client for Bronze layer uploads
- **loguru**: Logging framework
- **dagster**: Orchestration framework (separate pyproject.toml in `dagster_project/`)
- **apache-airflow**: Alternative orchestration (in `airflow_prj/`)

## Important Notes

- The project uses **UV** for dependency management - never manually edit `pyproject.toml`
- Always run `uvx ruff check .` after making code changes
- The Dagster project has its own separate `pyproject.toml` with different Python version constraints (`>=3.8,<3.13` vs main project's `>=3.12`)
- Process dates in Dagster assets are offset by 4 hours from partition keys
- DuckDB credentials must be set in each session via `SET` commands (not persisted)
