# Using DuckDB for building data pipelines

Building a simple data lake based on Medallion Archirecture, to incrementally ingest and process Github events.

![DuckDB gharchive pipeline](https://github.com/user-attachments/assets/0b37081e-a786-4687-9a3f-99c768f963b1)

## Prerequisites

### Install UV (Python Package Manager)

UV is a fast Python package manager used in this project. Install it:

**Linux/macOS:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Verify installation:
```bash
uv --version
```

### Install Astro CLI (for Airflow)

Astro CLI is used to run Airflow locally. Install it:

**Linux:**
```bash
curl -sSL install.astronomer.io | sudo bash -s
```

**macOS:**
```bash
brew install astro
```

**Windows:**
```powershell
winget install -e --id Astronomer.Astro
```

Verify installation:
```bash
astro version
```

## Clone The Repository

```bash
git clone https://github.com/levyvix/duckdb_s3.git
cd duckdb_s3
```

## Setup Python Environment

This project uses UV for dependency management:

```bash
# Install dependencies using UV
uv sync
```

## Configuration

### For Standalone Scripts and Dagster

1. Copy `config.ini.template` to `config.ini`:
```bash
cp config.ini.template config.ini
```

2. Edit `config.ini` and fill in your AWS S3 credentials in the `[aws]` section:
```ini
[aws]
s3_access_key_id = your_access_key_here
s3_secret_access_key = your_secret_key_here
s3_region_name = us-east-1

[datalake]
bronze_bucket = your-bronze-bucket
silver_bucket = your-silver-bucket
gold_bucket = your-gold-bucket
```

### For Airflow

Configure Airflow Variables in the Airflow UI (Admin → Variables):

Required variables:
- `s3_access_key_id` - AWS access key
- `s3_secret_access_key` - AWS secret key
- `s3_region_name` - AWS region (e.g., us-east-1)
- `bronze_bucket` - S3 bucket name for Bronze layer
- `silver_bucket` - S3 bucket name for Silver layer
- `gold_bucket` - S3 bucket name for Gold layer

## Run the Project

### Option 1: Standalone Scripts

Use `uv run` to execute pipeline scripts:

```bash
# Ingest data (Bronze layer)
uv run scripts/main_ingest.py

# Transform data (Silver layer)
uv run scripts/main_transform.py

# Aggregate data (Gold layer)
uv run scripts/main_agg.py
```

### Read the Gold Data

To read the aggregated gold data, you can use DuckDB's Python API. Below is an example script to read the gold data from S3:

```python
import duckdb

# Initialize DuckDB connection
con = duckdb.connect()

# Define the path to the gold data
gold_data_path = "s3://<gold_bucket>/gharchive/events/<year-month-day>/*.parquet"

# Read the gold data
df = con.execute(f"SELECT * FROM '{gold_data_path}'").fetchdf()

# Display the data
print(df.head())
```

Replace `<gold_bucket>` and `<year-month-day>` with your actual bucket name and date.

### Option 2: Dagster

The project includes a Dagster pipeline to orchestrate the data pipeline:

```bash
cd dagster_project
dagster dev
```

Then, open the [Dagster web interface](http://localhost:3000) in your browser.

**Run the Job:**

In the Dagster UI, navigate to the **Deployments** tab and start the `duckdb_s3_job` job. It will ask if you want to run the latest partition or a backfill. Choose the **latest** partition and click the **Start Job** button.

The pipeline runs hourly via the `duckdb_s3_schedule` (cron: `0 * * * *`).

### Option 3: Airflow

The project includes an Airflow DAG to orchestrate the data pipeline:

```bash
cd airflow_prj
astro dev start
```

Then, open the [Airflow web interface](http://localhost:8080) in your browser.

**IMPORTANT:** Before running the DAG, you must configure the Airflow Variables in the UI (Admin → Variables) as described in the Configuration section above.

**Run the DAG:**

In the Airflow UI, find the `duckdb_s3` DAG and trigger it manually or wait for it to run on schedule.
