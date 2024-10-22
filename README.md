# Using DuckDB for building data pipelines

Building a simple data lake based on Medallion Archirecture, to incrementally ingest and process Github events.

![DuckDB gharchive pipeline](https://github.com/user-attachments/assets/0b37081e-a786-4687-9a3f-99c768f963b1)

### Clone The Repository

```bash
git clone https://github.com/levyvix/duckdb_s3.git
```

### Setup Python Virtual Environment

```bash
$ cd duckdb_s3
$ python3 -m venv .venv
$ source .venv/bin/activate (Linux) | .venv\Scripts\activate (Windows)

# Install required packages
$ pip install -r requirements.txt
```

### Configuration

1. Rename `config.ini.template` to `config.ini`
2. Edit `config.ini` and fill in your actual AWS S3 credential values in the `[aws]` section.
3. If you are using a S3 compatible storage, setup the `s3_endpoint_url` parameter as well. Otherwise remove the line
4. Edit `config.ini` and fill in the bucket names in `[datalake]` section for each zone in your data lake.

## Run the project

### Ingest Data

Run the main_ingest.py script to start the ingestion pipeline.

```bash
python scripts/main_ingest.py
```

### Transform Data

Run the main_transform.py script to transform the ingested data.

```bash
python scripts/main_transform.py
```

### Aggregate Data

Run the main_agg.py script to aggregate the transformed data.

```bash
python scripts/main_agg.py
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
