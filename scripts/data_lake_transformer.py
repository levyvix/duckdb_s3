import configparser
import sys
from datetime import datetime
from pathlib import Path

import duckdb
from loguru import logger

logger.remove()

logger.add(
    Path(__file__).parent / "logs" / "transform.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
)

logger.add(
    sys.stdout,
    colorize=True,
)

# TODO: add delta lake support


class DataLakeTransformer(object):
    def __init__(self, dataset_base_path: str):
        self.dataset_base_path: str = dataset_base_path
        self.config: configparser.ConfigParser = self._load_config()
        self.con: duckdb.DuckDBPyConnection = self._init_duckdb_connection()
        self._set_duckdb_s3_credentials()
        logger.success("DuckDB connection initialized")

    def _load_config(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser()

        config_path = Path.joinpath(Path(__file__).parent.parent, "config.ini")
        config.read(config_path)

        logger.success("Config loaded from {}", config_path)

        return config

    def _init_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        logger.success("DuckDB extension httpfs installed and loaded")
        return con

    def _set_duckdb_s3_credentials(self):
        """Read S3 credentials and endpoint from config file"""
        aws_access_key_id = self.config.get("aws", "s3_access_key_id")
        aws_secret_access_key = self.config.get("aws", "s3_secret_access_key")
        # Set S3 credentials
        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")

    def transform(
        self,
        process_date: datetime = datetime.now().replace(
            minute=0, second=0, microsecond=0
        ),
    ) -> duckdb.DuckDBPyRelation:
        """
        Serialize and clean raw data, then export to parquet format on next zone.

        :param process_date: the process date corresponding to the hourly partition to serialise
        """
        bronze_bucket = self.config.get("datalake", "bronze_bucket")
        silver_bucket = self.config.get("datalake", "silver_bucket")

        # extract
        logger.info("DuckDB - serializing data...")

        year_month_day = process_date.strftime("%Y-%m-%d")
        hour = process_date.strftime("%H")

        source_path = f"s3://{bronze_bucket}/{self.dataset_base_path}/{year_month_day}/{hour}/{year_month_day}-{hour}.json.gz"
        target_path = f"s3://{silver_bucket}/{self.dataset_base_path}/{year_month_day}/{hour}/{year_month_day}-{hour}.parquet"
        self.con.execute(f"""
                         create or replace table gharchive_raw as 
                         from read_json_auto('{source_path}', ignore_errors=true)
                         """)

        logger.success("DuckDB - data serialized")
        # clean

        query = """
            SELECT 
            id AS "event_id",
            actor.id AS "user_id",
            actor.login AS "user_name",
            actor.display_login AS "user_display_name",
            type AS "event_type",
            repo.id AS "repo_id",
            repo.name AS "repo_name",
            repo.url AS "repo_url",
            created_at AS "event_date"
            FROM 'gharchive_raw'
        """

        logger.info("DuckDB - cleaning data...")
        self.con.execute(f"CREATE OR REPLACE TABLE gharchive_clean AS FROM ({query})")

        # load
        result_table = self.con.table("gharchive_clean")

        result_table.write_parquet(target_path)

    def aggregate_silver_data(self, process_date: datetime):
        """
        Aggregate raw data and export to parquet format.

        :param process_date: the process date corresponding to the daily partition to aggregate
        """

        try:
            source_bucket = self.config.get("datalake", "silver_bucket")
            sink_bucket = self.config.get("datalake", "gold_bucket")

            year_month_day = process_date.strftime("%Y-%m-%d")

            source_path = f"s3://{source_bucket}/{self.dataset_base_path}/{year_month_day}/*/*.parquet"
            target_path = f"s3://{sink_bucket}/{self.dataset_base_path}/{year_month_day}/{year_month_day}.parquet"

            # get data
            logger.info("DuckDB - aggregating data...")
            query = f"""
                SELECT 
                event_type,
                repo_id,
                repo_name,
                repo_url,
                DATE_TRUNC('day',CAST(event_date AS TIMESTAMP)) AS event_date,
                count(*) AS event_count
                FROM '{source_path}'
                GROUP BY ALL
            """
            self.con.execute(f"CREATE OR REPLACE TABLE gharchive_agg AS FROM ({query})")

            logger.success("DuckDB - data aggregated")

            # load

            logger.info("DuckDB - writing aggregated data to parquet...")

            result_table = self.con.table("gharchive_agg")

            result_table.write_parquet(target_path)

            logger.success("DuckDB - aggregated data written to parquet")

        except Exception as e:
            logger.error(f"Error in aggregate_silver_data: {str(e)}")
