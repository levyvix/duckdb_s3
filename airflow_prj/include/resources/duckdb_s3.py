import configparser
import io
from datetime import datetime
from pathlib import Path

import boto3
import duckdb
import requests
from airflow.models import Variable


class DataLakeIngester:
    def __init__(self, dataset_base_path: str):
        """Initializes the DataLakeIngester object.

        Args:
            s3_path (str): like "gharchive/events"
        """
        self.dataset_base_path = dataset_base_path
        self.s3_client = None
        self._init_s3_client()

    def _get_env_var(self, key: str) -> str:
        return Variable.get(key, "key_not_found")

    def _init_s3_client(self):
        try:
            aws_access_key_id = self._get_env_var("s3_access_key_id")
            aws_secret_access_key = self._get_env_var("s3_secret_access_key")
            region_name = self._get_env_var("s3_region_name")

            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )

            try:
                print(self.s3_client.list_buckets()[:1])
                # print("Buckets:", [bucket["Name"] for bucket in response["Buckets"]])
            except Exception as e:
                print(f"Failed to list S3 buckets: {str(e)}")
        except Exception as e:
            print(f"Failed to initialize S3 client: {str(e)}")

    def _date_to_s3_key(self, process_date: datetime) -> str:
        return "gharchive/events/{}/{}/{}.json.gz".format(
            process_date.strftime("%Y-%m-%d"),
            process_date.strftime("%H"),
            process_date.strftime("%Y-%m-%d-%H"),
        )

    def _get_data_from_url(self, url: str) -> bytes:
        response = requests.get(url)
        response.raise_for_status()

        return response.content

    def ingest_hourly_gharchive(self, process_date: datetime):
        # self.init_s3_client()
        process_date_str = process_date.strftime("%Y-%m-%d-%H")

        # if the Hour is below 10, remove the leading 0
        if process_date_str[-2] == "0":
            process_date_str = process_date_str[:-2] + process_date_str[-1]

        url = f"https://data.gharchive.org/{process_date_str}.json.gz"

        try:
            response_content = self._get_data_from_url(url)
            target_s3_key = self._date_to_s3_key(process_date)

            # Upload to S3
            with io.BytesIO(response_content) as data_stream:
                self.s3_client.upload_fileobj(
                    data_stream, "dataeng-landing-zone-957", target_s3_key
                )

            print(
                f"Successfully uploaded to s3://dataeng-landing-zone-957/{target_s3_key}"
            )
        except Exception as e:
            print(f"Failed to ingest data for {process_date_str}: {str(e)}")
            raise e


class DataLakeTransformer:
    def __init__(self, dataset_base_path: str):
        self.dataset_base_path: str = dataset_base_path
        self.config: configparser.ConfigParser = self._load_config()
        self.con: duckdb.DuckDBPyConnection = self._init_duckdb_connection()
        self._set_duckdb_s3_credentials()

    def _load_config(self) -> configparser.ConfigParser:
        config = configparser.ConfigParser()

        config_path = Path.joinpath(Path(__file__).parent.parent, "config.ini")
        config.read(config_path)

        return config

    def _get_env_var(self, key: str) -> str:
        return Variable.get(key, "key_not_found")

    def _init_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        return con

    def _set_duckdb_s3_credentials(self):
        """Read S3 credentials and endpoint from config file"""
        aws_access_key_id = self._get_env_var("s3_access_key_id")
        aws_secret_access_key = self._get_env_var("s3_secret_access_key")
        # Set S3 credentials
        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")

    def _build_path(
        self, bucket: str, process_date: datetime, file_extension: str
    ) -> str:
        year_month_day = process_date.strftime("%Y-%m-%d")
        hour = process_date.strftime("%H")
        return f"s3://{bucket}/{self.dataset_base_path}/{year_month_day}/{hour}/{year_month_day}-{hour}.{file_extension}"

    def transform(self, process_date: datetime) -> duckdb.DuckDBPyRelation:
        """
        Serialize and clean raw data, then export to parquet format on next zone.

        :param process_date: the process date corresponding to the hourly partition to serialise
        """
        bronze_bucket = self._get_env_var("bronze_bucket")
        silver_bucket = self._get_env_var("silver_bucket")

        print(f"bronze_bucket: {bronze_bucket}")
        print(f"silver_bucket: {silver_bucket}")

        # remove leading zero from hour if below 10
        if process_date.strftime("%H")[0] == "0":
            process_date = process_date.replace(
                hour=int(process_date.strftime("%H")[1])
            )

        source_path = self._build_path(bronze_bucket, process_date, "json.gz")
        target_path = self._build_path(silver_bucket, process_date, "parquet")

        self._serialize_data(source_path)
        self._clean_data()
        self._write_data_to_parquet(target_path, duckdb_table="gharchive_clean")

    def _serialize_data(self, source_path: str):
        self.con.execute(f"""
						 create or replace table gharchive_raw as 
						 from read_json_auto('{source_path}', ignore_errors=true)
						 """)

    def _clean_data(self):
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
        self.con.execute(f"CREATE OR REPLACE TABLE gharchive_clean AS FROM ({query})")

    def _write_data_to_parquet(self, target_path: str, duckdb_table: str):
        result_table = self.con.table(f"{duckdb_table}")
        result_table.write_parquet(target_path)

    def aggregate_silver_data(self, process_date: datetime):
        """
        Aggregate raw data and export to parquet format.

        :param process_date: the process date corresponding to the daily partition to aggregate
        """
        try:
            source_bucket = self._get_env_var("silver_bucket")
            sink_bucket = self._get_env_var("gold_bucket")

            # remove leading zero from hour if below 10
            if process_date.strftime("%H")[0] == "0":
                process_date = process_date.replace(
                    hour=int(process_date.strftime("%H")[1])
                )

            year_month_day = process_date.strftime("%Y-%m-%d")

            source_path = f"s3://{source_bucket}/{self.dataset_base_path}/{year_month_day}/*/*.parquet"
            target_path = f"s3://{sink_bucket}/{self.dataset_base_path}/{year_month_day}/{year_month_day}.parquet"

            self._aggregate_data(source_path)
            self._write_data_to_parquet(target_path, duckdb_table="gharchive_agg")

        except Exception as e:
            print(f"Error in aggregate_silver_data: {str(e)}")

    def _aggregate_data(self, source_path: str):
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
