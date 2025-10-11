import configparser
import sys
from datetime import datetime
from pathlib import Path

import duckdb
from loguru import logger

# Configurar logging
logger.remove()
logger.add(
    Path(__file__).parent / "logs" / "transform.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
)
logger.add(sys.stdout, colorize=True)


class DataLakeTransformer:
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
        """Configurar credenciais S3 no DuckDB a partir do arquivo de configuração"""
        aws_access_key_id = self.config.get("aws", "s3_access_key_id")
        aws_secret_access_key = self.config.get("aws", "s3_secret_access_key")
        aws_region = self.config.get("aws", "s3_region_name")

        self.con.execute(f"SET s3_access_key_id='{aws_access_key_id}'")
        self.con.execute(f"SET s3_secret_access_key='{aws_secret_access_key}'")
        self.con.execute(f"SET s3_region='{aws_region}'")

    def _build_path(self, bucket: str, process_date: datetime, file_extension: str) -> str:
        year_month_day = process_date.strftime("%Y-%m-%d")
        hour = process_date.strftime("%H")
        return f"s3://{bucket}/{self.dataset_base_path}/{year_month_day}/{hour}/{year_month_day}-{hour}.{file_extension}"

    def transform(self, process_date: datetime) -> None:
        """
        Serializar e limpar dados brutos, depois exportar para formato parquet na próxima camada.

        :param process_date: data de processamento correspondente à partição horária a serializar
        """
        bronze_bucket = self.config.get("datalake", "bronze_bucket")
        silver_bucket = self.config.get("datalake", "silver_bucket")

        source_path = self._build_path(bronze_bucket, process_date, "json.gz")
        target_path = self._build_path(silver_bucket, process_date, "parquet")
        logger.info(f"Source path: {source_path}")
        logger.info(f"Target path: {target_path}")
        self._serialize_data(source_path)
        self._clean_data()
        self._write_data_to_parquet(target_path, duckdb_table="gharchive_clean")

    def _serialize_data(self, source_path: str):
        logger.info("DuckDB - serializando dados...")
        self.con.execute(f"""
            CREATE OR REPLACE TABLE gharchive_raw AS
            FROM read_json_auto('{source_path}', ignore_errors=true)
        """)
        logger.success("DuckDB - dados serializados")

    def _clean_data(self):
        logger.info("DuckDB - limpando dados...")
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
        logger.success("DuckDB - dados limpos")

    def _write_data_to_parquet(self, target_path: str, duckdb_table: str):
        logger.info("DuckDB - escrevendo dados limpos em parquet...")
        result_table = self.con.table(duckdb_table)
        result_table.write_parquet(target_path)
        logger.success("DuckDB - dados escritos em parquet")

    def aggregate_silver_data(self, process_date: datetime):
        """
        Agregar dados da camada silver e exportar para formato parquet.

        :param process_date: data de processamento correspondente à partição diária a agregar
        """
        source_bucket = self.config.get("datalake", "silver_bucket")
        sink_bucket = self.config.get("datalake", "gold_bucket")

        year_month_day = process_date.strftime("%Y-%m-%d")

        source_path = f"s3://{source_bucket}/{self.dataset_base_path}/{year_month_day}/*/*.parquet"
        target_path = f"s3://{sink_bucket}/{self.dataset_base_path}/{year_month_day}/{year_month_day}.parquet"

        self._aggregate_data(source_path)
        self._write_data_to_parquet(target_path, duckdb_table="gharchive_agg")

    def _aggregate_data(self, source_path: str):
        logger.info("DuckDB - agregando dados...")
        query = f"""
            SELECT
                event_type,
                repo_id,
                repo_name,
                repo_url,
                DATE_TRUNC('day', CAST(event_date AS TIMESTAMP)) AS event_date,
                count(*) AS event_count
            FROM '{source_path}'
            GROUP BY ALL
        """
        self.con.execute(f"CREATE OR REPLACE TABLE gharchive_agg AS FROM ({query})")
        logger.success("DuckDB - dados agregados")
