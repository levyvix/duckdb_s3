import configparser
import io
import sys
from datetime import datetime
from pathlib import Path

import boto3
import requests
from loguru import logger

# Configurar logging
logger.remove()
logger.add(
    Path(__file__).parent / "logs" / "ingest.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
)
logger.add(sys.stdout, colorize=True)


class DataLakeIngester:
    def __init__(self, dataset_base_path: str):
        """Inicializa o objeto DataLakeIngester.

        Args:
            dataset_base_path: caminho base no S3, exemplo: "gharchive/events"
        """
        self.dataset_base_path = dataset_base_path
        self.config = self._load_config()
        self.s3_client = self._init_s3_client()

    def _load_config(self):
        config = configparser.ConfigParser()

        config_path = Path.joinpath(Path(__file__).parent.parent, "config.ini")
        config.read(config_path)

        logger.success("Config loaded from {}", config_path)

        return config

    def _init_s3_client(self):
        aws_access_key_id = self.config["aws"]["s3_access_key_id"]
        aws_secret_access_key = self.config["aws"]["s3_secret_access_key"]
        region_name = self.config["aws"]["s3_region_name"]

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
        logger.success("Cliente S3 inicializado")
        return s3_client

    def _date_to_s3_key(self, process_date: datetime) -> str:
        year_month_day = process_date.strftime("%Y-%m-%d")
        hour = process_date.strftime("%H")
        return f"gharchive/events/{year_month_day}/{hour}/{year_month_day}-{hour}.json.gz"

    def _get_data_from_url(self, url: str) -> bytes:
        logger.info("Baixando dados de {}", url)
        response = requests.get(url)
        response.raise_for_status()
        logger.success("Dados baixados de {}", url)
        return response.content

    def _upload_file_to_s3_path(self, content: bytes, s3_key: str):
        bucket_name = self.config["datalake"]["bronze_bucket"]
        logger.info("Enviando dados para S3: {}", s3_key)
        self.s3_client.upload_fileobj(io.BytesIO(content), bucket_name, s3_key)
        logger.success("Dados enviados para S3: {}", s3_key)

    def ingest_hourly_gharchive(self, process_date: datetime):
        process_date_str = process_date.strftime("%Y-%m-%d-%H")
        logger.info("Ingerindo dados para {}", process_date_str)

        url = f"https://data.gharchive.org/{process_date_str}.json.gz"
        response_content = self._get_data_from_url(url)

        target_s3_key = self._date_to_s3_key(process_date)
        self._upload_file_to_s3_path(response_content, target_s3_key)
