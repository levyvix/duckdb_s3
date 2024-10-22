import configparser
import io
import sys
from datetime import datetime
from pathlib import Path

import boto3
import requests
from loguru import logger

logger.remove()

logger.add(
    Path(__file__).parent / "logs" / "ingest.log",
    rotation="1 day",
    retention="7 days",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} - {level} - {message}",
)

logger.add(
    sys.stdout,
    colorize=True,
)


class DataLakeIngester:
    def __init__(self, dataset_base_path: str):
        """Initializes the DataLakeIngester object.

        Args:
            s3_path (str): like "gharchive/events"
        """
        self.dataset_base_path = dataset_base_path
        self.s3_client = None
        self.config: configparser.ConfigParser = self._load_config()
        self._init_s3_client()
        self._load_config()

    def _load_config(self):
        config = configparser.ConfigParser()

        config_path = Path.joinpath(Path(__file__).parent.parent, "config.ini")
        config.read(config_path)

        logger.success("Config loaded from {}", config_path)

        return config

    def _init_s3_client(self):
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.config["aws"]["s3_access_key_id"],
                aws_secret_access_key=self.config["aws"]["s3_secret_access_key"],
                region_name=self.config["aws"]["s3_region_name"],
            )
            logger.success("S3 client initialized")
        except Exception as e:
            logger.error("Failed to initialize S3 client: {}", e)

    def _date_to_s3_key(self, process_date: datetime) -> str:
        return "gharchive/events/{}/{}/{}.json.gz".format(
            process_date.strftime("%Y-%m-%d"),
            process_date.strftime("%H"),
            process_date.strftime("%Y-%m-%d-%H"),
        )

    def _get_data_from_url(self, url: str) -> bytes:
        logger.info("Downloading data from {}", url)

        response = requests.get(url)
        response.raise_for_status()
        logger.success("Downloaded data from {}", url)

        return response.content

    def _upload_file_to_s3_path(self, content: bytes, s3_key: str):
        logger.info("Uploading data to S3: {}", s3_key)
        self.s3_client.upload_fileobj(
            io.BytesIO(content), "dataeng-landing-zone-957", s3_key
        )
        logger.success("Uploaded data to S3: {}", s3_key)

    def ingest_hourly_gharchive(self, process_date: datetime):
        # self.init_s3_client()
        process_date_str = process_date.strftime("%Y-%m-%d-%H")
        logger.info("Ingesting data for {}", process_date_str)

        url = f"https://data.gharchive.org/{process_date_str}.json.gz"

        response_content = self._get_data_from_url(url)

        target_s3_key = self._date_to_s3_key(process_date)

        self._upload_file_to_s3_path(response_content, target_s3_key)
