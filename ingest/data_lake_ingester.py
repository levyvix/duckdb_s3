import io
from datetime import datetime

import boto3
import dotenv
import requests


class DataLakeIngester:
    def __init__(self, s3_path: str):
        """Args:

        Args:
            s3_path (str): like "gharchive/events"
        """
        self.s3_path = s3_path
        self.s3_client = None
        self.init_s3_client()

    def init_s3_client(self):
        env = dotenv.dotenv_values()

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=env["AWS_ACCESS_KEY"],
            aws_secret_access_key=env["AWS_SECRET_KEY"],
            region_name="us-east-2",
        )

    def ingest_hourly_gharchive(self, process_date: datetime):
        # self.init_s3_client()
        process_date_str = process_date.strftime("%Y-%m-%d-%H")

        url = f"https://data.gharchive.org/{process_date_str}.json.gz"
        response = requests.get(url)
        response.raise_for_status()

        response_content = response.content

        # gharchive/events/{yyyy-mm-dd}/{hh}/{yyyy-mm-dd-H}.json.gz
        target_s3_key = "gharchive/events/{}/{}/{}.json.gz".format(
            process_date.strftime("%Y-%m-%d"),
            process_date.strftime("%H"),
            process_date_str,
        )
        self.s3_client.upload_fileobj(
            io.BytesIO(response_content), "dataeng-landing-zone-957", target_s3_key
        )
