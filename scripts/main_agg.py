#!/usr/bin/env python3
import datetime as dt
import logging
import sys
from datetime import datetime, timedelta

# Add the parent directory to the Python path
from data_lake_transformer import DataLakeTransformer
from loguru import logger

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    try:
        transformer = DataLakeTransformer(dataset_base_path="gharchive/events")
        now = datetime.now(dt.timezone.utc)
        # Calculate the process_date for the previous day's data aggregation
        process_date = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)
        transformer.aggregate_silver_data(process_date)
        logging.info(f"Successfully aggregated bronze data for {process_date}")
    except Exception as e:
        logging.error(f"Error in aggregate_silver_data: {str(e)}")


if __name__ == "__main__":
    main()
