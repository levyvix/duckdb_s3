import datetime as dt
import sys
from datetime import datetime, timedelta

# Add the parent directory to the Python path
from data_lake_transformer import DataLakeTransformer
from loguru import logger

# Setup logging
logger.remove()

logger.add(
    sys.stderr,
    colorize=True,
)


def main():
    try:
        transformer = DataLakeTransformer(dataset_base_path="gharchive/events")
        now = datetime.now(dt.timezone.utc)
        process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(
            hours=3
        )
        transformer.transform(process_date)
        logger.success(f"Successfully serialised raw data for {process_date}")
    except Exception as e:
        logger.error(f"Error in serialise_raw_data: {str(e)}")


if __name__ == "__main__":
    main()
