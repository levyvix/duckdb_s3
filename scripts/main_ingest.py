import datetime as dt
from datetime import datetime, timedelta

from data_lake_ingester import DataLakeIngester


def main():
    ingester = DataLakeIngester("gharchive/events")

    now = datetime.now(dt.timezone.utc)

    # 2024-10-20 00:00:00+00:00
    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)

    ingester.ingest_hourly_gharchive(process_date)


if __name__ == "__main__":
    main()
