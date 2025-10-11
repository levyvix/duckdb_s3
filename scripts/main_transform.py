import sys
from datetime import datetime, timedelta, timezone

from data_lake_transformer import DataLakeTransformer
from loguru import logger

# Configurar logging
logger.remove()
logger.add(sys.stderr, colorize=True)


def main():
    transformer = DataLakeTransformer(dataset_base_path="gharchive/events")

    now = datetime.now(timezone.utc)
    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)

    transformer.transform(process_date)
    logger.success(f"Dados transformados com sucesso para {process_date}")


if __name__ == "__main__":
    main()
