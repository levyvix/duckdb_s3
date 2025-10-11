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
    # Calcular a data para agregar dados do dia anterior
    process_date = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)

    transformer.aggregate_silver_data(process_date)
    logger.success(f"Dados agregados com sucesso para {process_date}")


if __name__ == "__main__":
    main()
