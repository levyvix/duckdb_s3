import datetime as dt
from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    asset,
)

from .resources_duckdb_s3 import DataLakeIngesterResource, DataLakeTransformerResource


@asset
def ingest_data(
    context: AssetExecutionContext, data_lake_ingester: DataLakeIngesterResource
) -> MaterializeResult:
    context.log.info("Ingesting data")

    # get datetiem of execution
    now = datetime.now(dt.timezone.utc)

    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_ingester.get_ingester().ingest_hourly_gharchive(process_date)


@asset(deps=[ingest_data])
def transform_data(
    context: AssetExecutionContext, data_lake_transformer: DataLakeTransformerResource
):
    context.log.info("Transforming data")

    # get datetiem of execution
    now = datetime.now(dt.timezone.utc)

    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_transformer.get_transformer().transform(process_date)


@asset(deps=[transform_data])
def aggregate_data(
    context: AssetExecutionContext, data_lake_transformer: DataLakeTransformerResource
):
    context.log.info("Aggregating data")

    # get datetiem of execution
    now = datetime.now(dt.timezone.utc)

    process_date = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_transformer.get_transformer().aggregate_silver_data(process_date)
