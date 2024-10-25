import datetime as dt
from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    HourlyPartitionsDefinition,
    MaterializeResult,
    asset,
)

from ..resources import DataLakeIngesterResource, DataLakeTransformerResource


@asset(
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime(2024, 10, 10, 0, 0, 0)
    ),
)
def ingest_data(
    context: AssetExecutionContext, data_lake_ingester: DataLakeIngesterResource
) -> MaterializeResult:
    context.log.info("Ingesting data")

    # get datetiem of execution
    partition = context.partition_key
    context.log.info("Partition: " + str(partition))
    now = context.partition_key

    now_datetime = datetime.strptime(now, "%Y-%m-%d-%H:%M")

    process_date = now_datetime - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_ingester.get_ingester().ingest_hourly_gharchive(process_date)


@asset(
    deps=[ingest_data],
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime(2024, 10, 10, 0, 0, 0)
    ),
)
def transform_data(
    context: AssetExecutionContext, data_lake_transformer: DataLakeTransformerResource
):
    context.log.info("Transforming data")

    # get datetiem of execution
    now = context.partition_key

    now_datetime = datetime.strptime(now, "%Y-%m-%d-%H:%M")

    process_date = now_datetime - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_transformer.get_transformer().transform(process_date)


@asset(
    deps=[transform_data],
    partitions_def=HourlyPartitionsDefinition(
        start_date=datetime(2024, 10, 10, 0, 0, 0)
    ),
)
def aggregate_data(
    context: AssetExecutionContext,
    data_lake_transformer: DataLakeTransformerResource,
):
    context.log.info("Aggregating data")

    # get datetiem of execution
    now = context.partition_key
    now_datetime = datetime.strptime(now, "%Y-%m-%d-%H:%M")

    process_date = now_datetime - timedelta(hours=4)

    context.log.info(f"Processing date: {process_date}")

    data_lake_transformer.get_transformer().aggregate_silver_data(process_date)
