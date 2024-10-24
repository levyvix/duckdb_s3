from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from .assets import duckdb_s3 as assets_duckdb_s3
from .resources import DataLakeIngesterResource, DataLakeTransformerResource

# -- duckdb_s3
duckdb_s3_assets = load_assets_from_modules([assets_duckdb_s3])
duckdb_s3_job = define_asset_job(
    name="duckdb_s3_job",
    selection=AssetSelection.assets(*duckdb_s3_assets),
    description="Ingest and transform data",
)
duckdb_s3_schedule = ScheduleDefinition(
    name="duckdb_s3_schedule",
    cron_schedule="0 * * * *",
    job=duckdb_s3_job,
    description="Ingest and transform data",
)

defs = Definitions(
    assets=duckdb_s3_assets,
    jobs=[duckdb_s3_job],
    schedules=[duckdb_s3_schedule],
    resources={
        "data_lake_ingester": DataLakeIngesterResource(
            dataset_base_path="gharchive/events"
        ),
        "data_lake_transformer": DataLakeTransformerResource(
            dataset_base_path="gharchive/events"
        ),
    },
)
