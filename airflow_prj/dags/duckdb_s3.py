import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from include.resources.duckdb_s3 import DataLakeIngester, DataLakeTransformer
from pendulum import datetime


@dag(
    dag_id="duckdb_s3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
def init():
    @task
    def ingest_hourly_gharchive():
        context = get_current_context()

        partition_date = (
            context["logical_date"]
            .set(
                minute=0,
                microsecond=0,
                second=0,
            )
            .subtract(hours=3)
        )
        print(partition_date)
        dli = DataLakeIngester("gharchive/events")
        dli.ingest_hourly_gharchive(partition_date)

    @task
    def transform():
        context = get_current_context()

        partition_date = (
            context["logical_date"]
            .set(
                minute=0,
                microsecond=0,
                second=0,
            )
            .subtract(hours=3)
        )

        dlt = DataLakeTransformer("gharchive/events")
        dlt.transform(partition_date)

    @task
    def aggregate():
        context = get_current_context()

        partition_date = (
            context["logical_date"]
            .set(
                minute=0,
                microsecond=0,
                second=0,
            )
            .subtract(hours=3)
        )

        dlt = DataLakeTransformer("gharchive/events")
        dlt.aggregate_silver_data(partition_date)

    ingest_hourly_gharchive() >> transform() >> aggregate()


dag_start = init()
