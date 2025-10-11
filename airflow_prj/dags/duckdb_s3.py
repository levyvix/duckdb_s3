from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from include.resources.duckdb_s3 import DataLakeIngester, DataLakeTransformer
from pendulum import datetime, now


def calcular_data_particao():
    """Calcular data de partição a partir do contexto do Airflow"""
    context = get_current_context()
    partition_date = context.get("logical_date", now()).set(minute=0, microsecond=0, second=0).subtract(hours=3)
    return partition_date


@dag(
    dag_id="duckdb_s3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
def init():
    @task
    def ingest_hourly_gharchive():
        partition_date = calcular_data_particao()
        ingester = DataLakeIngester("gharchive/events")
        ingester.ingest_hourly_gharchive(partition_date)

    @task
    def transform():
        partition_date = calcular_data_particao()
        transformer = DataLakeTransformer("gharchive/events")
        transformer.transform(partition_date)

    @task
    def aggregate():
        partition_date = calcular_data_particao()
        # Aggregate data needs date without hour (daily aggregation)
        agg_date = partition_date.set(hour=0, minute=0, second=0, microsecond=0)
        transformer = DataLakeTransformer("gharchive/events")
        transformer.aggregate_silver_data(agg_date)

    ingest_hourly_gharchive() >> transform() >> aggregate()  # type: ignore


dag_start = init()
