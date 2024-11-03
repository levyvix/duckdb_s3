# md5
import hashlib

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)


@dlt.source(name="marvel_api")
def marvel_source(
    private_key: str = dlt.secrets.value, public_key: str = dlt.secrets.value
):
    # Gerar o timestamp e o hash de autenticação
    ts = pendulum.now().timestamp().__str__()
    hash_value = hashlib.md5(f"{ts}{private_key}{public_key}".encode()).hexdigest()

    # Configuração da Marvel API com DLT
    config: RESTAPIConfig = {
        "client": {
            "base_url": "http://gateway.marvel.com/v1/public/",
            "auth": None,  # A Marvel usa hash para autenticação, então o auth direto é omitido
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    "apikey": public_key,
                    "ts": ts,
                    "hash": hash_value,
                },
            },
        },
        "resources": [
            {
                "name": "comics",
                "endpoint": {
                    "path": "comics",
                },
            },
            {
                "name": "characters",
                "endpoint": {
                    "path": "characters",
                },
            },
            {
                "name": "creators",
                "endpoint": {
                    "path": "creators",
                },
            },
            {
                "name": "events",
                "endpoint": {
                    "path": "events",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


def load_data():
    pipeline = dlt.pipeline(
        pipeline_name="marvel_api_first_try",
        destination="duckdb",
        dataset_name="marvel_comics",
    )

    load_info = pipeline.run(marvel_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    load_data()
