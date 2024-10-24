from dagster import ConfigurableResource

from .duckdb_s3 import DataLakeIngester, DataLakeTransformer


class DataLakeIngesterResource(ConfigurableResource):
    dataset_base_path: str

    def get_ingester(self):
        return DataLakeIngester(dataset_base_path=self.dataset_base_path)


class DataLakeTransformerResource(ConfigurableResource):
    dataset_base_path: str

    def get_transformer(self):
        return DataLakeTransformer(dataset_base_path=self.dataset_base_path)
