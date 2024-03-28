from dagster import ConfigurableResource
from deltalake import DeltaTable, write_deltalake
from pandas import DataFrame


class DeltaLakeResource(ConfigurableResource):
    access_key: str
    secret_key: str
    endpoint: str
    aws_region: str
    s3_path_property: str

    @property
    def _storage_options(self):
        return {
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
            "AWS_ENDPOINT_URL": self.endpoint,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_REGION": self.aws_region,
        }

    def load_table(self) -> DeltaTable:
        return DeltaTable(self.s3_path_property, storage_options=self._storage_options)

    def create_table(self, df: DataFrame) -> None:
        write_deltalake(self.s3_path_property, df, storage_options=self._storage_options)

    def merge_table(
        self, target_dt: DeltaTable, source: DataFrame, join_condition: str
    ) -> None:
        target_dt.merge(
            source=source,
            predicate=join_condition,
            source_alias="source",
            target_alias="target",
        ).execute()
