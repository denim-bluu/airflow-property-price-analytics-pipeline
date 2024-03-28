from ..resources.deltalake_manager import DeltaLakeResource
from dagster_aws.s3 import S3Resource
from dagster_aws.redshift import RedshiftClientResource
import yaml
from pathlib import Path

CONFIG = yaml.load(
    open(Path(__file__).parent.parent / "config.yaml"), Loader=yaml.FullLoader
)

RESOURCES_DEV = {
    "deltalake_manager": DeltaLakeResource(
        access_key=CONFIG["AWS"]["ACCESS_KEY_ID"],
        secret_key=CONFIG["AWS"]["SECRET_ACCESS_KEY"],
        endpoint="http://127.0.0.1:9000",
        aws_region=CONFIG["AWS"]["S3"]["AWS_REGION"],
        s3_path_property=CONFIG["AWS"]["S3"]["S3_PATH_PROPERTY"],
    ),
    "s3": S3Resource(
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id=CONFIG["AWS"]["ACCESS_KEY_ID"],
        aws_secret_access_key=CONFIG["AWS"]["ACCESS_KEY_ID"],
    ),
    "redshift": RedshiftClientResource(
        host=CONFIG["AWS"]["REDSHIFT"]["HOST"],
        port=CONFIG["AWS"]["REDSHIFT"]["PORT"],
        user=CONFIG["AWS"]["REDSHIFT"]["USER"],
        password=CONFIG["AWS"]["REDSHIFT"]["PASSWORD"],
        database=CONFIG["AWS"]["REDSHIFT"]["DATABASE"],
    ),
}
