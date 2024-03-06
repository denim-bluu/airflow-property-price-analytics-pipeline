import json
from typing import Any

import boto3

from resources import S3_CLIENT


def export_to_s3(data: Any, bucket: str, key: str) -> None:
    """Export data to an S3 bucket."""
    S3_CLIENT.put_object(Body=data, Bucket=bucket, Key=key)
    print(f"Data successfully exported to s3://{bucket}/{key}")


def read_json_from_s3(bucket: str, key: str) -> Any:
    """Read a JSON file from S3."""
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def folder_exists_and_not_empty(bucket: str, path: str) -> bool:
    """
    Folder should exists.
    Folder should not be empty.
    """
    s3 = boto3.client("s3")
    if not path.endswith("/"):
        path = path + "/"
    resp = s3.list_objects(Bucket=bucket, Prefix=path, Delimiter="/", MaxKeys=1)
    return "Contents" in resp


def check_bucket_exists(bucket: str) -> bool:
    """Check if a bucket exists."""
    try:
        S3_CLIENT.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket} exists")
        return True
    except Exception as e:
        print(e)
        return False
