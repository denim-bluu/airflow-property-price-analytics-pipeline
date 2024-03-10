import io
import json
import logging
import os
from typing import Any

import boto3
import polars as pl
from botocore.exceptions import ClientError
from util.parser import get_config


def get_s3_client():
    config = get_config()
    return boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id=config.get("AWS_CREDS", "AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS_CREDS", "AWS_SECRET_ACCESS_KEY"),
    )


def upload_file_to_s3(file_name: str, bucket: str, object_name: str | None = None):
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    s3_client = get_s3_client()
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        if response:
            logging.info(f"File uploaded to s3://{bucket}/{object_name}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_data_to_s3(data: Any, bucket: str, key: str) -> None:
    """Export data to an S3 bucket."""
    logging.info(f"Exporting data to s3://{bucket}/{key}")
    s3_client = get_s3_client()
    s3_client.put_object(Body=data, Bucket=bucket, Key=key)
    print(f"Data successfully exported to s3://{bucket}/{key}")


def read_json_from_s3(bucket: str, key: str) -> Any:
    """Read a JSON file from S3."""
    logging.info(f"Reading JSON file from s3://{bucket}/{key}")
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def read_parquet_from_s3(bucket: str, key: str) -> pl.DataFrame:
    """Read a JSON file from S3."""
    logging.info(f"Reading parquet file from s3://{bucket}/{key}")
    s3_client = get_s3_client()
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pl.read_parquet(io.BytesIO(response["Body"].read()))


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


def check_file_exists(bucket: str, key: str) -> bool:
    """Check if a file exists."""
    s3_client = get_s3_client()
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def check_bucket_exists(bucket: str) -> bool:
    """Check if a bucket exists."""
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket} exists")
        return True
    except Exception as e:
        print(e)
        return False
