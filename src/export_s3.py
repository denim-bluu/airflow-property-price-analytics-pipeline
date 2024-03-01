from typing import Any
import boto3
from dotenv import load_dotenv
import os


def export_to_s3(data: Any, bucket: str, key: str):
    """Export data to an S3 bucket."""
    load_dotenv()
    s3 = boto3.client(
        "s3",
        region_name="eu-west-1",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    s3.put_object(Body=data, Bucket=bucket, Key=key)
    print(f"Data successfully exported to s3://{bucket}/{key}")
