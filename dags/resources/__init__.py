import boto3
import os

from dotenv import load_dotenv

load_dotenv()

S3_CLIENT = boto3.client(
    "s3",
    region_name="eu-west-1",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)
