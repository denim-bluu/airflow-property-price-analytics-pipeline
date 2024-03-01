from typing import Any
from delta import DeltaTable, configure_spark_with_delta_pip
from delta._typing import ColumnMapping
from dotenv import load_dotenv
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pathlib import Path
from src.export_s3 import export_to_s3
from datetime import datetime
from src.zoopla_scraper import run_zoopla_scraper
import os

# Load environment variables from .env file
load_dotenv()


def create_spark_session():
    """Create and configure a Spark session."""
    builder = (
        SparkSession.builder.appName("MyApp")  # type: ignore
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWSAWS_ACCESS_KEY_ID"))  # type: ignore
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))  # type: ignore
        .config(
            "spark.sql.shuffle.partitions", "4"
        )  # default is 200 partitions which is too many for local
        .config("spark.master", "local[*]")
    )
    my_packages = ["org.apache.hadoop:hadoop-aws:3.3.2"]
    return configure_spark_with_delta_pip(
        builder, extra_packages=my_packages
    ).getOrCreate()


def validate_property_data(df: DataFrame) -> DataFrame:
    """Validates property data against a basic schema."""
    # Add more validation logic as needed
    return df.filter(
        F.col("price").isNotNull()
        & F.col("bedrooms").between(0, 10)
        & F.col("bathrooms").between(0, 10)
        & F.col("livingrooms").between(0, 10)
        & F.col("address").isNotNull()
        & F.col("listed_date").isNotNull()
    )


def merge_delta_tables(spark: SparkSession, df: DataFrame, path: str):
    """Merge a DataFrame with an existing Delta table or create one if it doesn't exist."""
    try:
        dt = DeltaTable.forPath(spark, path)
        merge_condition = "source.id = target.id"
        update_mapping: ColumnMapping = {col: f"source.{col}" for col in df.columns}

        dt.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdate(
            set=update_mapping
        ).whenNotMatchedInsertAll().execute()

    except AnalysisException:
        df.write.format("delta").save(path)


def store_scraped_data_to_s3(data: Any, run_date: str):
    export_to_s3(data, "houseprices3", f"data/raw/{run_date}/scraped_output.json")


def read_json_from_s3(bucket: str, key: str):
    """Read a JSON file from S3."""
    return spark.read.json(f"s3a://{bucket}/{key}")


def scrape_and_process_properties(data):
    """Runs the scraper and processes the data."""
    df = spark.createDataFrame(data)
    return validate_property_data(df)


if __name__ == "__main__":
    spark = create_spark_session()
    path = Path("s3a://houseprices3")
    run_date = str(datetime.today().date())
    run_zoopla_scraper(run_date)
    with open(f"scraped_output_{run_date}.json", "r") as f:
        data = f.read()
    store_scraped_data_to_s3(data, run_date)
    # df = scrape_and_process_properties()
    # merge_delta_tables(spark, df, str(path / "delta-table"))
