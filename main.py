import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.errors.exceptions.base import AnalysisException
from delta import DeltaTable
from delta._typing import ColumnMapping
from typing import Any
import json
# from src.zoopla_scraper import run_zoopla_scraper

# Load environment variables from .env file
load_dotenv()


def create_spark_session():
    """Create and configure a Spark session."""
    conf = (
        SparkConf()
        .setAppName("MY_APP")  # replace with your desired name
        .set(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2",
        )
        .set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))  # type: ignore
        .set("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))  # type: ignore
        .set(
            "spark.sql.shuffle.partitions", "4"
        )  # default is 200 partitions which is too many for local
        .setMaster(
            "local[*]"
        )  # replace the * with your desired number of cores. * for use all.
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()  # type: ignore


def validate_property_data(df):
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


def scrape_and_process_properties():
    """Runs the scraper and processes the data."""
    # properties = run_zoopla_scraper()

    # data: Any = [model.model_dump() for model in properties]
    with open("data/output.json", "r") as file:
        data: Any = json.load(file)
    df = spark.createDataFrame(data)
    df = validate_property_data(df)

    return df


if __name__ == "__main__":
    spark = create_spark_session()
    properties_df = scrape_and_process_properties()
    merge_delta_tables(spark, properties_df, "s3a://houseprices3/data")
