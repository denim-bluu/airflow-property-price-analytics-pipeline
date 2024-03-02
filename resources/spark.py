import os

from delta import DeltaTable, configure_spark_with_delta_pip
from delta._typing import ColumnMapping
from dotenv import load_dotenv
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from util import const

# Load environment variables from .env file
load_dotenv()


def create_spark_session() -> SparkSession:
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
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))  # type: ignore
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


def append_delta_table(df: DataFrame):
    print(f"Creating new Delta table at {const.DELTA_DATA_DIR}")
    df.write.format("delta").mode("append").save(
        f"s3a://{const.AWS_S3_BUCKET}/{const.DELTA_DATA_DIR}"
    )


def merge_delta_tables(spark: SparkSession, df: DataFrame):
    """Merge a DataFrame with an existing Delta table or create one if it doesn't exist."""
    try:
        dt = DeltaTable.forPath(spark, const.DELTA_DATA_DIR)
        print(f"Merging data with Delta table at {const.DELTA_DATA_DIR}")
        merge_condition = "source.id = target.id"
        update_mapping: ColumnMapping = {col: f"source.{col}" for col in df.columns}

        dt.alias("target").merge(df.alias("source"), merge_condition).whenMatchedUpdate(
            set=update_mapping
        ).whenNotMatchedInsertAll().execute()

    except AnalysisException:
        print(f"Creating new Delta table at {const.DELTA_DATA_DIR}")
        df.write.format("delta").save(
            f"s3a://{const.AWS_S3_BUCKET}/{const.DELTA_DATA_DIR}"
        )
