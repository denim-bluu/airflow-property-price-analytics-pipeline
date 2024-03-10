from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from util.parser import get_config


def create_spark_session() -> SparkSession:
    """Create and configure a Spark session."""
    config = get_config()
    builder = (
        SparkSession.builder.appName("MyApp")  # type: ignore
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.access.key",
            config.get("AWS_CREDS", "AWS_ACCESS_KEY_ID"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            config.get("AWS_CREDS", "AWS_SECRET_ACCESS_KEY"),
        )
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.115",
        )
    )
    return builder.getOrCreate()


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


def upsert_to_existing_table(
    df1: DataFrame, df2: DataFrame, keys: list[str]
) -> DataFrame:
    """
    Upsert the given DataFrame to the existing table.

    Args:
        df1 (DataFrame): The DataFrame to upsert.
        df2 (DataFrame): The existing DataFrame.
        key (list[str]): The key to use for the upsert.
    """
    return df1.alias("a").join(df2.alias("b"), keys, how="outer").select("*")
