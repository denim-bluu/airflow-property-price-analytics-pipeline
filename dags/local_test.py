from scraper.zoopla_scraper import run_zoopla_scraper
from resources.s3 import read_parquet_from_s3, check_file_exists
from resources.spark import (
    create_spark_session,
    validate_property_data,
    upsert_to_existing_table,
)
import constants


if __name__ == "__main__":
    run_date = "9999-12-31"
    location = "london/bromley"
    run_zoopla_scraper(run_date, location=location)
    data = read_parquet_from_s3(
        constants.AWS_S3_BUCKET,
        f"{constants.RAW_DATA_DIR}/{'_'.join([run_date + location.replace('/','-')])}.parquet",
    )
    spark = create_spark_session()
    df = spark.createDataFrame(data.to_pandas())
    df = validate_property_data(df)
    if check_file_exists(
        constants.AWS_S3_BUCKET,
        f"{constants.STAGING_DATA_DIR}/staging.parquet",
    ):
        stg_table = read_parquet_from_s3(
            constants.AWS_S3_BUCKET,
            f"{constants.STAGING_DATA_DIR}/staging.parquet",
        )
        stg_table = spark.createDataFrame(stg_table.to_pandas())
        df = upsert_to_existing_table(df, stg_table, ["id", "price"])
        df.write.mode("overwrite").parquet(
            f"s3a://{constants.AWS_S3_BUCKET}/{constants.STAGING_DATA_DIR}/staging.parquet"
        )
        
