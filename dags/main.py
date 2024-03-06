from resources.s3 import read_json_from_s3
from datetime import datetime
from scraper.zoopla_scraper import run_zoopla_scraper
from util import const
from resources.spark import (
    create_spark_session,
    append_delta_table,
    validate_property_data,
)

if __name__ == "__main__":
    spark = create_spark_session()
    run_date = str(datetime.today().date())
    run_zoopla_scraper(run_date)
    json_data = read_json_from_s3(
        const.AWS_S3_BUCKET, f"{const.JSON_DATA_DIR.format(run_date=run_date)}"
    )
    df = spark.createDataFrame(json_data)
    df = validate_property_data(df)
    append_delta_table(df)
