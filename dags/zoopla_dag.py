import pendulum

from airflow.decorators import dag, task
from scraper.zoopla_scraper import run_zoopla_scraper
from resources.s3 import read_parquet_from_s3, check_bucket_exists
from resources.spark import (
    create_spark_session,
    validate_property_data
)
import constants
from datetime import timedelta
import os

# Airflow DAG definitions
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 3, 2),  # Change to your start date
    "email": ["denim.bluu@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="zoopla_scraping_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)
def zoopla_etl_dag():
    @task()
    def test_spark_task():
        spark = create_spark_session()
        print(f"Spark version: {spark.version}")

    @task()
    def check_env_vars():
        required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(
                f"Environment variables not found: {', '.join(missing_vars)}"
            )

    @task()
    def check_bucket_exists_task():
        if not check_bucket_exists(constants.AWS_S3_BUCKET):
            raise ValueError(f"Bucket {constants.AWS_S3_BUCKET} does not exist")

    @task()
    def scrape_bromley_properties(**kwargs):
        run_date = kwargs["ds"]
        run_zoopla_scraper(run_date, location="london/bromley")
        print(f"Scraped data for {run_date} and uploaded to S3")

    @task()
    def process_bromley_properties(**kwargs):
        run_date = kwargs["ds"]
        spark = create_spark_session()
        data = read_parquet_from_s3(
        constants.AWS_S3_BUCKET,
        f"{constants.RAW_DATA_DIR}/{'_'.join([run_date + location.replace('/','-')])}.parquet",
    )
        df = spark.createDataFrame(json_data)
        df = validate_property_data(df)

    t0 = test_spark_task()
    t1 = check_env_vars()
    t2 = check_bucket_exists_task()
    t3 = scrape_bromley_properties()
    t4 = process_bromley_properties()

    t0 >> t1 >> t2
    t2.set_downstream(t3)
    t3.set_downstream(t4)


zoopla_etl = zoopla_etl_dag()
