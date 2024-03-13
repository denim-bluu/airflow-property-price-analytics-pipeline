import logging
import os
import urllib.parse
from datetime import timedelta
from typing import Any, Dict, List

import constants
import pendulum
import polars as pl
from airflow.decorators import dag, task
from loguru import logger
from resources.redshift import run_redshift_statement
from resources.s3 import check_bucket_exists, upload_file_to_s3
from scraper.zoopla_scraper import scrape_page
from util.parser import get_yaml_config

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
    def check_bucket_exists_task() -> bool:
        if not check_bucket_exists(constants.S3_BUCKET):
            raise ValueError(f"Bucket {constants.S3_BUCKET} does not exist")
        return True

    @task()
    def scrape_zoopla(location: str, **kwargs) -> List[Dict[str, Any]]:
        run_date = kwargs["ds"]
        # Construct the URL
        params = {
            "beds_min": 2,
            "is_retirement_home": "false",
            "is_shared_ownership": "false",
        }
        logging.info(f"Scraping Zoopla for {location} on {run_date}")

        # Scrape the page
        url = (
            f"{constants.ZOOPLA_BASE_URL}/{location}/?{urllib.parse.urlencode(params)}"
        )
        logging.info(f"URL: {url}")
        all_properties = scrape_page(url)

        return [property.model_dump() for property in all_properties]

    @task()
    def to_aws_s3(data: Any, location: str, **kwargs) -> str:
        run_date = kwargs["ds"]
        file_name = f"{'_'.join([run_date + location.replace('/', '-')])}.parquet"
        pl.DataFrame(data).write_parquet("temp_data.parquet")
        logger.info(
            f"Uploading to s3://{constants.S3_BUCKET}/{constants.RAW_DIR}/{file_name}"
        )
        upload_file_to_s3(
            "temp_data.parquet", constants.S3_BUCKET, f"{constants.RAW_DIR}/{file_name}"
        )
        os.remove("temp_data.parquet")
        return f"s3://{constants.S3_BUCKET}/{constants.RAW_DIR}/{file_name}"

    @task()
    def copy_to_redshift(s3_file_path, **kwargs) -> bool:
        config = get_yaml_config()
        iam_role = config.get("AWS_CREDS", "AWS_RS_IAM_ROLE")
        create_table = """
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR(255),
            listing_title VARCHAR(255),
            description VARCHAR(255),
            listed_date VARCHAR(255),
            address VARCHAR(255),
            price BIGINT,
            bedrooms BIGINT,
            bathrooms BIGINT,
            livingrooms BIGINT,
            terms VARCHAR(255),
            finger_print VARCHAR(255)
        );
        """
        sql_copy_to_stg = f"COPY {constants.STAGING_TABLE} FROM '{s3_file_path}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"
        logger.info(f"Creating table {constants.PROD_TABLE}")
        run_redshift_statement(create_table.format(table_name=constants.PROD_TABLE))
        logger.info(f"Creating table {constants.PROD_TABLE}")
        run_redshift_statement(create_table.format(table_name=constants.STAGING_TABLE))
        logger.info(f"Copying data to {constants.STAGING_TABLE}")
        run_redshift_statement(sql_copy_to_stg)
        return True

    @task()
    def upsert_to_prod(**kwargs) -> bool:
        delete_from_table = f"DELETE FROM {constants.PROD_TABLE} USING {constants.STAGING_TABLE} WHERE {constants.PROD_TABLE}.finger_print = {constants.STAGING_TABLE}.finger_print;"
        insert_into_table = f"INSERT INTO {constants.PROD_TABLE} SELECT * FROM {constants.STAGING_TABLE};"
        drop_stg_table = "DROP TABLE {constants.STAGING_TABLE};"
        logger.info(f"Upserting data to {constants.PROD_TABLE}")
        logger.info(f"Deleting from {constants.PROD_TABLE}")
        run_redshift_statement(delete_from_table)
        logger.info(f"Inserting into {constants.PROD_TABLE}")
        run_redshift_statement(insert_into_table)
        logger.info(f"Dropping {constants.STAGING_TABLE}")
        run_redshift_statement(drop_stg_table)
        return True

    location = "london/bromley"
    t1 = check_bucket_exists_task()
    properties_data = scrape_zoopla(location)
    s3_file_path = to_aws_s3(properties_data, location)
    t2 = copy_to_redshift(s3_file_path)
    t3 = upsert_to_prod()

    t1 >> properties_data >> s3_file_path >> t2 >> t3


zoopla_etl = zoopla_etl_dag()
