import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from scraper.zoopla_scraper import run_zoopla_scraper
from resources.s3 import read_json_from_s3
from resources.conn_operator import create_connection
from resources.spark import (
    create_spark_session,
    append_delta_table,
    validate_property_data,
)
from util import const
from datetime import timedelta

# Airflow DAG definitions
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 3, 2),  # Change to your start date
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "zoopla_scraping_dag",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 3, 1, tz="UTC"),
    description="A DAG for scraping Zoopla and storing data in Delta Lake",
    schedule="@daily",
    catchup=False,
    
)


def scrape_and_upload_to_s3(**kwargs):
    run_date = kwargs["ds"]
    run_zoopla_scraper(run_date)
    print(f"Scraped data for {run_date} and uploaded to S3")


def download_and_process_data(**kwargs):
    run_date = kwargs["ds"]
    spark = create_spark_session()
    json_data = read_json_from_s3(
        const.AWS_S3_BUCKET, f"{const.JSON_DATA_DIR.format(run_date=run_date)}"
    )
    df = spark.createDataFrame(json_data)
    df = validate_property_data(df)
    append_delta_table(df)


# Define the tasks (operators) in the DAG
t0 = PythonOperator(
    task_id="setup_connection",
    python_callable=create_connection,
    provide_context=True,
    dag=dag,
)
t1 = PythonOperator(
    task_id="scrape_and_upload_to_s3",
    python_callable=scrape_and_upload_to_s3,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id="download_and_process_data",
    python_callable=download_and_process_data,
    provide_context=True,
    dag=dag,
)

# Define the task sequence
t0 >> t1 >> t2
