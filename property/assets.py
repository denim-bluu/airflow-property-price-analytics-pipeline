import os
from typing import Any, Dict, List

import pendulum
import polars as pl
from .scraper.zoopla_scraper import ZooplaSCraper, example_url_constructor
from dagster import AssetExecutionContext, asset, MaterializeResult, MetadataValue
from deltalake import _internal
from .resources import DeltaLakeResource, RedshiftClientResource, CONFIG
from dagster_aws.s3 import S3Resource


@asset
def bromley_data(context: AssetExecutionContext) -> List[Dict[str, Any]]:
    url = example_url_constructor(2, "london/bromley")
    context.log.info(f"Scraping data from {url}")
    all_properties = ZooplaSCraper().scrape_url(url)
    return [property.model_dump() for property in all_properties]


@asset
def upload_zoopla_data_to_s3_as_parquet(
    context: AssetExecutionContext,
    s3: S3Resource,
    bromley_data,
) -> MaterializeResult:
    data = pl.DataFrame(bromley_data)
    data.write_parquet("bromley.parquet")
    context.log.info("Uploading data to S3")
    s3.get_client().upload_file(
        "bromley.parquet",
        "housepriceetl",
        f"{pendulum.now().date()}/bromley.parquet",
    )
    context.log.info("Data uploaded to S3")
    os.remove("bromley.parquet")
    return MaterializeResult(
        metadata={
            "shape": f"{data.shape}",
            "columns": data.columns,
            "head": MetadataValue.md(data.to_pandas().head(5).to_markdown()),
        }
    )


@asset
def upload_zoopla_data_to_s3_delta_table(
    context: AssetExecutionContext, deltalake_manager: DeltaLakeResource, bromley_data
) -> MaterializeResult:
    data = pl.DataFrame(bromley_data)

    try:
        context.log.info("Loading table")
        dt = deltalake_manager.load_table()
        context.log.info("Merging data")
        deltalake_manager.merge_table(
            dt, data.to_pandas(), "source.finger_print = target.finger_print"
        )
        return MaterializeResult(
            metadata={
                "preview": MetadataValue.md(dt.to_pandas().head(5).to_markdown()),
                "row": MetadataValue.int(dt.to_pandas().shape[0]),
                "col": MetadataValue.int(dt.to_pandas().shape[1]),
            }
        )
    except _internal.TableNotFoundError:
        context.log.info("Table not found, creating table")
        deltalake_manager.create_table(data.to_pandas())
        return MaterializeResult(
            metadata={
                "preview": MetadataValue.md(data.to_pandas().head(5).to_markdown()),
                "row": MetadataValue.int(data.to_pandas().shape[0]),
                "col": MetadataValue.int(data.to_pandas().shape[1]),
            }
        )


@asset
def check_redshift_prod_table(
    context: AssetExecutionContext, redshift: RedshiftClientResource
) -> MaterializeResult:
    create_table = """
    CREATE TABLE IF NOT EXISTS prod_property_data (
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
    context.log.info(redshift.Config)
    context.log.info("Creating prod table")
    redshift.get_client().execute_query(create_table)
    temp = redshift.get_client().execute_query(
        "SELECT id FROM prod_property_data LIMIT 5", fetch_results=True
    )
    if temp:
        context.log.info("Prod table already exists")
        return MaterializeResult(
            metadata={
                "status": "Prod table already exists successfully",
                "preview": MetadataValue.md(temp),
            }
        )
    return MaterializeResult(metadata={"status": "Prod table created successfully"})


@asset(deps=[upload_zoopla_data_to_s3_delta_table, check_redshift_prod_table])
def upload_staging_table(
    context: AssetExecutionContext, redshift: RedshiftClientResource
) -> MaterializeResult:
    context.log.info("Uploading staging table")
    context.log.debug(CONFIG)
    copy_query = f"""
    COPY stg_property_data
    FROM '{CONFIG["AWS"]["S3"]["S3_PATH_PROPERTY"]}/' 
    ACCESS_KEY_ID '{CONFIG["AWS"]["ACCESS_KEY_ID"]}' 
    SECRET_ACCESS_KEY '{CONFIG["AWS"]["SECRET_ACCESS_KEY"]}' 
    FORMAT AS PARQUET;
    """
    redshift.get_client().execute_query(copy_query)
    return MaterializeResult(metadata={"status": "Data uploaded successfully"})
