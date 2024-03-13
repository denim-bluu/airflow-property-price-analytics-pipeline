import boto3
import logging
import time
from util.parser import get_yaml_config


def get_redshift_client():
    config = get_yaml_config()
    return boto3.client(
        "redshift-data",
        region_name="eu-west-1",
        aws_access_key_id=config["AWS"]["ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS"]["SECRET_ACCESS_KEY"],
    )


def run_redshift_statement(sql_statement: str, max_wait_cycles: int = 5):
    """
    Generic function to handle redshift statements (DDL, SQL..),
    it retries for the maximum MAX_WAIT_CYCLES.
    Returns the result set if the statement return results.
    """
    config = get_yaml_config()
    client = get_redshift_client()
    res = client.execute_statement(
        Database=config["AWS"]["REDSHIFT_DATABASE"],
        WorkgroupName=config["AWS"]["REDSHIFT_WORKGROUP"],
        Sql=sql_statement,
    )

    # DDL statements such as CREATE TABLE doesn't have result set.
    has_result_set = False
    done = False
    attempts = 0

    while not done and attempts < max_wait_cycles:
        attempts += 1
        time.sleep(1)

        desc = client.describe_statement(Id=res["Id"])
        query_status = desc["Status"]

        if query_status == "FAILED":
            raise Exception("SQL query failed: " + desc["Error"])

        elif query_status == "FINISHED":
            done = True
            has_result_set = desc["HasResultSet"]
        else:
            logging.info("Current working... query status is: {} ".format(query_status))

    if not done and attempts >= max_wait_cycles:
        raise Exception("Maximum of " + str(attempts) + " attempts reached.")

    if has_result_set:
        data = client.get_statement_result(Id=res["Id"])
        return data
