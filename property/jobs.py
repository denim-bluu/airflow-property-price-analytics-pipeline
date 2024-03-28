from dagster import OpExecutionContext, op, job
from dagster_aws.s3 import S3Resource
from dagster_aws.redshift import RedshiftClientResource
from .resources import RESOURCES_DEV


@op
def example_s3_op(context: OpExecutionContext, s3: S3Resource):
    context.log.info(
        s3.get_client().list_objects_v2(
            Bucket="houseprice",
        )
    )
    return None


@op
def example_redshift_asset_op(
    context: OpExecutionContext, redshift: RedshiftClientResource
):
    context.log.info(
        redshift.get_client().execute_query("SELECT 1", fetch_results=True)
    )
    return None


@job(resource_defs=RESOURCES_DEV)
def check_connections():
    example_s3_op()
    example_redshift_asset_op()
