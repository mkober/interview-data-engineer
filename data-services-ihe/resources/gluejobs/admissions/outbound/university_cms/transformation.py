"""
This class is used for Processing the data from S3 and creating the Iceberg Table
Outbound Glue job that exports data files into an S3 bucket under integration services account
This job also trigger the sync between the S3 bucket on integration service account and SFTP connector.
The SFTP connector initiates the transfer from S3 to the target location at University
"""

from datetime import datetime
import boto3
import json
import pkg_resources
from data_transformer.factory.transformation_context import TransformationContext
from data_transformer.factory.transformer_factory import DataTransformerFactory
from pyspark.sql.functions import sys, SparkContext
from data_transformer.utils import config_utils
from awsglue.context import GlueContext  # pylint: disable=E0401
from awsglue.job import Job  # pylint: disable=E0401
from awsglue.utils import getResolvedOptions

s3 = boto3.client(service_name="s3", region_name="us-east-1")

glue_client = boto3.client(
    service_name="glue",
    region_name="us-east-1",
    endpoint_url="https://glue.us-east-1.amazonaws.com",
)

ssm = boto3.client("ssm", region_name="us-east-1")

dynamodb = boto3.client("dynamodb", region_name="us-east-1")

### Initialize Spark & Glue Context#####
spark_context = SparkContext()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session
## Initialize logger instance
logger = glue_context.get_logger()

# Log installed packages
packages = [
    f"{pkg.project_name}=={pkg.version}"
    for pkg in sorted(pkg_resources.working_set, key=lambda x: x.project_name.lower())
]
logger.info(f"Installed packages: {', '.join(packages)}")


logger.info("Starting transformation job......")

# Initialize the Glue Job
job = Job(glue_context)
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "JobID",
        "tenant_code",
        "env_name",
        "ds_account_number",
        "is_account_number",
    ],
)
logger.info(f"Parsed args: {str(args)}")
job.init(args["JOB_NAME"], args)
job_run_id = args["JOB_RUN_ID"]
env_name = args["env_name"]
tenant_code = args["tenant_code"]
ds_account_number = args["ds_account_number"]
is_account_number = args["is_account_number"]

# process optional override parameters
optional_args = {}
override_params = [
    "OVERRIDE_START_DATE_ISO8601",
    "OVERRIDE_END_DATE_ISO8601",
]

if all("--{}".format(override_param) in sys.argv for override_param in override_params):
    optional_args = getResolvedOptions(sys.argv, override_params)
logger.info(f"Optional args: {str(optional_args)}")

override_start_date = optional_args.get("OVERRIDE_START_DATE_ISO8601")
override_end_date = optional_args.get("OVERRIDE_END_DATE_ISO8601")

if env_name is None or env_name == "":
    raise Exception(
        "Required parameter evnvironment name is code param [env_name] not configured"
        " on this glue job."
    )
if tenant_code is None or tenant_code == "":
    raise Exception(
        "Required parameter tenant code param [tenant_code] not configured on this glue"
        " job."
    )
if ds_account_number is None or ds_account_number == "":
    raise Exception(
        "Required parameter account number param [ds_account_number] not configured on"
        " this glue job."
    )

job_name = args["JOB_NAME"]
logger.info("----------Initializing Parameters -------")
logger.info(f"Job Run Id: : {str(job_run_id)}")
print("Job Run Id: ", str(job_run_id))
logger.info(f"---- Tenant Environment: {str(tenant_code)} {str(env_name)} -------")
print("Environment: ", str(env_name))
print("Tenant Code: ", str(tenant_code))
logger.info("Initializing TransformationContext")
context = TransformationContext(
    spark_context=spark_context,
    glue_context=glue_context,
    catalog_name="my_catalog",
    job_name=job_name,
    tenant_code=tenant_code,
    env_name=env_name,
    correlation_id=job_run_id,
    ds_account_number=str(ds_account_number),
    is_account_number=str(is_account_number),
    override_start_date=override_start_date,
    override_end_date=override_end_date,
)
print(" Transformer Context ", str(context))

data_artifacts_ssm_parameter = (
    f"/cdk/{env_name}/dps-data-services-baseline/data-artifact-bucket-arn"
)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")


def log_info(metricsName, success_count, failed_count):
    data = {
        "successMetricName": env_name + "_" + metricsName + "_Success_Count",
        "failedMetricName": env_name + "_" + metricsName + "_Failed_Count",
        "successCount": success_count,
        "failedCount": failed_count,
        "jobName": job_name,
        "tenantCode": tenant_code,
    }
    logger.info(json.dumps(data))


logger.info("Loading University CMS Transformer")
university_cms_transformer = DataTransformerFactory.get_data_transformer(
    "UniversityCMS", context
)

logger.info("Executing CMS Transformer")
response = university_cms_transformer.transform()
log_info(
    "CMS_Transformation",
    response.success_count,
    response.failed_count,
)

if response.failed_count > 0:
    error_json = response.errors_to_json()
    logger.error(f"Job Run Id: {job_run_id} Error Data: {error_json}")
    config_utils.upload_to_s3(
        error_json,
        f"{job_name}-{job_run_id}-admissions-cms-{timestamp}.log",
        data_artifacts_ssm_parameter,
        "output/transformation-errors/university_admissions_cms/",
    )

# Publish output to target S3 buckets on integration services account
university_cms_transformer.publish(response)
print("University CMS Data files published to S3")
