"""
Airflow DAG for reading data from S3 and loading into Snowflake with CloudWatch logging.

This DAG demonstrates:
- Reading data from S3
- Transforming data 
- Loading data into Snowflake
- CloudWatch metrics and logging
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_file_transform import S3FileTransformOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import task
from airflow.models import Variable
import boto3
import json
import logging
import pandas as pd
import io
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger(__name__)

def get_secret(secret_name: str, region_name: str = "us-east-1") -> dict:
    """
    Fetch secret from AWS Secrets Manager.
    """
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)

# Fetch AWS and Snowflake secrets
aws_secret = get_secret("aws/airflow/credentials")
snowflake_secret = get_secret("snowflake/airflow/credentials")


# AWS and Snowflake Connection IDs
AWS_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"
CLOUDWATCH_NAMESPACE = "AirflowDAGs"

# S3 Configuration
S3_BUCKET = aws_secret["s3_bucket"]
S3_PREFIX = aws_secret.get("s3_prefix", "raw_data/")   # optional default
S3_LOCAL_PATH = "/tmp/data/"

SNOWFLAKE_DATABASE = snowflake_secret["database"]
SNOWFLAKE_SCHEMA = snowflake_secret["schema"]
SNOWFLAKE_WAREHOUSE = snowflake_secret["warehouse"]
SNOWFLAKE_ROLE = snowflake_secret["role"]
SNOWFLAKE_TABLE = snowflake_secret["table"]


# DAG Configuration
default_args = {
    "owner": "data-ingestion",
    "depends_on_past": True,
    "start_date": datetime(2024, 1, 1),
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "s3_to_snowflake_load",
    default_args=default_args,
    description="Load data from S3 to Snowflake with CloudWatch monitoring",
    schedule_interval="0 2 * * *",  # Daily at 2 AM UTC
    catchup=False,
    tags=["s3", "snowflake", "data-pipeline"],
    doc_md=__doc__,
)


def send_cloudwatch_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    """
    Send a custom metric to CloudWatch.
    
    Args:
        metric_name: Name of the metric
        value: Metric value
        unit: Unit of measurement (Count, Bytes, Seconds, etc.)
    """
    try:
        cloudwatch = session.client("cloudwatch")
        cloudwatch.put_metric_data(
            Namespace=CLOUDWATCH_NAMESPACE,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Value": value,
                    "Unit": unit,
                    "Timestamp": datetime.utcnow(),
                    "Dimensions": [
                        {"Name": "DAGId", "Value": "s3_to_snowflake_load"},
                        {"Name": "Environment", "Value": "production"},
                    ],
                }
            ],
        )
        logger.info(f"CloudWatch metric sent: {metric_name} = {value} {unit}")
    except ClientError as e:
        logger.error(f"Failed to send CloudWatch metric: {str(e)}")
        raise


def check_s3_files() -> dict:
    """
    Check for new files in S3 and gather metadata.
    
    Returns:
        Dictionary containing file information
    """
    try:
        session = boto3.session.Session(
            aws_access_key_id=aws_secret["aws_access_key_id"],
            aws_secret_access_key=aws_secret["aws_secret_access_key"],
            region_name=aws_secret["region"]
        )
        s3_client = session.client("s3")

        
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=S3_PREFIX,
        )
        
        if "Contents" not in response:
            logger.info(f"No files found in s3://{S3_BUCKET}/{S3_PREFIX}")
            send_cloudwatch_metric("S3FilesCount", 0)
            return {"files": [], "count": 0}
        
        files = [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith((".csv", ".parquet", ".json"))]
        total_size = sum(obj["Size"] for obj in response["Contents"])
        
        logger.info(f"Found {len(files)} files in S3, total size: {total_size} bytes")
        
        # Send metrics to CloudWatch
        send_cloudwatch_metric("S3FilesCount", len(files))
        send_cloudwatch_metric("S3DataSize", total_size, "Bytes")
        
        return {
            "files": files,
            "count": len(files),
            "total_size": total_size,
        }
    except ClientError as e:
        logger.error(f"Failed to check S3 files: {str(e)}")
        send_cloudwatch_metric("S3CheckError", 1)
        raise


def validate_data(**context) -> bool:
    """
    Validate data before loading to Snowflake.
    
    Returns:
        True if validation passes
    """
    try:
        logger.info("Starting data validation")
        # Add custom validation logic here
        
        s3_client = boto3.client("s3", region_name="us-east-1")
        files = context['ti'].xcom_pull(task_ids="check_s3_files")["files"]
        
        for key in files:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), nrows=100)  # sample
            
            # Example checks
            if "customer_name" not in df.columns:
                raise ValueError("Missing column: customer_name")
            if df["email"].isnull().any():
                raise ValueError("Null emails found")
    
        send_cloudwatch_metric("DataValidationStatus", 1)  # 1 = success
        return True
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        send_cloudwatch_metric("DataValidationStatus", 0)  # 0 = failure
        raise


# Task 1: Check S3 for new files
check_s3_task = PythonOperator(
    task_id="check_s3_files",
    python_callable=check_s3_files,
    dag=dag,
)

# Task 2: Validate data
validate_data_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag,
)

# Task 3: Create or replace Snowflake table
create_snowflake_table = SnowflakeOperator(
    task_id="create_snowflake_table",
    sql=f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (
        id STRING,
        customer_name STRING,
        email STRING,
        country STRING,
        created_at TIMESTAMP_NTZ,
        updated_at TIMESTAMP_NTZ
    );
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    dag=dag,
)

# Task 4: Load data from S3 to Snowflake
load_s3_to_snowflake = S3ToSnowflakeOperator(
    task_id="load_s3_to_snowflake",
    s3_keys=[f"s3://{S3_BUCKET}/{S3_PREFIX}*.csv"],
    table=SNOWFLAKE_TABLE,
    schema=SNOWFLAKE_SCHEMA,
    database=SNOWFLAKE_DATABASE,
    file_format="(type=CSV, skip_header=1, field_delimiter=',')",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    aws_conn_id=AWS_CONN_ID,
    warehouse=SNOWFLAKE_WAREHOUSE,
    role=SNOWFLAKE_ROLE,
    dag=dag,
    on_failure_callback=lambda context: send_cloudwatch_metric("LoadFailure", 1),
)

# Task 5: Post-load data quality checks
data_quality_check = SnowflakeOperator(
    task_id="data_quality_check",
    sql=f"""
    SELECT COUNT(*) as total_rows
    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
    WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 day';
    """,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    dag=dag,
)

# Task 6: Log success metrics
def log_success_metrics(**context):
    """Log success metrics to CloudWatch."""
    logger.info("Pipeline executed successfully")
    send_cloudwatch_metric("PipelineSuccess", 1)
    send_cloudwatch_metric("PipelineFailure", 0)


log_metrics_task = PythonOperator(
    task_id="log_success_metrics",
    python_callable=log_success_metrics,
    dag=dag,
)

# Define task dependencies
check_s3_task >> validate_data_task >> create_snowflake_table >> load_s3_to_snowflake >> data_quality_check >> log_metrics_task
