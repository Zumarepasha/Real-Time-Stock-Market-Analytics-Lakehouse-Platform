from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.trigger_rule import TriggerRule


# Default Arguments
default_args = {
    "owner": "data-engineering-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# DAG Definition
@dag(
    dag_id="stock_market_batch_maintenance_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    concurrency=10,
    tags=[
        "stocks",
        "delta-lake",
        "redshift",
        "maintenance",
        "production"
    ]
)

def stock_market_batch_maintenance_pipeline():

    # Start Task
    @task
    def start_pipeline():

        print(
            "Starting Stock Market "
            "Batch Maintenance Pipeline"
        )

    # Redshift Incremental Loader
    redshift_loader = GlueJobOperator(
        task_id="redshift_loader",
        job_name="redshift_incremental_loader",

        script_args={

            "--JOB_NAME":
                "redshift_incremental_loader",

            "--GOLD_PATH":
                "s3://stock-gold-bucket/delta/",

            "--CHECKPOINT_BUCKET":
                "stock-metadata-bucket",

            "--CHECKPOINT_KEY":
                "redshift-loader/checkpoint.json",

            "--REDSHIFT_JDBC_URL":
                "jdbc:redshift://redshift-cluster.us-east-1.redshift.amazonaws.com:5439/dev",

            "--REDSHIFT_USER":
                "admin",

            "--REDSHIFT_PASSWORD":
                "replace-with-secret",

            "--REDSHIFT_DATABASE":
                "dev",

            "--REDSHIFT_SCHEMA":
                "analytics",

            "--REDSHIFT_TABLE":
                "stock_market_summary",

            "--REDSHIFT_STAGING_TABLE":
                "stock_market_summary_stg",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:stock-alerts",

            "--AWS_REGION":
                "us-east-1"
        },

        wait_for_completion=True,
        aws_conn_id="aws_default"
    )

    # Optimize Gold Delta Table
    optimize_gold = GlueJobOperator(

        task_id="optimize_gold_delta",
        job_name="optimize_gold_delta",

        script_args={

            "--JOB_NAME":
                "optimize_gold_delta",

            "--TABLE_PATH":
                "s3://stock-gold-bucket/delta/",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:stock-alerts",

            "--AWS_REGION":
                "us-east-1"
        },

        wait_for_completion=True,
        aws_conn_id="aws_default"
    )

    # Vacuum Silver Delta Table
    vacuum_silver = GlueJobOperator(

        task_id="vacuum_silver_delta",
        job_name="vacuum_silver_delta",
        script_args={

            "--JOB_NAME":
                "vacuum_silver_delta",

            "--TABLE_PATH":
                "s3://stock-silver-bucket/delta/",

            "--RETENTION_HOURS":
                "168",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:stock-alerts",

            "--AWS_REGION":
                "us-east-1"
        },

        wait_for_completion=True,
        aws_conn_id="aws_default"
    )

    # Vacuum Gold Delta Table
    vacuum_gold = GlueJobOperator(

        task_id="vacuum_gold_delta",
        job_name="vacuum_gold_delta",

        script_args={

            "--JOB_NAME":
                "vacuum_gold_delta",

            "--TABLE_PATH":
                "s3://stock-gold-bucket/delta/",

            "--RETENTION_HOURS":
                "168",

            "--SNS_TOPIC_ARN":
                "arn:aws:sns:us-east-1:974175181125:stock-alerts",

            "--AWS_REGION":
                "us-east-1"
        },

        wait_for_completion=True,
        aws_conn_id="aws_default"
    )

    # Success Notification
    success_notification = SnsPublishOperator(

        task_id="success_notification",
        target_arn="arn:aws:sns:us-east-1:974175181125:stock-alerts",

        subject="Stock Pipeline Success",
        message=(
            "Stock Market Batch Maintenance "
            "Pipeline Completed Successfully"
        ),

        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Failure Notification
    failure_notification = SnsPublishOperator(

        task_id="failure_notification",

        target_arn= "arn:aws:sns:us-east-1:974175181125:stock-alerts",

        subject="Stock Pipeline Failure",
        message=(
            "Stock Market Batch Maintenance "
            "Pipeline Failed"
        ),

        aws_conn_id="aws_default",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # Start Task
    start = start_pipeline()

    # DAG Dependencies

    (
        start
        >>
        redshift_loader
        >>
        optimize_gold
        >>
        [
            vacuum_silver,
            vacuum_gold
        ]
        >>
        success_notification
    )

    # Failure Flow

    [
        redshift_loader,
        optimize_gold,
        vacuum_silver,
        vacuum_gold
    ] >> failure_notification

# Instantiate DAG
stock_market_batch_maintenance_pipeline()