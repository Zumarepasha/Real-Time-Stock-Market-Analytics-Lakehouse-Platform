import sys
import json
import logging
import traceback
from datetime import datetime, timezone
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class RedshiftLoader:

    # Constructor
    def __init__(self):

        # Resolve Glue Job Arguments
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "GOLD_PATH",
                "CHECKPOINT_BUCKET",
                "CHECKPOINT_KEY",
                "REDSHIFT_JDBC_URL",
                "REDSHIFT_USER",
                "REDSHIFT_PASSWORD",
                "REDSHIFT_DATABASE",
                "REDSHIFT_SCHEMA",
                "REDSHIFT_TABLE",
                "REDSHIFT_CLUSTER",
                "REDSHIFT_STAGING_TABLE",
                "SNS_TOPIC_ARN",
                "AWS_REGION"
            ]
        )

        self.job_name = args["JOB_NAME"]
        self.gold_path = args["GOLD_PATH"]
        self.checkpoint_bucket = args["CHECKPOINT_BUCKET"]
        self.checkpoint_key = args["CHECKPOINT_KEY"]
        self.redshift_jdbc_url = args["REDSHIFT_JDBC_URL"]
        self.redshift_user = args["REDSHIFT_USER"]
        self.redshift_password = args["REDSHIFT_PASSWORD"]
        self.redshift_database = args["REDSHIFT_DATABASE"]
        self.redshift_schema = args["REDSHIFT_SCHEMA"]
        self.redshift_table = args["REDSHIFT_TABLE"]
        self.redshift_cluster = args["REDSHIFT_CLUSTER"]
        self.redshift_staging_table = args["REDSHIFT_STAGING_TABLE"]
        self.sns_topic_arn = args["SNS_TOPIC_ARN"]
        self.aws_region = args["AWS_REGION"]

        # Logger
        self.logger = logging.getLogger(self.job_name)
        self.logger.setLevel(logging.INFO)

        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
        stream_handler.setFormatter(formatter)

        if not self.logger.handlers:
            self.logger.addHandler(stream_handler)

        # AWS Clients
        self.s3_client = boto3.client(
            "s3",
            region_name=self.aws_region
        )

        self.sns_client = boto3.client(
            "sns",
            region_name=self.aws_region
        )

        self.redshift_data_api = boto3.client(
            "redshift-data",
            region_name=self.aws_region
        )

        # Spark Session
        self.spark = (
            SparkSession.builder
            .appName(self.job_name)
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info(
            "Spark Session Initialized Successfully"
        )

    # Send SNS Alert
    def send_sns_alert(self, subject, message):
        """Send SNS Alert"""

        self.sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )

    # Read Checkpoint File
    def read_checkpoint(self):
        """Read Checkpoint File"""

        self.logger.info(
            "Reading Incremental Checkpoint File"
        )

        try:

            response = self.s3_client.get_object(
                Bucket=self.checkpoint_bucket,
                Key=self.checkpoint_key
            )

            checkpoint_data = json.loads(
                response["Body"].read().decode("utf-8")
            )

            last_loaded_time = checkpoint_data[
                "last_loaded_time"
            ]

            self.logger.info(
                f"Last Loaded Timestamp: "
                f"{last_loaded_time}"
            )
            return last_loaded_time

        except self.s3_client.exceptions.NoSuchKey as e:

            self.logger.warning(
                "Checkpoint File Not Found. "
            )
            raise e

    # Update Checkpoint File
    def update_checkpoint(self, current_timestamp):
        """Update Checkpoint File"""

        self.logger.info(
            "Updating Incremental Checkpoint File"
        )

        checkpoint_payload = {
            "pipeline_name": self.job_name,
            "last_loaded_time": current_timestamp,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }

        self.s3_client.put_object(
            Bucket=self.checkpoint_bucket,
            Key=self.checkpoint_key,
            Body=json.dumps(checkpoint_payload),
            ContentType="application/json"
        )

        self.logger.info(
            "Checkpoint Updated Successfully"
        )

    # Read Gold Delta Table
    def read_gold_delta(self):
        """Read Gold Delta Table"""

        self.logger.info(
            "Reading Gold Delta Table"
        )

        gold_df = (
            self.spark.read
            .format("delta")
            .load(self.gold_path)
        )
        return gold_df

    # Get Incremental Data
    def get_incremental_data(self, df, last_loaded_time, current_timestamp):
        """Get Incremental Data"""

        self.logger.info(
            "Filtering Incremental Records"
        )

        incremental_df = df.filter(
            (col("processed_timestamp") >= last_loaded_time)
            &
            (col("processed_timestamp") < current_timestamp)
        )
        return incremental_df

    # Load Into Redshift Staging Table
    def load_to_staging(self, df):
        """Load Into Redshift Staging Table"""

        self.logger.info(
            "Loading Incremental Data Into "
            "Redshift Staging Table"
        )

        (
            df.write
            .format("jdbc")
            .option(
                "url", self.redshift_jdbc_url
            )
            .option(
                "dbtable",f"{self.redshift_schema}.{self.redshift_staging_table}"
            )
            .option(
                "user", self.redshift_user
            )
            .option(
                "password", self.redshift_password
            )
            .option(
                "driver", "com.amazon.redshift.jdbc42.Driver"
            )
            .mode("overwrite")
            .save()
        )

        self.logger.info(
            "Staging Load Completed"
        )

    # Execute Merge SQL
    def execute_merge_sql(self):
        """Execute Merge SQL"""

        self.logger.info(
            "Executing Redshift Merge"
        )

        merge_sql = f"""
        BEGIN;

        UPDATE
            {self.redshift_schema}.{self.redshift_table} target
        SET
            total_trade_volume = source.total_trade_volume,
            total_trade_value = source.total_trade_value,
            average_trade_price = source.average_trade_price,
            total_trades = source.total_trades,
            processed_timestamp = source.processed_timestamp
        FROM
            {self.redshift_schema}.{self.redshift_staging_table} source
        WHERE
            target.window_start = source.window_start
            AND target.window_end = source.window_end
            AND target.stock_symbol = source.stock_symbol
            AND target.exchange = source.exchange;


        DELETE FROM
            {self.redshift_schema}.{self.redshift_staging_table} stg
        USING
            {self.redshift_schema}.{self.redshift_table} target
        WHERE
            target.window_start = stg.window_start
            AND target.window_end = stg.window_end
            AND target.stock_symbol = stg.stock_symbol
            AND target.exchange = stg.exchange;

        INSERT INTO
            {self.redshift_schema}.{self.redshift_table}
        SELECT *
        FROM
            {self.redshift_schema}.{self.redshift_staging_table};

        END;"""

        response = self.redshift_data_api.execute_statement(
            ClusterIdentifier=self.redshift_cluster,
            Database=self.redshift_database,
            DbUser=self.redshift_user,
            Sql=merge_sql
        )

        self.logger.info(
            f"Merge Statement Submitted: "
            f"{response['Id']}"
        )

    # Main Process
    def process(self):

        try:

            self.logger.info(
                f"Starting Job: {self.job_name}"
            )

            # Read Last Checkpoint
            last_loaded_time = self.read_checkpoint()

            # Current Batch Timestamp
            current_timestamp = datetime.now(timezone.utc).isoformat()

            self.logger.info(
                f"Current Batch Timestamp: "
                f"{current_timestamp}"
            )

            # Read Gold Delta Table
            gold_df = self.read_gold_delta()

            # Filter Incremental Data
            incremental_df = self.get_incremental_data(gold_df, last_loaded_time, current_timestamp)

            # Check Empty Batch
            if incremental_df.rdd.isEmpty():

                self.logger.info(
                    "No Incremental Records Found"
                )

                return

            # Load Into Staging
            self.load_to_staging(incremental_df)

            # Execute Merge
            self.execute_merge_sql()

            # Update Checkpoint ONLY AFTER SUCCESS
            self.update_checkpoint(current_timestamp)

            self.logger.info(
                "Incremental Redshift Load Completed"
            )

        except Exception as e:

            error_message = traceback.format_exc()
            self.logger.error(error_message)

            self.send_sns_alert(
                subject=f"Redshift Loader Failed - "
                        f"{self.job_name}",
                message=error_message
            )
            raise e

# Main Entry Point
if __name__ == "__main__":

    job = RedshiftLoader()
    job.process()