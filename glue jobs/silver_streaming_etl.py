import sys
import logging
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable


class SilverStreamingETL:

    # Constructor
    def __init__(self):

        # Resolve Glue Arguments
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "BRONZE_PATH",
                "SILVER_PATH",
                "CHECKPOINT_PATH",
                "SNS_TOPIC_ARN",
                "AWS_REGION"
            ]
        )

        self.job_name = args["JOB_NAME"]
        self.bronze_path = args["BRONZE_PATH"]
        self.silver_path = args["SILVER_PATH"]
        self.checkpoint_path = args["CHECKPOINT_PATH"]
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

        # SNS Client
        self.sns_client = boto3.client(
            "sns",
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
            .config(
                "spark.sql.adaptive.enabled",
                "true"
            )
            .config(
                "spark.sql.shuffle.partitions",
                "200"
            )
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        self.logger.info("Spark Session Initialized Successfully")

        # Define Bronze Schema
        self.schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("stock_symbol", StringType(), True),
            StructField("trade_price", DoubleType(), True),
            StructField("trade_volume", LongType(), True),
            StructField("trade_type", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("event_time", StringType(), True)
        ])

    # SNS Alert
    def send_sns_alert(self, subject, message):
        """SNS Alert"""

        self.sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )

    # Read Bronze Streaming Data
    def read_bronze_stream(self):
        """Read Bronze Streaming Data from S3"""

        self.logger.info(
            "Starting Bronze Streaming Read from S3"
        )

        bronze_stream_df = (
            self.spark.readStream
            .format("parquet")
            .schema(self.schema)
            .load(self.bronze_path)
        )

        return bronze_stream_df

    # Transform Streaming Data
    def transform_stream(self, df):
        """Transform Streaming Data"""

        self.logger.info(
            "Applying Streaming Transformations"
        )

        transformed_df = (
            df.withColumn(
                "event_time",
                to_timestamp(col("event_time"))
            )
            .withColumn(
                "stock_symbol",
                upper(trim(col("stock_symbol")))
            )
            .withColumn(
                "trade_type",
                upper(trim(col("trade_type")))
            )
            .withColumn(
                "exchange",
                upper(trim(col("exchange")))
            )
            .withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
        )

        # Filter Invalid Records
        transformed_df = transformed_df.filter(
            (col("event_id").isNotNull()) &
            (col("stock_symbol").isNotNull()) &
            (col("trade_price") > 0) &
            (col("trade_volume") > 0) &
            (col("event_time").isNotNull())
        )

        # Watermark and Deduplication
        transformed_df = transformed_df.withWatermark("event_time","20 minutes")\
                        .dropDuplicates(["event_id"])
        
        # Partition Columns
        transformed_df = (
            transformed_df
            .withColumn(
                "year",
                year(col("event_time"))
            )
            .withColumn(
                "month",
                month(col("event_time"))
            )
            .withColumn(
                "day",
                dayofmonth(col("event_time"))
            )
            .withColumn(
                "hour",
                hour(col("event_time"))
            )
        )

        return transformed_df

    # Merge Function
    def merge_batch(self, micro_batch_df, batch_id):
        """Merge Function"""

        self.logger.info(
            f"Processing Batch ID: {batch_id}"
        )

        if micro_batch_df.rdd.isEmpty():

            self.logger.info(
                "Micro Batch Empty. Skipping Merge"
            )
            return

        is_dt_table = DeltaTable.isDeltaTable(
            self.spark,
            self.silver_path
        )

        # Create Silver Delta Table If Not Exists
        if not is_dt_table:

            self.logger.info(
                "Creating New Silver Delta Table"
            )
            (
                micro_batch_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy(
                    "year",
                    "month",
                    "day",
                    "hour"
                )
                .save(self.silver_path)
            )
            return

        # Delta Merge
        delta_table = DeltaTable.forPath(
            self.spark,
            self.silver_path
        )

        (
            delta_table.alias("target")
            .merge(
                micro_batch_df.alias("source"),
                "target.event_id = source.event_id"
            )
            .whenMatchedUpdate(
                set={
                    "stock_symbol": "source.stock_symbol",
                    "trade_price": "source.trade_price",
                    "trade_volume": "source.trade_volume",
                    "trade_type": "source.trade_type",
                    "exchange": "source.exchange",
                    "event_time": "source.event_time",
                    "ingestion_timestamp": "source.ingestion_timestamp"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        self.logger.info(
            f"Batch {batch_id} Merge Completed"
        )

    # Start Streaming Query
    def start_streaming_query(self, transformed_df):
        """Start Streaming Query"""

        self.logger.info(
            "Starting Streaming Write to Silver Delta"
        )

        query = (
            transformed_df.writeStream
            .foreachBatch(self.merge_batch)
            .outputMode("append")
            .option(
                "checkpointLocation",
                self.checkpoint_path
            )
            .trigger(processingTime="1 minute")
            .start()
        )

        query.awaitTermination()

    # Main Process
    def process(self):
        """Main Process"""

        try:

            self.logger.info(
                f"Starting Glue Streaming Job: {self.job_name}"
            )

            # Read Stream
            bronze_stream_df = self.read_bronze_stream()

            # Transform Stream
            transformed_df = self.transform_stream(bronze_stream_df)

            # Start Streaming Sink
            self.start_streaming_query(transformed_df)

        except Exception as e:

            error_message = traceback.format_exc()

            self.logger.error(error_message)
            self.send_sns_alert(
                subject=f"Glue Streaming Job Failed - {self.job_name}",
                message=error_message
            )
            raise e

        finally:
            self.spark.stop()
            self.logger.info(
                "Spark Session Stopped"
            )

# Main Entry Point
if __name__ == "__main__":

    job = SilverStreamingETL()
    job.process()