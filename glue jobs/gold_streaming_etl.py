import sys
import logging
import traceback
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

class GoldStreamingETL:

    # Constructor
    def __init__(self):

        # Resolve Glue Job Arguments
        args = getResolvedOptions(
            sys.argv,
            [
                "JOB_NAME",
                "SILVER_PATH",
                "GOLD_PATH",
                "CHECKPOINT_PATH",
                "SNS_TOPIC_ARN",
                "AWS_REGION"
            ]
        )

        self.job_name = args["JOB_NAME"]
        self.silver_path = args["SILVER_PATH"]
        self.gold_path = args["GOLD_PATH"]
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
        self.logger.info(
            "Spark Session Initialized Successfully"
        )

    # SNS Alert
    def send_sns_alert(self, subject, message):
        """SNS Alert"""

        self.sns_client.publish(
            TopicArn=self.sns_topic_arn,
            Subject=subject,
            Message=message
        )

    # Read Silver Delta Stream
    def read_silver_stream(self):
        """Read Silver Delta Stream"""

        self.logger.info(
            "Reading Silver Delta Stream"
        )

        silver_stream_df = (
            self.spark.readStream
            .format("delta")
            .load(self.silver_path)
        )
        return silver_stream_df

    # Aggregate Streaming Data
    def aggregate_stream(self, df):
        """Aggregate Streaming Data"""

        self.logger.info(
            "Applying Streaming Aggregations"
        )

        # Watermark
        watermarked_df = df.withWatermark("event_time", "60 minutes")

        # Rolling Window Aggregation
        aggregated_df = (
            watermarked_df
            .groupBy(window(col("event_time"), "60 minutes", "1 minute"),
                col("stock_symbol"),
                col("exchange")
            )
            .agg(
                sum("trade_volume").alias("total_trade_volume"),
                sum(col("trade_price") * col("trade_volume")).alias("total_trade_value"),
                avg("trade_price").alias("average_trade_price"),
                count("*").alias("total_trades")
            )
        )
        
        # Flatten Window Columns
        aggregated_df = (
            aggregated_df.withColumn("window_start", col("window.start"))\
            .withColumn("window_end", col("window.end"))
            .drop("window")
        )

        # Add Audit Timestamp
        aggregated_df = aggregated_df.withColumn("processed_timestamp", current_timestamp())

        return aggregated_df

    # Merge Into Gold Delta Table
    def merge_batch(self, micro_batch_df, batch_id):
        """Merge Into Gold Delta Table"""

        self.logger.info(
            f"Processing Gold Batch ID: {batch_id}"
        )

        if micro_batch_df.rdd.isEmpty():

            self.logger.info(
                "Empty Batch. Skipping Merge"
            )
            return

        # Create Gold Delta Table
        is_dt_table = DeltaTable.isDeltaTable(
            self.spark,
            self.gold_path
        )

        if not is_dt_table:

            self.logger.info(
                "Creating Gold Delta Table"
            )

            (
                micro_batch_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("stock_symbol")
                .save(self.gold_path)
            )
            return

        # Existing Gold Delta Table
        delta_table = DeltaTable.forPath(
            self.spark,
            self.gold_path
        )

        merge_condition = """
        target.stock_symbol = source.stock_symbol
        AND target.exchange = source.exchange
        AND target.window_start = source.window_start
        AND target.window_end = source.window_end
        """

        (
            delta_table.alias("target")
            .merge(
                micro_batch_df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdate(
                set={
                    "total_trade_volume": "source.total_trade_volume",
                    "total_trade_value": "source.total_trade_value",
                    "average_trade_price": "source.average_trade_price",
                    "total_trades": "source.total_trades",
                    "processed_timestamp": "source.processed_timestamp"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        self.logger.info(
            f"Gold Batch {batch_id} Merge Completed"
        )

    # Start Streaming Query
    def start_streaming_query(self, aggregated_df):
        """Start Streaming Query"""

        self.logger.info(
            "Starting Gold Streaming Query"
        )

        query = (
            aggregated_df.writeStream
            .foreachBatch(self.merge_batch)
            .outputMode("update")
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
                f"Starting Gold Streaming Job: {self.job_name}"
            )

            # Read Silver Stream
            silver_stream_df = self.read_silver_stream()

            # Aggregate Stream
            aggregated_df = self.aggregate_stream(
                silver_stream_df
            )

            # Start Streaming Sink
            self.start_streaming_query(
                aggregated_df
            )

        except Exception as e:

            error_message = traceback.format_exc()
            self.logger.error(error_message)
            self.send_sns_alert(
                subject=f"Gold Streaming Job Failed - {self.job_name}",
                message=error_message
            )
            raise e

# Main Entry Point
if __name__ == "__main__":

    job = GoldStreamingETL()
    job.process()