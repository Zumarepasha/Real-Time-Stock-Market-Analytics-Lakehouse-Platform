
# Kinesis Stream

resource "aws_kinesis_stream" "stock_stream" {

  name = "${var.project_name}-stock-stream"
  retention_period = 24

  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords"
  ]
}

# Firehose Delivery Stream

resource "aws_kinesis_firehose_delivery_stream" "bronze_firehose" {

  name = "${var.project_name}-bronze-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {

    kinesis_stream_arn = aws_kinesis_stream.stock_stream.arn
    role_arn = "Iam/role"
  }

  extended_s3_configuration {

    role_arn = "Iam/role"
    bucket_arn = aws_s3_bucket.bronze_bucket.arn
    prefix = "raw-stock-events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
    buffering_interval = 60
    buffering_size = 128
    compression_format = "GZIP"
    error_output_prefix = "errors/"
  }
}