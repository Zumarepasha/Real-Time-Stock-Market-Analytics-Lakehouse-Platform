
# Glue Database

resource "aws_glue_catalog_database" "stock_market_db" {

  name = "${var.project_name}_db"
}

# Bronze Glue Crawler

resource "aws_glue_crawler" "bronze_crawler" {

  name = "${var.project_name}-bronze-crawler"
  role = "Iam/role"

  database_name = aws_glue_catalog_database.stock_market_db.name
  table_prefix = "bronze_"

  s3_target {

    path = "s3://${aws_s3_bucket.bronze_bucket.bucket}/raw-stock-events/"
  }

  schema_change_policy {

    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# Silver Glue Crawler

resource "aws_glue_crawler" "silver_crawler" {

  name = "${var.project_name}-silver-crawler"
  role = "Iam/role"

  database_name = aws_glue_catalog_database.stock_market_db.name
  table_prefix = "silver_"

  delta_target {

    delta_tables = [
      "s3://${aws_s3_bucket.silver_bucket.bucket}/delta/"
    ]
    create_native_delta_table = true
  }

  schema_change_policy {

    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# Gold Glue Crawler

resource "aws_glue_crawler" "gold_crawler" {

  name = "${var.project_name}-gold-crawler"
  role = "Iam/role"

  database_name = aws_glue_catalog_database.stock_market_db.name
  table_prefix = "gold_"

  delta_target {

    delta_tables = [
      "s3://${aws_s3_bucket.gold_bucket.bucket}/delta/"
    ]
    create_native_delta_table = true
  }

  schema_change_policy {

    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
}

# Bronze Catalog Table

resource "aws_glue_catalog_table" "bronze_stock_events" {

  name = "bronze_stock_events"
  database_name = aws_glue_catalog_database.stock_market_db.name
  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "json"
  }

  storage_descriptor {

    location = "s3://${aws_s3_bucket.bronze_bucket.bucket}/raw-stock-events/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    serde_info {

      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    columns {

      name = "stock_symbol"
      type = "string"
    }

    columns {

      name = "exchange"
      type = "string"
    }

    columns {

      name = "trade_price"
      type = "double"
    }

    columns {

      name = "trade_volume"
      type = "bigint"
    }

    columns {

      name = "event_timestamp"
      type = "timestamp"
    }
  }
}

# Silver Catalog Table

resource "aws_glue_catalog_table" "silver_stock_events" {

  name = "silver_stock_events"
  database_name = aws_glue_catalog_database.stock_market_db.name
  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "delta"
  }

  storage_descriptor {

    location = "s3://${aws_s3_bucket.silver_bucket.bucket}/delta/"
    input_format  = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"

    columns {

      name = "stock_symbol"
      type = "string"
    }

    columns {

      name = "exchange"
      type = "string"
    }

    columns {

      name = "trade_price"
      type = "double"
    }

    columns {

      name = "trade_volume"
      type = "bigint"
    }

    columns {

      name = "event_timestamp"
      type = "timestamp"
    }

    columns {

      name = "processed_timestamp"
      type = "timestamp"
    }
  }
}

# Gold Catalog Table

resource "aws_glue_catalog_table" "gold_stock_aggregates" {

  name = "gold_stock_aggregates"
  database_name = aws_glue_catalog_database.stock_market_db.name
  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "delta"
  }

  storage_descriptor {

    location = "s3://${aws_s3_bucket.gold_bucket.bucket}/delta/"
    input_format  = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"

    columns {

      name = "window_start"
      type = "timestamp"
    }

    columns {

      name = "window_end"
      type = "timestamp"
    }

    columns {

      name = "stock_symbol"
      type = "string"
    }

    columns {

      name = "exchange"
      type = "string"
    }

    columns {

      name = "total_trade_volume"
      type = "bigint"
    }

    columns {

      name = "total_trade_value"
      type = "double"
    }

    columns {

      name = "average_trade_price"
      type = "double"
    }

    columns {

      name = "total_trades"
      type = "bigint"
    }

    columns {

      name = "processed_timestamp"
      type = "timestamp"
    }
  }
}

# Silver Streaming Job

resource "aws_glue_job" "silver_streaming_job" {

  name     = "silver_incremental_etl"
  role_arn = "Iam/role"
  glue_version = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  execution_class = "FLEX"

  command {

    script_location = "s3://${aws_s3_bucket.silver_bucket.bucket}/scripts/silver_incremental_etl.py"
    python_version = "3"
  }

  default_arguments = {

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-job-insights" = "true"
    "--TempDir" = "s3://${aws_s3_bucket.checkpoint_bucket.bucket}/temp/"
  }

  execution_property {

    max_concurrent_runs = 1
  }
  max_retries = 1
  timeout = 2880
}

# Gold Streaming Job

resource "aws_glue_job" "gold_streaming_job" {

  name     = "gold_streaming_etl"
  role_arn = "Iam/role"

  glue_version = "4.0"
  worker_type       = "G.2X"
  number_of_workers = 10
  command {

    script_location = "s3://${aws_s3_bucket.gold_bucket.bucket}/scripts/gold_streaming_etl.py"
    python_version = "3"
  }

  default_arguments = {

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-job-insights" = "true"
    "--TempDir" = "s3://${aws_s3_bucket.checkpoint_bucket.bucket}/temp/"
  }

  execution_property {

    max_concurrent_runs = 1
  }
  timeout = 2880
}

# Redshift Loader Job

resource "aws_glue_job" "redshift_loader_job" {

  name     = "redshift_incremental_loader"
  role_arn = "Iam/role"

  glue_version = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  command {

    script_location = "s3://${aws_s3_bucket.mwaa_bucket.bucket}/scripts/redshift_loader.py"
    python_version = "3"
  }

  default_arguments = {

    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics" = "true"
    "--enable-job-insights" = "true"
  }

  max_retries = 2
  timeout = 60
}