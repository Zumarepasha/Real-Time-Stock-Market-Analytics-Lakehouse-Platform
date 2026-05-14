
# Bronze Bucket

resource "aws_s3_bucket" "bronze_bucket" {

  bucket = "${var.project_name}-bronze-bucket"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {

  bucket = aws_s3_bucket.bronze_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Silver Bucket

resource "aws_s3_bucket" "silver_bucket" {

  bucket = "${var.project_name}-silver-bucket"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "silver_versioning" {

  bucket = aws_s3_bucket.silver_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Gold Bucket

resource "aws_s3_bucket" "gold_bucket" {

  bucket = "${var.project_name}-gold-bucket"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "gold_versioning" {

  bucket = aws_s3_bucket.gold_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Checkpoint Bucket

resource "aws_s3_bucket" "checkpoint_bucket" {

  bucket = "${var.project_name}-checkpoint-bucket"
  force_destroy = true
}

# Incremental Bucket

resource "aws_s3_bucket" "metadata_bucket" {

  bucket = "${var.project_name}-metadata-bucket"
  force_destroy = true
}

# MWAA Bucket

resource "aws_s3_bucket" "mwaa_bucket" {

  bucket = "${var.project_name}-mwaa-bucket"
  force_destroy = true
}

# DAG Folder Upload

resource "aws_s3_object" "dag_file" {

  bucket = aws_s3_bucket.mwaa_bucket.id

  key = "dags/stock_market_pipeline.py"
  source = "../airflow/stock_market_pipeline.py"
  etag = filemd5("../airflow/stock_market_pipeline.py")
}