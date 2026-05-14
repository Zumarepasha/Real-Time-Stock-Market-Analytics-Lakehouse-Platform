resource "aws_mwaa_environment" "mwaa" {

  name = "${var.project_name}-mwaa"
  airflow_version = "2.8.1"
  environment_class = "mw1.small"
  execution_role_arn = "Iam/role"
  source_bucket_arn = aws_s3_bucket.mwaa_bucket.arn
  dag_s3_path = "dags"
  requirements_s3_path = "requirements/requirements.txt"

  network_configuration {

    security_group_ids = [
      aws_security_group.mwaa_sg.id
    ]
    subnet_ids = [
      aws_subnet.private_subnet_1.id,
      aws_subnet.private_subnet_2.id
    ]
  }

  webserver_access_mode = "PUBLIC_ONLY"

  logging_configuration {

    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    scheduler_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
    webserver_logs {
      enabled   = true
      log_level = "INFO"
    }
    worker_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  weekly_maintenance_window_start = "SUN:03:00"

  min_workers = 1
  max_workers = 5
  schedulers = 2
}