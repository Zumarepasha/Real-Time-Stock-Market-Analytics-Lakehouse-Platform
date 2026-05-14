
# Glue Failure Alarm

resource "aws_cloudwatch_metric_alarm" "glue_job_failure_alarm" {

  alarm_name = "${var.project_name}-glue-failure-alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  metric_name = "glue.driver.aggregate.numFailedTasks"
  namespace = "Glue"
  period = 300
  statistic = "Sum"
  threshold = 1
  alarm_description = "Glue Job Failure Alarm"

  alarm_actions = [
    aws_sns_topic.pipeline_alerts.arn
  ]
}

# MWAA CPU Alarm

resource "aws_cloudwatch_metric_alarm" "mwaa_cpu_alarm" {

  alarm_name = "${var.project_name}-mwaa-cpu-alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 2
  metric_name = "CPUUtilization"
  namespace = "AWS/MWAA"
  period = 300
  statistic = "Average"
  threshold = 80

  alarm_actions = [
    aws_sns_topic.pipeline_alerts.arn
  ]
}