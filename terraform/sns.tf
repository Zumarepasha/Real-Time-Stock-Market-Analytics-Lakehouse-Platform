resource "aws_sns_topic" "pipeline_alerts" {

  name = "${var.project_name}-alerts"
}

resource "aws_sns_topic_subscription" "email_alerts" {

  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol = "email"
  endpoint = var.alert_email
}