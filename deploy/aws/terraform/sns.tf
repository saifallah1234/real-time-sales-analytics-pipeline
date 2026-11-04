module "alerts" {
  source  = "terraform-aws-modules/sns/aws"
  version = "7.2.0"
  name    = "cdc-alerts"

  tags = {
    Environment = "dev"
    Terraform   = "true"
  }
}
resource "aws_sns_topic_subscription" "email" {
  topic_arn = module.alerts.topic_arn
  protocol  = "email"
  endpoint  = var.alert_email
}
