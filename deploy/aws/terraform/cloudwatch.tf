# Pipeline alarms; each one emails the SNS topic in sns.tf. Time to fire = period x evaluation_periods.
# Debezium and "not consumed" start in ALARM after apply, until the manual steps and first DAG run are done.

module "debezium_silent_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-debezium-silent"
  alarm_description   = "No records in Kinesis for 10 min, despite the 1/min heartbeat: Debezium is down."
  namespace           = "AWS/Kinesis"
  metric_name         = "IncomingRecords"
  dimensions          = { StreamName = aws_kinesis_stream.cdc_events.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 2
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

module "kinesis_write_throttled_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-kinesis-write-throttled"
  alarm_description   = "Writes exceed the shard limit (1 MB/s or 1,000 records/s)."
  namespace           = "AWS/Kinesis"
  metric_name         = "WriteProvisionedThroughputExceeded"
  dimensions          = { StreamName = aws_kinesis_stream.cdc_events.name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 0
  treat_missing_data  = "notBreaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

# The MV only reads when dbt_dag refreshes it, so iterator age has no data once refreshes stop: this alarm covers that.
module "kinesis_not_consumed_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-kinesis-not-consumed"
  alarm_description   = "No MV refresh read Kinesis for 6h; events expire after 24h. Is dbt_dag running?"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.Success"
  dimensions          = { StreamName = aws_kinesis_stream.cdc_events.name }
  statistic           = "SampleCount"
  period              = 21600
  evaluation_periods  = 1
  comparison_operator = "LessThanThreshold"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

module "kinesis_iterator_age_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-kinesis-iterator-age"
  alarm_description   = "MV refresh is over 12h behind (50% of the 24h retention): events risk expiring."
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  dimensions          = { StreamName = aws_kinesis_stream.cdc_events.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 43200000
  treat_missing_data  = "notBreaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

module "rds_slot_lag_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-rds-slot-lag"
  alarm_description   = "Replication slot is over 5 GB behind: WAL is filling the 20 GB RDS disk."
  namespace           = "AWS/RDS"
  metric_name         = "OldestReplicationSlotLag"
  dimensions          = { DBInstanceIdentifier = module.db.db_instance_identifier }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 5368709120
  treat_missing_data  = "notBreaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

module "rds_low_storage_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name          = "cdc-rds-low-storage"
  alarm_description   = "Under 4 GB free on the RDS disk."
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  dimensions          = { DBInstanceIdentifier = module.db.db_instance_identifier }
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "LessThanThreshold"
  threshold           = 4294967296
  treat_missing_data  = "notBreaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}

module "redshift_usage_limit_alarm" {
  source  = "terraform-aws-modules/cloudwatch/aws//modules/metric-alarm"
  version = "~> 6.0"

  alarm_name        = "cdc-redshift-usage-limit"
  alarm_description = "Over 80% of the daily 16 RPU-hour limit: Redshift deactivates itself at 100%."
  namespace         = "AWS/Redshift-Serverless"
  metric_name       = "UsageLimitConsumed"
  dimensions = {
    UsageLimitId = aws_redshiftserverless_usage_limit.cdc.id
    UsageType    = "SERVERLESS_COMPUTE"
    Workgroup    = aws_redshiftserverless_workgroup.cdc.workgroup_name
  }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  comparison_operator = "GreaterThanThreshold"
  threshold           = 12.8
  treat_missing_data  = "notBreaching"
  alarm_actions       = [module.alerts.topic_arn]
  ok_actions          = [module.alerts.topic_arn]
}


locals {
  stream    = aws_kinesis_stream.cdc_events.name
  db        = module.db.db_instance_identifier
  workgroup = aws_redshiftserverless_workgroup.cdc.workgroup_name

  dashboard_graphs = [
    { title = "RDS slot lag (bytes)", stat = "Maximum", metric = ["AWS/RDS", "OldestReplicationSlotLag", "DBInstanceIdentifier", local.db] },
    { title = "RDS free storage (bytes)", stat = "Minimum", metric = ["AWS/RDS", "FreeStorageSpace", "DBInstanceIdentifier", local.db] },
    { title = "Kinesis incoming records", stat = "Sum", metric = ["AWS/Kinesis", "IncomingRecords", "StreamName", local.stream] },
    { title = "Kinesis write throttling", stat = "Sum", metric = ["AWS/Kinesis", "WriteProvisionedThroughputExceeded", "StreamName", local.stream] },
    { title = "MV reads (GetRecords calls)", stat = "SampleCount", metric = ["AWS/Kinesis", "GetRecords.Success", "StreamName", local.stream] },
    { title = "Iterator age (ms)", stat = "Maximum", metric = ["AWS/Kinesis", "GetRecords.IteratorAgeMilliseconds", "StreamName", local.stream] },
    { title = "Redshift usage limit consumed (RPU-hours)", stat = "Maximum", metric = ["AWS/Redshift-Serverless", "UsageLimitConsumed", "UsageLimitId", aws_redshiftserverless_usage_limit.cdc.id, "UsageType", "SERVERLESS_COMPUTE", "Workgroup", local.workgroup] },
  ]
}

resource "aws_cloudwatch_dashboard" "cdc" {
  dashboard_name = "cdc-pipeline"
  dashboard_body = jsonencode({
    widgets = [for i, g in local.dashboard_graphs : {
      type   = "metric"
      width  = 12
      height = 6
      x      = (i % 2) * 12
      y      = floor(i / 2) * 6
      properties = {
        title   = g.title
        region  = var.region_name
        view    = "timeSeries"
        stat    = g.stat
        period  = 300
        metrics = [g.metric]
      }
    }]
  })
}
