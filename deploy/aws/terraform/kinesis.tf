# All 12 tables go to this one stream (Debezium's ByLogicalTableRouter).
# One shard keeps every change in commit order and costs ~$0.018/h.
resource "aws_kinesis_stream" "cdc_events" {
  name             = "cdc_events"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}
