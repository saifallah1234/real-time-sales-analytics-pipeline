# Warehouse: Redshift Serverless reads the Kinesis stream through a streaming materialized view.
# The view itself is created by SQL after apply (see CLAUDE.md, AWS deployment step 3).

# Redshift wants at least one upper, lower and digit.
resource "random_password" "redshift" {
  length      = 24
  special     = false
  min_upper   = 1
  min_lower   = 1
  min_numeric = 1
}

# --- IAM: Redshift assumes this role to read cdc_events ---
data "aws_iam_policy_document" "redshift_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_kinesis" {
  name               = "cdc-redshift-kinesis"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume_role.json
}

data "aws_iam_policy_document" "redshift_kinesis" {
  statement {
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.cdc_events.arn]
  }

  statement {
    actions   = ["kinesis:ListStreams"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "redshift_kinesis" {
  name   = "cdc-redshift-kinesis"
  role   = aws_iam_role.redshift_kinesis.id
  policy = data.aws_iam_policy_document.redshift_kinesis.json
}

# --- Network: public endpoint, reachable only from my IP (local Airflow/dbt) ---
resource "aws_security_group" "redshift" {
  name        = "cdc-redshift-sg"
  description = "Redshift access from my IP only"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "redshift_from_my_ip" {
  security_group_id = aws_security_group.redshift.id
  cidr_ipv4         = var.my_ip
  ip_protocol       = "tcp"
  from_port         = 5439
  to_port           = 5439
}

# --- Redshift Serverless ---
resource "aws_redshiftserverless_namespace" "cdc" {
  namespace_name      = "cdc-namespace"
  db_name             = "dev"
  admin_username      = "admin"
  admin_user_password = random_password.redshift.result
  iam_roles           = [aws_iam_role.redshift_kinesis.arn]
}

resource "aws_redshiftserverless_workgroup" "cdc" {
  namespace_name = aws_redshiftserverless_namespace.cdc.namespace_name
  workgroup_name = "cdc-workgroup"

  # 8 RPU is the minimum; capping max at 8 stops it scaling up the bill.
  base_capacity = 8
  max_capacity  = 8

  publicly_accessible = true
  subnet_ids          = module.vpc.public_subnets
  security_group_ids  = [aws_security_group.redshift.id]
}

# Safety net: shut the workgroup off after ~2h of compute per day.
resource "aws_redshiftserverless_usage_limit" "cdc" {
  resource_arn  = aws_redshiftserverless_workgroup.cdc.arn
  usage_type    = "serverless-compute"
  amount        = 16
  period        = "daily"
  breach_action = "deactivate"
}
