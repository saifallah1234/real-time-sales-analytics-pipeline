# Identity of the Debezium EC2. Names must start with "cdc-" (TerraformUser's policy only allows those).

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "debezium_ec2" {
  name               = "cdc-debezium-ec2"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

resource "aws_iam_role_policy_attachment" "debezium_ssm" {
  role       = aws_iam_role.debezium_ec2.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# EC2 can't take a role directly; it takes an instance profile wrapping the role.
resource "aws_iam_instance_profile" "debezium_ec2" {
  name = "cdc-debezium-ec2"
  role = aws_iam_role.debezium_ec2.name
}

# Debezium Server writes change events to the Kinesis stream (this stream only).
data "aws_iam_policy_document" "debezium_kinesis" {
  statement {
    actions = [
      "kinesis:PutRecord",
      "kinesis:PutRecords",
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.cdc_events.arn]
  }
}

resource "aws_iam_role_policy" "debezium_kinesis" {
  name   = "cdc-debezium-kinesis"
  role   = aws_iam_role.debezium_ec2.id
  policy = data.aws_iam_policy_document.debezium_kinesis.json
}
