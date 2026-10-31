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
