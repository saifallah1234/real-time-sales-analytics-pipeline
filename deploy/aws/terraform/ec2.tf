module "ec2_instance" {
  source  = "terraform-aws-modules/ec2-instance/aws"
  version = "~> 6.4"

  name = "cdc-debezium"

  instance_type        = "t3.small"
  subnet_id            = module.vpc.public_subnets[0]
  iam_instance_profile = aws_iam_instance_profile.debezium_ec2.name
  ami                  = data.aws_ami.amazone_linux_2.id
  # The module creates a security group: no inbound rules, all outbound (needed for SSM + downloads).
  security_group_name = "cdc-debezium-sg"

  metadata_options = {
    http_tokens = "required"
  }

  root_block_device = {
    size      = 20
    type      = "gp3"
    encrypted = true
  }

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}

data "aws_ami" "amazone_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-kernel-6.18-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

