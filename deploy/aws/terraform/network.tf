module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 6.7"

  name = "cdc-vpc"
  cidr = "10.0.0.0/16"

  # RDS needs 2 AZs; Redshift Serverless needs 3 (public subnets, one per AZ).
  azs              = ["${var.region_name}a", "${var.region_name}b", "${var.region_name}c"]
  public_subnets   = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  database_subnets = ["10.0.11.0/24", "10.0.12.0/24"]

  # Public IP lets the Debezium EC2 reach the internet without a NAT gateway (~$32/month).
  map_public_ip_on_launch = true
  enable_nat_gateway      = false
  enable_vpn_gateway      = false

  # DB subnets get no internet route: RDS stays private.
  create_database_subnet_group = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}
