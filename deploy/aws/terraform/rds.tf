# Source database for Debezium. Private, reachable only from the Debezium EC2.

resource "aws_security_group" "rds" {
  name        = "cdc-rds-sg"
  description = "Postgres access from the Debezium EC2 only"
  vpc_id      = module.vpc.vpc_id
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_debezium" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = module.ec2_instance.security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

# Kept in state only (state is gitignored). Secrets Manager would cost money and isn't in the IAM policy.
resource "random_password" "db" {
  length  = 24
  special = false
}

module "db" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 7.2"

  identifier = "cdc-db"

  engine               = "postgres"
  engine_version       = "15"
  family               = "postgres15"
  major_engine_version = "15"
  instance_class       = "db.t4g.micro"

  allocated_storage = 20
  storage_type      = "gp3"

  # No db_name: postgres/init/01_init.sql creates cdc_project itself.
  username                    = "postgres"
  manage_master_user_password = false
  password_wo                 = random_password.db.result
  password_wo_version         = 1
  port                        = 5432

  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.rds.id]
  publicly_accessible    = false
  multi_az               = false

  # Debezium reads the WAL through logical replication.
  parameters = [
    {
      name         = "rds.logical_replication"
      value        = "1"
      apply_method = "pending-reboot"
    }
  ]
  create_db_option_group = false

  # Ephemeral test DB: no backups or final snapshot (they'd keep billing after destroy).
  backup_retention_period = 0
  skip_final_snapshot     = true
  deletion_protection     = false
  apply_immediately       = true

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}
