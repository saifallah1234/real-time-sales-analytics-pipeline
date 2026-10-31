output "vpc_id" {
  value = module.vpc.vpc_id
}

output "public_subnet_ids" {
  value = module.vpc.public_subnets
}

output "database_subnet_group_name" {
  value = module.vpc.database_subnet_group_name
}

output "debezium_instance_id" {
  value = module.ec2_instance.id
}

output "db_address" {
  value = module.db.db_instance_address
}

output "db_password" {
  value     = random_password.db.result
  sensitive = true
}
