variable "region_name" {
  description = "The AWS region to deploy resources in"
  type        = string
  default     = "eu-central-1"
}

variable "cdc_tag" {
  description = "The tag to identify resources created by the CDC project"
  type        = string
  default     = "cdc"
}

variable "my_ip" {
  description = "Your public IP as a /32 CIDR, allowed to reach Redshift on 5439 (set in a git-ignored terraform.tfvars)"
  type        = string
}

variable "alert_email" {
  description = "The email address to receive alerts from SNS (set in a git-ignored terraform.tfvars)"
  type        = string
}
