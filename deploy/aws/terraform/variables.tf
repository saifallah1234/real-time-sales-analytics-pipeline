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