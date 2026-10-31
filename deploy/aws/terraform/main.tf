provider "aws" {
  region = var.region_name
  default_tags { tags = { Project = var.cdc_tag } }
}