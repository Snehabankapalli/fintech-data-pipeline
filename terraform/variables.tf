variable "aws_region"        { default = "us-east-1" }
variable "env"               { default = "production" }
variable "project_name"      { default = "fintech-pipeline" }
variable "snowflake_account" { sensitive = true }
variable "aws_access_key"    { sensitive = true }
variable "aws_secret_key"    { sensitive = true }
