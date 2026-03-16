##############################################################################
# Fintech Data Pipeline — AWS Infrastructure
# Mirrors SoFi-grade serverless-first architecture
# Provisions: S3 buckets, EMR Serverless, Glue, Lambda, IAM roles
##############################################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
  }

  backend "s3" {
    bucket = "your-terraform-state-bucket"
    key    = "fintech-pipeline/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

provider "snowflake" {
  account = var.snowflake_account
  role    = "SYSADMIN"
}

# ── S3 Buckets ───────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${var.env}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "processed" {
  bucket = "${var.project_name}-processed-${var.env}"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project_name}-scripts-${var.env}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── EMR Serverless Application ───────────────────────────────────────────────

resource "aws_emrserverless_application" "spark" {
  name          = "${var.project_name}-spark-${var.env}"
  release_label = "emr-6.15.0"
  type          = "SPARK"

  initial_capacity {
    initial_capacity_type = "DRIVER"
    initial_capacity_config {
      worker_count = 1
      worker_configuration {
        cpu    = "4 vCPU"
        memory = "16 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "200 vCPU"
    memory = "1000 GB"
    disk   = "1000 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  tags = local.common_tags
}

# ── AWS Glue Catalog ─────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "fintech" {
  name        = "${var.project_name}_${var.env}"
  description = "Fintech pipeline Glue catalog"
}

resource "aws_glue_crawler" "transactions" {
  database_name = aws_glue_catalog_database.fintech.name
  name          = "${var.project_name}-transactions-crawler-${var.env}"
  role          = aws_iam_role.glue.arn

  s3_target {
    path = "s3://${aws_s3_bucket.processed.bucket}/transactions/"
  }

  schedule = "cron(0 * * * ? *)"   # Hourly
  tags     = local.common_tags
}

# ── Lambda for Snowpipe Trigger ───────────────────────────────────────────────

resource "aws_lambda_function" "snowpipe_trigger" {
  function_name = "${var.project_name}-snowpipe-trigger-${var.env}"
  role          = aws_iam_role.lambda.arn
  handler       = "handler.trigger_snowpipe"
  runtime       = "python3.11"
  timeout       = 60
  memory_size   = 256

  environment {
    variables = {
      SNOWFLAKE_ACCOUNT   = var.snowflake_account
      SNOWFLAKE_PIPE_NAME = "FINTECH_DB.RAW.TRANSACTIONS_PIPE"
      ENV                 = var.env
    }
  }

  tags = local.common_tags
}

resource "aws_s3_bucket_notification" "trigger_lambda" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.snowpipe_trigger.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "transactions/"
    filter_suffix       = ".json"
  }
}

# ── Snowflake Objects ────────────────────────────────────────────────────────

resource "snowflake_database" "fintech" {
  name    = "FINTECH_DB"
  comment = "Fintech credit card data warehouse"
}

resource "snowflake_warehouse" "fintech" {
  name           = "FINTECH_WH"
  warehouse_size = "MEDIUM"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Fintech pipeline compute warehouse"
}

resource "snowflake_stage" "raw_transactions" {
  name        = "RAW_TRANSACTIONS_STAGE"
  database    = snowflake_database.fintech.name
  schema      = "RAW"
  url         = "s3://${aws_s3_bucket.raw.bucket}/transactions/"
  credentials = "AWS_KEY_ID='${var.aws_access_key}' AWS_SECRET_KEY='${var.aws_secret_key}'"
  comment     = "External stage for raw transaction JSON from Fiserv"
}

# ── IAM Roles ────────────────────────────────────────────────────────────────

resource "aws_iam_role" "emr_serverless" {
  name               = "${var.project_name}-emr-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.emr_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role" "glue" {
  name               = "${var.project_name}-glue-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
  tags               = local.common_tags
}

resource "aws_iam_role" "lambda" {
  name               = "${var.project_name}-lambda-role-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "emr_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ── Locals ───────────────────────────────────────────────────────────────────

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.env
    ManagedBy   = "Terraform"
    Owner       = "data-engineering"
  }
}
