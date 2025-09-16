terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Module for Data Lakes (RAW, Trusted, Delivery)
module "s3" {
  source = "./modules/s3"
  
  bucket_prefix = var.bucket_prefix
  aws_region   = var.aws_region
  
  tags = var.common_tags
}

# SQS Module for Message Queuing
module "sqs" {
  source = "./modules/sqs"
  
  queue_name_prefix = var.queue_name_prefix
  raw_bucket_arn    = module.s3.raw_bucket_arn  # Add this line
  bucket_prefix     = var.bucket_prefix          # Add this line
  
  tags = var.common_tags
}

# Lambda Module for ETL Processing
module "lambda" {
  source = "./modules/lambda"
  
  aws_region            = var.aws_region
  lambda_function_name  = var.lambda_function_name
  lambda_handler        = var.lambda_handler
  lambda_runtime        = var.lambda_runtime
  lambda_zip_path       = var.lambda_zip_path
  
  # S3 Buckets ARNs for permissions
  raw_bucket_arn      = module.s3.raw_bucket_arn
  trusted_bucket_arn  = module.s3.trusted_bucket_arn
  delivery_bucket_arn = module.s3.delivery_bucket_arn
  
  # SQS Queue ARN for permissions
  sqs_queue_arn = module.sqs.queue_arn
  
  lambda_environment = {
    RAW_BUCKET      = module.s3.raw_bucket_name
    TRUSTED_BUCKET  = module.s3.trusted_bucket_name
    DELIVERY_BUCKET = module.s3.delivery_bucket_name
    SQS_QUEUE_URL   = module.sqs.queue_url
  }
  
  tags = var.common_tags
}

# Fixed S3 Bucket Notification with proper dependencies
resource "aws_s3_bucket_notification" "raw_bucket_notification" {
  bucket = module.s3.raw_bucket_name

  queue {
    queue_arn = module.sqs.queue_arn
    events    = ["s3:ObjectCreated:*"]
  }

  # Critical: Ensure SQS policy is created before notification
  depends_on = [
    module.sqs.queue_policy
  ]
}

# Event source mapping for SQS to Lambda
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = module.sqs.queue_arn
  function_name    = module.lambda.lambda_function_arn
  batch_size       = var.lambda_batch_size
}