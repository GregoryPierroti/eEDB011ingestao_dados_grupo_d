variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "lambda_function_name" {
  description = "Lambda function name"
  type        = string
  default     = "etl_lambda_function"
}

variable "lambda_handler" {
  description = "Lambda handler"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.11"
}

variable "lambda_zip_path" {
  description = "Path to Lambda deployment package"
  type        = string
  default     = "./lambda_function.zip"
}

variable "lambda_environment" {
  description = "Environment variables for Lambda"
  type        = map(string)
  default     = {}
}

variable "raw_bucket_arn" {
  description = "ARN of the RAW S3 bucket"
  type        = string
}

variable "trusted_bucket_arn" {
  description = "ARN of the Trusted S3 bucket"
  type        = string
}

variable "delivery_bucket_arn" {
  description = "ARN of the Delivery S3 bucket"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS queue"
  type        = string
}

variable "tags" {
  description = "Tags to apply to Lambda resources"
  type        = map(string)
  default     = {}
}