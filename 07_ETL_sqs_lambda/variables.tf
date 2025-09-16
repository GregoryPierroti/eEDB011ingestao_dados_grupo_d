variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "bucket_prefix" {
  description = "Prefix for S3 bucket names"
  type        = string
  default     = "etl-pipeline"
}

variable "queue_name_prefix" {
  description = "Prefix for SQS queue names"
  type        = string
  default     = "etl-processing"
}

variable "lambda_function_name" {
  description = "Name of the ETL Lambda function"
  type        = string
  default     = "etl-processor"
}

variable "lambda_handler" {
  description = "Lambda function handler"
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "lambda_runtime" {
  description = "Lambda runtime version"
  type        = string
  default     = "python3.11"
}

variable "lambda_zip_path" {
  description = "Path to Lambda deployment package"
  type        = string
  default     = "./lambda_function.zip"
}

variable "lambda_batch_size" {
  description = "Number of SQS messages to process in a single Lambda invocation"
  type        = number
  default     = 10
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "ETL-Pipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}