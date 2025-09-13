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

# Additional variables for CodePipeline deployment
# Add these to your existing variables.tf file

variable "use_codecommit" {
  description = "Use AWS CodeCommit as source repository (false for GitHub)"
  type        = bool
  default     = false
}

variable "github_owner" {
  description = "GitHub repository owner (required if use_codecommit = false)"
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repository name (required if use_codecommit = false)"
  type        = string
  default     = ""
}

variable "github_token" {
  description = "GitHub personal access token (required if use_codecommit = false)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "source_branch" {
  description = "Source branch for the pipeline"
  type        = string
  default     = "main"
}

variable "require_manual_approval" {
  description = "Require manual approval before deployment"
  type        = bool
  default     = true
}

variable "run_deployment_test" {
  description = "Run deployment test after successful deployment"
  type        = bool
  default     = true
}

variable "enable_codepipeline" {
  description = "Enable CodePipeline for CI/CD deployment"
  type        = bool
  default     = false
}