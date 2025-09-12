variable "bucket_prefix" {
  description = "Prefix for S3 bucket names"
  type        = string
  default     = "etl-pipeline"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Tags to apply to S3 resources"
  type        = map(string)
  default     = {}
}