variable "queue_name_prefix" {
  description = "Prefix for SQS queue names"
  type        = string
  default     = "etl-processing"
}

variable "tags" {
  description = "Tags to apply to SQS resources"
  type        = map(string)
  default     = {}
}

variable "raw_bucket_arn" {
  description = "ARN of the RAW S3 bucket that will send messages"
  type        = string
  default     = null
}

variable "bucket_prefix" {
  description = "Prefix for S3 bucket names (fallback for ARN)"
  type        = string
  default     = "etl-pipeline"
}