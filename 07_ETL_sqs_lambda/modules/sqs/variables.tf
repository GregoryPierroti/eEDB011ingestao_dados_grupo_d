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