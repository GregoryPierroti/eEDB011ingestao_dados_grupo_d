variable "bucket_prefix" {
  description = "Prefix for S3 bucket names"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "use_codecommit" {
  description = "Use AWS CodeCommit as source repository"
  type        = bool
  default     = true
}

variable "github_owner" {
  description = "GitHub repository owner"
  type        = string
  default     = ""
}

variable "github_repo" {
  description = "GitHub repository name"
  type        = string
  default     = ""
}

variable "github_token" {
  description = "GitHub personal access token"
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

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}