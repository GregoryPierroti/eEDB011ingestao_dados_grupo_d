output "raw_bucket_name" {
  description = "Name of the RAW data S3 bucket"
  value       = module.s3.raw_bucket_name
}

output "trusted_bucket_name" {
  description = "Name of the Trusted data S3 bucket"
  value       = module.s3.trusted_bucket_name
}

output "delivery_bucket_name" {
  description = "Name of the Delivery data S3 bucket"
  value       = module.s3.delivery_bucket_name
}

output "sqs_queue_url" {
  description = "URL of the SQS queue"
  value       = module.sqs.queue_url
}

output "lambda_function_name" {
  description = "Name of the ETL Lambda function"
  value       = module.lambda.lambda_function_name
}

output "lambda_function_arn" {
  description = "ARN of the ETL Lambda function"
  value       = module.lambda.lambda_function_arn
}

output "pipeline_endpoints" {
  description = "Important endpoints for the ETL pipeline"
  value = {
    raw_bucket_upload_url    = "s3://${module.s3.raw_bucket_name}/"
    trusted_bucket_url       = "s3://${module.s3.trusted_bucket_name}/"
    delivery_bucket_url      = "s3://${module.s3.delivery_bucket_name}/"
    sqs_queue_url           = module.sqs.queue_url
    lambda_function_name    = module.lambda.lambda_function_name
  }
}