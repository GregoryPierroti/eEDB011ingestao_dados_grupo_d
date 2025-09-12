output "raw_bucket_name" {
  description = "Name of the RAW data bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "raw_bucket_arn" {
  description = "ARN of the RAW data bucket"
  value       = aws_s3_bucket.raw.arn
}

output "trusted_bucket_name" {
  description = "Name of the Trusted data bucket"
  value       = aws_s3_bucket.trusted.bucket
}

output "trusted_bucket_arn" {
  description = "ARN of the Trusted data bucket"
  value       = aws_s3_bucket.trusted.arn
}

output "delivery_bucket_name" {
  description = "Name of the Delivery data bucket"
  value       = aws_s3_bucket.delivery.bucket
}

output "delivery_bucket_arn" {
  description = "ARN of the Delivery data bucket"
  value       = aws_s3_bucket.delivery.arn
}

output "bucket_names" {
  description = "All bucket names for easy reference"
  value = {
    raw      = aws_s3_bucket.raw.bucket
    trusted  = aws_s3_bucket.trusted.bucket
    delivery = aws_s3_bucket.delivery.bucket
  }
}