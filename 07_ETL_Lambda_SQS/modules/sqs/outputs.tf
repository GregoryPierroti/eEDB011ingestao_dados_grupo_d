output "queue_url" {
  description = "URL of the main SQS queue"
  value       = aws_sqs_queue.etl_queue.id
}

output "queue_arn" {
  description = "ARN of the main SQS queue"
  value       = aws_sqs_queue.etl_queue.arn
}

output "dlq_url" {
  description = "URL of the dead letter queue"
  value       = aws_sqs_queue.etl_dlq.id
}

output "dlq_arn" {
  description = "ARN of the dead letter queue"
  value       = aws_sqs_queue.etl_dlq.arn
}

output "queue_policy" {
  description = "SQS queue policy resource for dependency management"
  value       = aws_sqs_queue_policy.etl_queue_policy
}

output "queue_name" {
  description = "Name of the main SQS queue"
  value       = aws_sqs_queue.etl_queue.name
}