# Updated modules/sqs/main.tf with bucket-specific policy

# Main SQS Queue for ETL processing
resource "aws_sqs_queue" "etl_queue" {
  name                      = "${var.queue_name_prefix}-queue"
  delay_seconds             = 0
  max_message_size          = 262144
  message_retention_seconds = 1209600  # 14 days
  receive_wait_time_seconds = 10       # Long polling
  visibility_timeout_seconds = 300     # 5 minutes

  # Redrive policy - send failed messages to DLQ after 3 attempts
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.etl_dlq.arn
    maxReceiveCount     = 3
  })

  tags = merge(var.tags, {
    Purpose = "ETL Message Queue"
    Type    = "Main Queue"
  })
}

# Dead Letter Queue for failed messages
resource "aws_sqs_queue" "etl_dlq" {
  name                      = "${var.queue_name_prefix}-dlq"
  message_retention_seconds = 1209600  # 14 days

  tags = merge(var.tags, {
    Purpose = "ETL Dead Letter Queue"
    Type    = "DLQ"
  })
}

# Get current AWS account ID
data "aws_caller_identity" "current" {}

# Queue policy to allow S3 to send messages - BUCKET SPECIFIC VERSION
resource "aws_sqs_queue_policy" "etl_queue_policy" {
  queue_url = aws_sqs_queue.etl_queue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowS3ToSendMessage"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "sqs:SendMessage"
        Resource = aws_sqs_queue.etl_queue.arn
        Condition = {
          StringEquals = {
            "aws:SourceAccount" = data.aws_caller_identity.current.account_id
          }
          ArnLike = {
            "aws:SourceArn" = var.raw_bucket_arn != null ? var.raw_bucket_arn : "arn:aws:s3:::${var.bucket_prefix}*"
          }
        }
      }
    ]
  })
}