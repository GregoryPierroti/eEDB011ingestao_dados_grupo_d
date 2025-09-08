output "lambda_function_name" {
  description = "The name of the Lambda function"
  value       = aws_lambda_function.etl_lambda.function_name
}

output "lambda_function_arn" {
  description = "The ARN of the Lambda function"
  value       = aws_lambda_function.etl_lambda.arn
}