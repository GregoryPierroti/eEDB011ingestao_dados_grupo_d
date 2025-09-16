# terraform.tfvars - CORRECTED VERSION
aws_region = "us-east-1"
bucket_prefix = "my-etl-pipeline"
queue_name_prefix = "etl-processing"
lambda_function_name = "etl-processor"
lambda_handler = "lambda_function.lambda_handler"
lambda_runtime = "python3.11"
lambda_zip_path = "./lambda_function.zip"
lambda_batch_size = 10

# NEW: CodePipeline variables
enable_codepipeline = false
use_codecommit = true
codecommit_repository_name = "etl-atividade7-pipeline-repo"
source_branch = "master"
require_manual_approval = true
run_deployment_test = true

# Tags
common_tags = {
  Project     = "ETL-Streaming-Pipeline"
  Environment = "production"
  Team        = "DataEngineering"
  ManagedBy   = "Terraform"
  CostCenter  = "Analytics"
}