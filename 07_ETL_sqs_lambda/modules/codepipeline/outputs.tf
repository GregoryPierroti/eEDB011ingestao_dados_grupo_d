output "pipeline_name" {
  description = "Name of the CodePipeline"
  value       = aws_codepipeline.etl_pipeline.name
}

output "codecommit_clone_url_http" {
  description = "CodeCommit repository HTTP clone URL"
  value       = var.use_codecommit ? aws_codecommit_repository.etl_pipeline[0].clone_url_http : null
}

output "terraform_state_bucket" {
  description = "S3 bucket for Terraform state"
  value       = aws_s3_bucket.terraform_state.bucket
}

output "codebuild_project_name" {
  description = "Name of the CodeBuild project"
  value       = aws_codebuild_project.etl_pipeline.name
}