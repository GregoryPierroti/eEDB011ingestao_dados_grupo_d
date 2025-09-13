# CodePipeline Module for CI/CD (optional)
module "codepipeline" {
  source = "./modules/codepipeline"
  count  = var.enable_codepipeline ? 1 : 0
  
  bucket_prefix = var.bucket_prefix
  aws_region   = var.aws_region
  
  # CodePipeline specific variables
  use_codecommit          = var.use_codecommit
  github_owner            = var.github_owner
  github_repo             = var.github_repo
  github_token            = var.github_token
  source_branch           = var.source_branch
  require_manual_approval = var.require_manual_approval
  run_deployment_test     = var.run_deployment_test
  
  tags = var.common_tags
}