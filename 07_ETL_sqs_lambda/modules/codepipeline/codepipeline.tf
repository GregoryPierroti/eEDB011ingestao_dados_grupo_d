# codepipeline.tf
# Creates the CI/CD pipeline infrastructure for ETL deployment

# S3 bucket for CodePipeline artifacts
resource "aws_s3_bucket" "codepipeline_artifacts" {
  bucket = "${var.bucket_prefix}-codepipeline-artifacts-${random_id.pipeline_suffix.hex}"
  
  tags = merge(var.common_tags, {
    Purpose = "CodePipeline Artifacts"
  })
}

resource "aws_s3_bucket_versioning" "codepipeline_artifacts" {
  bucket = aws_s3_bucket.codepipeline_artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "codepipeline_artifacts" {
  bucket = aws_s3_bucket.codepipeline_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "codepipeline_artifacts" {
  bucket = aws_s3_bucket.codepipeline_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket for Terraform state (remote backend)
resource "aws_s3_bucket" "terraform_state" {
  bucket = "${var.bucket_prefix}-terraform-state-${random_id.pipeline_suffix.hex}"
  
  tags = merge(var.common_tags, {
    Purpose = "Terraform State"
  })
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# DynamoDB table for Terraform state locking
resource "aws_dynamodb_table" "terraform_state_lock" {
  name           = "${var.bucket_prefix}-terraform-state-lock"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = merge(var.common_tags, {
    Purpose = "Terraform State Lock"
  })
}

# CodeCommit repository (optional - if not using external Git)
resource "aws_codecommit_repository" "etl_pipeline" {
  count           = var.use_codecommit ? 1 : 0
  repository_name = "${var.bucket_prefix}-etl-pipeline"
  description     = "ETL Pipeline Infrastructure Code"

  tags = var.common_tags
}

# IAM role for CodeBuild
resource "aws_iam_role" "codebuild_role" {
  name = "${var.bucket_prefix}-codebuild-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codebuild.amazonaws.com"
        }
      }
    ]
  })

  tags = var.common_tags
}

# IAM policy for CodeBuild
resource "aws_iam_role_policy" "codebuild_policy" {
  role = aws_iam_role.codebuild_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.codepipeline_artifacts.arn,
          "${aws_s3_bucket.codepipeline_artifacts.arn}/*",
          aws_s3_bucket.terraform_state.arn,
          "${aws_s3_bucket.terraform_state.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:DeleteItem"
        ]
        Resource = aws_dynamodb_table.terraform_state_lock.arn
      },
      # Permissions needed for ETL pipeline deployment
      {
        Effect = "Allow"
        Action = [
          "s3:*",
          "lambda:*",
          "sqs:*",
          "iam:*",
          "logs:*"
        ]
        Resource = "*"
      }
    ]
  })
}

# CodeBuild project
resource "aws_codebuild_project" "etl_pipeline" {
  name          = "${var.bucket_prefix}-etl-pipeline-build"
  description   = "Build project for ETL Pipeline deployment"
  service_role  = aws_iam_role.codebuild_role.arn

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type                = "BUILD_GENERAL1_MEDIUM"
    image                      = "aws/codebuild/standard:7.0"
    type                       = "LINUX_CONTAINER"
    image_pull_credentials_type = "CODEBUILD"

    environment_variable {
      name  = "TF_STATE_BUCKET"
      value = aws_s3_bucket.terraform_state.bucket
    }

    environment_variable {
      name  = "TF_STATE_KEY"
      value = "etl-pipeline/terraform.tfstate"
    }

    environment_variable {
      name  = "TF_STATE_DYNAMODB_TABLE"
      value = aws_dynamodb_table.terraform_state_lock.name
    }

    environment_variable {
      name  = "RUN_DEPLOYMENT_TEST"
      value = var.run_deployment_test ? "true" : "false"
    }
  }

  source {
    type = "CODEPIPELINE"
    buildspec = "buildspec.yml"
  }

  tags = var.common_tags
}

# IAM role for CodePipeline
resource "aws_iam_role" "codepipeline_role" {
  name = "${var.bucket_prefix}-codepipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "codepipeline.amazonaws.com"
        }
      }
    ]
  })

  tags = var.common_tags
}

# IAM policy for CodePipeline
resource "aws_iam_role_policy" "codepipeline_policy" {
  role = aws_iam_role.codepipeline_role.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetBucketVersioning"
        ]
        Resource = [
          aws_s3_bucket.codepipeline_artifacts.arn,
          "${aws_s3_bucket.codepipeline_artifacts.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "codebuild:BatchGetBuilds",
          "codebuild:StartBuild"
        ]
        Resource = aws_codebuild_project.etl_pipeline.arn
      },
      # Add permissions for source (CodeCommit or GitHub)
      {
        Effect = "Allow"
        Action = [
          "codecommit:GetBranch",
          "codecommit:GetCommit",
          "codecommit:GetRepository",
          "codecommit:ListBranches",
          "codecommit:ListRepositories"
        ]
        Resource = var.use_codecommit ? aws_codecommit_repository.etl_pipeline[0].arn : "*"
      }
    ]
  })
}

# CodePipeline
resource "aws_codepipeline" "etl_pipeline" {
  name     = "${var.bucket_prefix}-etl-pipeline"
  role_arn = aws_iam_role.codepipeline_role.arn

  artifact_store {
    location = aws_s3_bucket.codepipeline_artifacts.bucket
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = var.use_codecommit ? "AWS" : "ThirdParty"
      provider         = var.use_codecommit ? "CodeCommit" : "GitHub"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = var.use_codecommit ? {
        RepositoryName = aws_codecommit_repository.etl_pipeline[0].repository_name
        BranchName     = var.source_branch
      } : {
        Owner  = var.github_owner
        Repo   = var.github_repo
        Branch = var.source_branch
        OAuthToken = var.github_token
      }
    }
  }

  stage {
    name = "Plan"

    action {
      name             = "Plan"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      input_artifacts  = ["source_output"]
      output_artifacts = ["plan_output"]
      version          = "1"

      configuration = {
        ProjectName = aws_codebuild_project.etl_pipeline.name
      }
    }
  }

  # Optional: Manual approval stage for production
  dynamic "stage" {
    for_each = var.require_manual_approval ? [1] : []
    content {
      name = "Approval"

      action {
        name     = "ManualApproval"
        category = "Approval"
        owner    = "AWS"
        provider = "Manual"
        version  = "1"

        configuration = {
          CustomData = "Please review the Terraform plan and approve deployment"
        }
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "Deploy"
      category        = "Build" 
      owner           = "AWS"
      provider        = "CodeBuild"
      input_artifacts = ["source_output"]
      version         = "1"

      configuration = {
        ProjectName = aws_codebuild_project.etl_pipeline.name
      }
    }
  }

  tags = var.common_tags
}

# Random suffix for unique resource names
resource "random_id" "pipeline_suffix" {
  byte_length = 4
}