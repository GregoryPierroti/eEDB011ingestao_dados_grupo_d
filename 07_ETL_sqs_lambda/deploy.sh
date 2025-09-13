#!/bin/bash
set -e

echo "🚀 Starting ETL Pipeline Deployment"

# Check prerequisites
command -v terraform >/dev/null 2>&1 || { echo "❌ Terraform is required but not installed. Aborting." >&2; exit 1; }
command -v aws >/dev/null 2>&1 || { echo "❌ AWS CLI is required but not installed. Aborting." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "❌ Python 3 is required but not installed. Aborting." >&2; exit 1; }
command -v pip3 >/dev/null 2>&1 || { echo "❌ pip3 is required but not installed. Aborting." >&2; exit 1; }

# Check AWS credentials
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "❌ AWS credentials not configured. Run 'aws configure' first."
    exit 1
fi

echo "✅ Prerequisites check passed"

# Create Lambda deployment package
echo "📦 Creating Lambda deployment package..."

# Clean previous build
rm -rf lambda_package/
rm -f lambda_function.zip

# Create package directory
mkdir -p lambda_package

# Install Python dependencies
echo "Installing Python dependencies..."
if [ -f "lambda_function/requirements.txt" ]; then
    pip3 install -r lambda_function/requirements.txt -t ./lambda_package/ --no-deps --quiet
else
    echo "⚠️  requirements.txt not found, skipping dependency installation"
fi

# Copy Lambda function
if [ -f "lambda_function/lambda_function.py" ]; then
    cp lambda_function/lambda_function.py ./lambda_package/
else
    echo "❌ Lambda function file not found at lambda_function/lambda_function.py"
    exit 1
fi

# Create ZIP package
cd lambda_package
zip -r ../lambda_function.zip . -x "*.pyc" "__pycache__/*" "*.dist-info/*" > /dev/null
cd ..

# Verify ZIP was created
if [ ! -f "lambda_function.zip" ]; then
    echo "❌ Failed to create Lambda deployment package"
    exit 1
fi

echo "✅ Lambda package created successfully ($(du -h lambda_function.zip | cut -f1))"

# Clean up temporary files
rm -rf lambda_package/

# Initialize Terraform
echo "🔧 Initializing Terraform..."
terraform init

# Validate Terraform configuration
echo "🔍 Validating Terraform configuration..."
terraform validate

# Plan deployment
echo "📋 Planning Terraform deployment..."
terraform plan -var-file=terraform.tfvars

# Ask for confirmation
read -p "Do you want to proceed with deployment? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Deployment cancelled by user"
    exit 1
fi

# Apply Terraform
echo "🚀 Deploying infrastructure..."
terraform apply -var-file=terraform.tfvars -auto-approve

# Show outputs
echo ""
echo "📊 Deployment completed successfully!"
echo "📋 Infrastructure outputs:"
terraform output

# Optional: Test deployment
read -p "Do you want to run a deployment test? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧪 Running deployment test..."
    
    # Get bucket name from Terraform output
    RAW_BUCKET=$(terraform output -raw raw_bucket_name 2>/dev/null || echo "")
    
    if [ ! -z "$RAW_BUCKET" ]; then
        # Create test file
        cat > test_deployment.csv << EOF
id,name,amount,date,category
1,Test Transaction,150.50,2024-01-15,Sales
2,Test Purchase,75.25,2024-01-16,Marketing
3,Test Sale,300.00,2024-01-17,Sales
EOF
        
        # Upload test file
        echo "📤 Uploading test file..."
        aws s3 cp test_deployment.csv s3://$RAW_BUCKET/deployment_test_$(date +%Y%m%d_%H%M%S).csv
        
        echo "✅ Test file uploaded successfully"
        echo "⏳ Wait a few moments and check the Trusted and Delivery buckets for processed data"
        
        # Clean up test file
        rm -f test_deployment.csv
    else
        echo "⚠️  Could not retrieve bucket names for testing"
    fi
fi

echo ""
echo "🎉 ETL Pipeline deployment completed successfully!"
echo "📖 Check the README.md for usage instructions and monitoring guidance"