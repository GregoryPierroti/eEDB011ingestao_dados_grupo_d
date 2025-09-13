#!/bin/bash

# ETL Pipeline Deployment Script
# Pipeline Cloud Streaming: AWS Lambda + S3 + SQS

set -e

echo "🚀 Starting ETL Pipeline Deployment..."

# Configuration
LAMBDA_FUNCTION_NAME="etl-processor"
REGION="us-east-1"
DEPLOYMENT_PACKAGE="lambda_function.zip"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed. Please install Terraform first."
        exit 1
    fi
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install AWS CLI first."
        exit 1
    fi
    
    # Check if AWS credentials are configured
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials are not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    # Check if Python is installed
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 is not installed. Please install Python3 first."
        exit 1
    fi
    
    # Check if pip is installed
    if ! command -v pip3 &> /dev/null; then
        log_error "pip3 is not installed. Please install pip3 first."
        exit 1
    fi
    
    log_success "All prerequisites are met!"
}

# Create Lambda deployment package
create_lambda_package() {
    log_info "Creating Lambda deployment package..."
    
    # Create temporary directory
    TEMP_DIR=$(mktemp -d)
    log_info "Using temporary directory: $TEMP_DIR"
    
    # Copy Lambda function code
    cp lambda_function.py "$TEMP_DIR/"
    cp requirements.txt "$TEMP_DIR/"
    
    # Install dependencies
    cd "$TEMP_DIR"
    pip3 install -r requirements.txt -t .
    
    # Create ZIP package
    zip -r "$DEPLOYMENT_PACKAGE" . -x "*.pyc" "__pycache__/*"
    
    # Move package to project directory
    mv "$DEPLOYMENT_PACKAGE" "$OLDPWD/"
    
    # Cleanup
    cd "$OLDPWD"
    rm -rf "$TEMP_DIR"
    
    log_success "Lambda deployment package created: $DEPLOYMENT_PACKAGE"
}

# Deploy infrastructure with Terraform
deploy_infrastructure() {
    log_info "Deploying infrastructure with Terraform..."
    
    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init
    
    # Validate Terraform configuration
    log_info "Validating Terraform configuration..."
    terraform validate
    
    # Plan deployment
    log_info "Planning Terraform deployment..."
    terraform plan -out=tfplan
    
    # Apply deployment
    log_info "Applying Terraform deployment..."
    terraform apply tfplan
    
    # Remove plan file
    rm -f tfplan
    
    log_success "Infrastructure deployed successfully!"
}

# Show deployment outputs
show_outputs() {
    log_info "Deployment completed! Here are your resources:"
    
    echo -e "\n${GREEN}=== PIPELINE RESOURCES ===${NC}"
    terraform output -json | python3 -c "
import json
import sys

data = json.load(sys.stdin)

print('📦 S3 Buckets:')
for bucket_type in ['raw_bucket_name', 'trusted_bucket_name', 'delivery_bucket_name']:
    if bucket_type in data:
        print(f'   • {bucket_type.replace(\"_\", \" \").title()}: {data[bucket_type][\"value\"]}')

print('\n📨 SQS Queue:')
if 'sqs_queue_url' in data:
    print(f'   • Queue URL: {data[\"sqs_queue_url\"][\"value\"]}')

print('\n⚡ Lambda Function:')
if 'lambda_function_name' in data:
    print(f'   • Function Name: {data[\"lambda_function_name\"][\"value\"]}')

print('\n🔗 Pipeline Flow:')
print('   RAW → TRUSTED → DELIVERY')
print('   Files uploaded to RAW bucket trigger SQS → Lambda → ETL processing')
"
}

# Test pipeline
test_pipeline() {
    log_info "Testing pipeline with sample data..."
    
    # Get bucket name from Terraform output
    RAW_BUCKET=$(terraform output -raw raw_bucket_name 2>/dev/null || echo "")
    
    if [ -z "$RAW_BUCKET" ]; then
        log_warning "Could not retrieve RAW bucket name. Skipping test."
        return
    fi
    
    # Create sample CSV file
    SAMPLE_FILE="sample_data_$(date +%Y%m%d_%H%M%S).csv"
    cat > "$SAMPLE_FILE" << EOF
id,name,amount,date,category
1,Transaction A,150.50,2024-01-15,Sales
2,Transaction B,75.25,2024-01-16,Marketing
3,Transaction C,300.00,2024-01-17,Sales
4,Transaction D,45.75,2024-01-18,Operations
5,Transaction E,220.30,2024-01-19,Sales
EOF
    
    log_info "Uploading sample file to RAW bucket: $SAMPLE_FILE"
    aws s3 cp "$SAMPLE_FILE" "s3://$RAW_BUCKET/"
    
    log_success "Sample file uploaded! Check CloudWatch logs for processing status."
    
    # Cleanup sample file
    rm -f "$SAMPLE_FILE"
    
    echo -e "\n${YELLOW}📊 Monitor Processing:${NC}"
    echo "   • CloudWatch Logs: aws logs tail /aws/lambda/$LAMBDA_FUNCTION_NAME --follow"
    echo "   • S3 Console: https://console.aws.amazon.com/s3/"
    echo "   • SQS Console: https://console.aws.amazon.com/sqs/"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up temporary files..."
    rm -f lambda_function.zip
    rm -f tfplan
    log_success "Cleanup completed!"
}

# Main deployment process
main() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                 ETL PIPELINE DEPLOYMENT                   ║"
    echo "║              RAW → TRUSTED → DELIVERY                     ║"
    echo "║            AWS Lambda + S3 + SQS Pipeline                 ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}\n"
    
    # Run deployment steps
    check_prerequisites
    create_lambda_package
    deploy_infrastructure
    show_outputs
    
    # Ask if user wants to test
    echo -e "\n${YELLOW}Do you want to test the pipeline with sample data? (y/n):${NC}"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        test_pipeline
    fi
    
    echo -e "\n${GREEN}🎉 ETL Pipeline deployment completed successfully!${NC}"
    echo -e "${BLUE}📚 Documentation:${NC}"
    echo "   • Upload files to RAW bucket to trigger processing"
    echo "   • Processed data will appear in TRUSTED and DELIVERY buckets"
    echo "   • Monitor Lambda logs in CloudWatch for processing status"
    echo "   • Data is partitioned by date for efficient querying"
    
    # Ask about cleanup
    echo -e "\n${YELLOW}Do you want to clean up temporary deployment files? (y/n):${NC}"
    read -r cleanup_response
    if [[ "$cleanup_response" =~ ^[Yy]$ ]]; then
        cleanup
    fi
}

# Handle script interruption
trap cleanup EXIT

# Run main function
main "$@"