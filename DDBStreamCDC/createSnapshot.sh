#!/bin/bash

# https://docs.aws.amazon.com/cli/latest/reference/dynamodb/export-table-to-point-in-time.html

# Function to check AWS credentials
check_aws_credentials() {
    if ! aws sts get-caller-identity &>/dev/null; then
        echo "Error: Unable to locate valid AWS credentials. Please run 'aws configure' to set up your AWS CLI."
        exit 1
    fi
}

# Check AWS credentials
check_aws_credentials

# Default values
DEFAULT_DYNAMODB_TABLE_NAME="export_test"
DEFAULT_S3_BUCKET="tinybird-test-dynamodb-export"
DEFAULT_REGION="eu-west-2"
DEFAULT_EXPORT_FORMAT="DYNAMODB_JSON"

# Use environment variables if set, otherwise use defaults
DYNAMODB_TABLE_NAME=${DYNAMODB_TABLE_NAME:-$DEFAULT_DYNAMODB_TABLE_NAME}
S3_BUCKET=${S3_BUCKET:-$DEFAULT_S3_BUCKET}
AWS_REGION=${AWS_REGION:-$DEFAULT_REGION}
EXPORT_FORMAT=${EXPORT_FORMAT:-$DEFAULT_EXPORT_FORMAT}
S3_PREFIX=${S3_PREFIX:-"DDBStreamCDC/${DYNAMODB_TABLE_NAME}"}
EXPORT_TIME=${EXPORT_TIME:-""}
S3_SSE_ALGORITHM=${S3_SSE_ALGORITHM:-""}
S3_SSE_KMS_KEY_ID=${S3_SSE_KMS_KEY_ID:-""}

# Construct the DynamoDB table ARN
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DYNAMODB_TABLE_ARN="arn:aws:dynamodb:${AWS_REGION}:${ACCOUNT_ID}:table/${DYNAMODB_TABLE_NAME}"

# Print the values being used
echo "Using the following values:"
echo "DYNAMODB_TABLE_NAME: $DYNAMODB_TABLE_NAME"
echo "DYNAMODB_TABLE_ARN: $DYNAMODB_TABLE_ARN"
echo "S3_BUCKET: $S3_BUCKET"
echo "S3_PREFIX: $S3_PREFIX"
echo "AWS_REGION: $AWS_REGION"
echo "EXPORT_FORMAT: $EXPORT_FORMAT"

# Construct the base command
CMD="aws dynamodb export-table-to-point-in-time \
    --table-arn \"$DYNAMODB_TABLE_ARN\" \
    --s3-bucket \"$S3_BUCKET\" \
    --s3-prefix \"$S3_PREFIX\" \
    --export-format \"$EXPORT_FORMAT\" \
    --region \"$AWS_REGION\""

# Add optional parameters if they are set
if [ -n "$EXPORT_TIME" ]; then
    CMD="$CMD --export-time \"$EXPORT_TIME\""
fi

if [ -n "$S3_SSE_ALGORITHM" ]; then
    CMD="$CMD --s3-sse-algorithm \"$S3_SSE_ALGORITHM\""
fi

if [ -n "$S3_SSE_KMS_KEY_ID" ]; then
    CMD="$CMD --s3-sse-kms-key-id \"$S3_SSE_KMS_KEY_ID\""
fi

# Execute the command
echo "Executing command: $CMD"
if eval $CMD; then
    echo "Export initiated successfully"
    echo "Export will be available in s3://$S3_BUCKET/$S3_PREFIX"
else
    echo "Failed to initiate export. Please check your AWS CLI configuration and permissions."
    exit 1
fi