#!/bin/bash

table_name="export_test"

# Define the attribute definitions and key schema
attribute_definitions='[{"AttributeName":"id","AttributeType":"S"}]'
key_schema='[{"AttributeName":"id","KeyType":"HASH"}]'

# Check if the table exists
if aws dynamodb describe-table --table-name "$table_name" 2>/dev/null; then
    echo "Table $table_name exists. Deleting..."
    aws dynamodb delete-table --table-name "$table_name"
    aws dynamodb wait table-not-exists --table-name "$table_name"
fi

# Create the table with dynamic provisioning
aws dynamodb create-table \
    --table-name "$table_name" \
    --attribute-definitions "$attribute_definitions" \
    --key-schema "$key_schema" \
    --billing-mode PAY_PER_REQUEST

# Wait for the table to be active
aws dynamodb wait table-exists --table-name "$table_name"
echo "Table $table_name created with on-demand provisioning."
