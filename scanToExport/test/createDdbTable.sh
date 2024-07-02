#!/bin/bash

table_name="export_test"

# Define the attribute definitions and key schema
attribute_definitions='[{"AttributeName":"id","AttributeType":"S"}]'
key_schema='[{"AttributeName":"id","KeyType":"HASH"}]'

# Function to create table and enable PITR and Streams
create_and_configure_table() {
    # Create the table
    aws dynamodb create-table \
        --table-name "$table_name" \
        --attribute-definitions "$attribute_definitions" \
        --key-schema "$key_schema" \
        --billing-mode PAY_PER_REQUEST \
        --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES

    # Wait for the table to be active
    aws dynamodb wait table-exists --table-name "$table_name"
    echo "Table $table_name created with on-demand provisioning and DDB Streams enabled."

    # Enable Point-in-Time Recovery
    aws dynamodb update-continuous-backups \
        --table-name "$table_name" \
        --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
    echo "Point-in-Time Recovery enabled for $table_name."
}

# Check if the table exists
if aws dynamodb describe-table --table-name "$table_name" 2>/dev/null; then
    echo "Table $table_name already exists."
    read -p "Do you want to recreate it? (y/n): " choice
    case "$choice" in 
        y|Y )
            echo "Deleting existing table..."
            aws dynamodb delete-table --table-name "$table_name"
            aws dynamodb wait table-not-exists --table-name "$table_name"
            echo "Creating new table..."
            create_and_configure_table
            ;;
        n|N )
            echo "Keeping existing table. Ensuring PITR and Streams are enabled..."
            aws dynamodb update-continuous-backups \
                --table-name "$table_name" \
                --point-in-time-recovery-specification PointInTimeRecoveryEnabled=true
            aws dynamodb update-table \
                --table-name "$table_name" \
                --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
            echo "Table $table_name updated with PITR and Streams enabled."
            ;;
        * )
            echo "Invalid input. No changes made."
            exit 1
            ;;
    esac
else
    echo "Table $table_name does not exist. Creating new table..."
    create_and_configure_table
fi