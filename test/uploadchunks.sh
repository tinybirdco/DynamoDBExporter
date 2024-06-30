#!/bin/bash

# Directory containing the chunk files
chunk_dir="chunks"

# Counter for processed files
processed=0
successful=0

# Total number of files
total=$(ls ${chunk_dir}/chunk_*.json 2>/dev/null | wc -l)

if [ $total -eq 0 ]; then
    echo "No chunk files found in $chunk_dir. Exiting."
    exit 0
fi

echo "Starting upload of $total files to DynamoDB..."

# Using AWS CLI to batch-write each chunk in the 'chunks' subdirectory to DynamoDB
for file in ${chunk_dir}/chunk_*.json; do
    ((processed++))
    echo "Processing file $processed of $total: $file"
    
    output=$(aws dynamodb batch-write-item --request-items file://$file --region eu-west-2 2>&1)
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo "Successfully uploaded $file"
        rm "$file"
        ((successful++))
    else
        echo "Error uploading $file. Error message:"
        echo "$output"
    fi
    
    echo "Progress: $processed/$total files processed"
    echo "----------------------------------------"
done

echo "Upload process completed. Total files processed: $processed"
echo "Successfully uploaded and removed: $successful"
echo "Failed uploads (files remaining): $((processed - successful))"

# Check if all files were processed successfully
if [ $successful -eq $total ]; then
    echo "All files were successfully uploaded and removed."
else
    echo "Some files failed to upload. Please check the remaining files in $chunk_dir"
fi