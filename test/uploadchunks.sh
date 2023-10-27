# Using AWS CLI to batch-write each chunk in the 'chunks' subdirectory to DynamoDB
for file in chunks/chunk_*.json; do
    aws dynamodb batch-write-item --request-items file://$file
done
