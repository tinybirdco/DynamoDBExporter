import boto3
import json
import os
import urllib3
from urllib.parse import urlencode
from io import BytesIO

# Fetch environment variables
TINYBIRD_API_KEY = os.environ.get('TB_DS_ADMIN_TOKEN', '')
TINYBIRD_API_ENDPOINT = os.environ.get('TB_API_ENDPOINT', 'https://api.tinybird.co/v0/datasources')
DDB_TABLES_TO_EXPORT = os.environ.get('DDB_TABLES_TO_EXPORT', '')
DDB_REGION = os.environ.get('DDB_REGION', '')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
DOWNLOAD_URL_EXPIRATION = os.environ.get('DOWNLOAD_URL_EXPIRATION', 1800)

# Validate mandatory environment variables
if not TINYBIRD_API_KEY or not DDB_TABLES_TO_EXPORT or not S3_BUCKET_NAME:
    raise Exception('Missing mandatory environment variables. Please check the README for instructions.')
else:
    DDB_TABLES_LIST = [x.strip() for x in DDB_TABLES_TO_EXPORT.split(',')]  # Convert comma-separated string to list

# Handle defaults
s3_client = boto3.client('s3')
ddb_client = boto3.client('dynamodb', region_name=DDB_REGION) if DDB_REGION else boto3.client('dynamodb')
http = urllib3.PoolManager()

def generate_presigned_url(bucket_name, object_key, expiration=1800):
    """Generate a presigned URL for the S3 object"""
    presigned_url = s3_client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket_name, 'Key': object_key},
        ExpiresIn=expiration
    )
    return presigned_url

def abort_multipart_upload(bucket, key, upload_id):
    s3_client.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

def initiate_multipart_upload(bucket, key):
    response = s3_client.create_multipart_upload(Bucket=bucket, Key=key)
    return response['UploadId']

def upload_part(bucket, key, upload_id, part_number, body):
    response = s3_client.upload_part(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        PartNumber=part_number,
        Body=body
    )
    return response['ETag']

def complete_multipart_upload(bucket, key, upload_id, parts):
    s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={
            'Parts': parts
        }
    )

def replace_tinybird_datasource(tinybird_table_name, output_bucket, output_key):
    # Prepare the request headers
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',  # Adjust content type if needed
    }

    # Prepare the Tinybird API request payload
    tinybird_params = {
        'name': tinybird_table_name,
        'mode': 'replace',
        'format': 'ndjson',
        'url': generate_presigned_url(output_bucket, output_key, expiration=DOWNLOAD_URL_EXPIRATION),
    }

    # Make the HTTP POST request to Tinybird using urllib3
    http = urllib3.PoolManager()
    encoded_params = urlencode(tinybird_params)
    response = http.request(
        'POST',
        TINYBIRD_API_ENDPOINT + '?' + encoded_params,
        headers=headers,
    )

    # Check the HTTP status code and raise an exception if it indicates an error
    if response.status >= 400:
        error_message = response.data.decode('utf-8')  # Get the error message from the response
        
        # Check if the error message indicates that the data source does not exist
        if "can not replace a non existing Data Source" in error_message:
            print(f"Data source '{tinybird_table_name}' does not exist. Please download the NDJSON from the bucket and create a new data source in Tinybird using the UI.")
        else:
            raise Exception(f"HTTP request to Tinybird failed with status code {response.status}: {error_message}")
    else:
        # Log the response from Tinybird (for debugging)
        print(response.status)
        print(response.data.decode('utf-8'))

def parse_dynamodb_attribute(attr):
    """Recursively parse DynamoDB attribute to a Python object."""
    if isinstance(attr, dict):
        # Handle null type
        if 'NULL' in attr and attr['NULL'] is True:
            return ''  # Clickhouse prefers empty strings over NULL values
        # String type
        elif 'S' in attr:
            return attr['S']
        # Number type
        elif 'N' in attr:
            try:
                return int(attr['N'])
            except ValueError:
                return float(attr['N'])
        # Boolean type
        elif 'BOOL' in attr:
            return attr['BOOL']
        # List type
        elif 'L' in attr:
            return [parse_dynamodb_attribute(item) for item in attr['L']]
        # Map type (nested dictionary)
        elif 'M' in attr:
            return {k: parse_dynamodb_attribute(v) for k, v in attr['M'].items()}
        else:
            # Fallback for unexpected types or structures
            return {k: parse_dynamodb_attribute(v) for k, v in attr.items()}
    else:
        # Not a dictionary, return as-is (this should not happen with standard DynamoDB structures)
        return attr

def convert_dynamodb_item_to_plain_json(dynamodb_item):
    """Convert a DynamoDB item to plain JSON."""
    return {k: parse_dynamodb_attribute(v) for k, v in dynamodb_item.items()}

def scan_and_upload_ddb_table_to_s3(upload_id, ddb_table_name, output_bucket, output_key):
    parts = []
    part_number = 0
    ndjson_buffer = BytesIO()  # Create a buffer to accumulate NDJSON data

    # Use Paginator to efficiently iterate over large datasets
    paginator = ddb_client.get_paginator('scan')
    for page in paginator.paginate(TableName=ddb_table_name):
        for item in page['Items']:
            plain_item = convert_dynamodb_item_to_plain_json(item)
            json_item = json.dumps(plain_item, ensure_ascii=False)
            ndjson_buffer.write((json_item + '\n').encode('utf-8'))

            # Check buffer size after a batch of writes
            if ndjson_buffer.tell() >= 5 * 1024 * 1024:  # 5MB
                part_number += 1
                etag = upload_part(output_bucket, output_key, upload_id, part_number, ndjson_buffer.getvalue())
                parts.append({'PartNumber': part_number, 'ETag': etag})
                ndjson_buffer.truncate(0)  # Clear the buffer
                ndjson_buffer.seek(0)  # Reset buffer position

    # Upload any remaining data in the buffer
    if ndjson_buffer.tell() > 0:
        part_number += 1
        etag = upload_part(output_bucket, output_key, upload_id, part_number, ndjson_buffer.getvalue())
        parts.append({'PartNumber': part_number, 'ETag': etag})

    complete_multipart_upload(output_bucket, output_key, upload_id, parts)

def lambda_handler(event, context):
    for ddb_table_name in DDB_TABLES_LIST:
        output_bucket = S3_BUCKET_NAME
        output_key = f'{ddb_table_name}.ndjson'

        print(f"Starting export of DynamoDB table '{ddb_table_name}' to '{output_key}' in S3 bucket '{output_bucket}'")
        
        try:
            upload_id = initiate_multipart_upload(output_bucket, output_key)
            # Scan DynamoDB table, convert to NDJSON and upload to S3
            scan_and_upload_ddb_table_to_s3(upload_id, ddb_table_name, output_bucket, output_key)

            # Create or replace the Tinybird data source
            print(f"Updating Tinybird data source with name '{ddb_table_name}'")
            replace_tinybird_datasource(ddb_table_name, output_bucket, output_key)

        except boto3.exceptions.Boto3Error as boto_err:
            print(f"Boto3 error: {boto_err}")
            abort_multipart_upload(output_bucket, output_key, upload_id)
            raise boto_err
        except Exception as e:
            print(f"Error during processing: {e}")
            abort_multipart_upload(output_bucket, output_key, upload_id)
            raise e
        finally:
            http.clear()  # Close all connection pools to avoid resource leakage

    print("Lambda execution completed.")
