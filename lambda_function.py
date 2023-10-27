import boto3
import json
import os
import urllib3
from urllib.parse import urlencode
from io import BytesIO

# Retrieve the Tinybird API key from the environment variable
TINYBIRD_API_KEY = os.environ['TB_DS_ADMIN_TOKEN']

DYNAMO_TABLE_NAME = '' # e.g 'my-dynamodb-table'
DYNAMO_REGION = '' # e.g. 'eu-west-1'
OUTPUT_BUCKET = ''  # e.g 'my-s3-bucket'
OUTPUT_KEY = ''  # e.g. 'my-folder/my-file.ndjson'
TINYBIRD_API_ENDPOINT = 'https://api.tinybird.co/v0/datasources'
TINYBIRD_TABLE_NAME = ''  # e.g. 'my-tinybird-table'
DOWNLOAD_URL_EXPIRATION = 1800  # 30 minutes

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb', region_name=DYNAMO_REGION)

def generate_presigned_url(bucket_name, object_key, expiration=1800):
    """Generate a presigned URL for the S3 object"""
    s3_client = boto3.client('s3')
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

def replace_tinybird_datasource():
    # Prepare the request headers
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',  # Adjust content type if needed
    }

    # Prepare the Tinybird API request payload
    tinybird_params = {
        'name': TINYBIRD_TABLE_NAME,
        'mode': 'replace',
        'format': 'ndjson',
        'url': generate_presigned_url(OUTPUT_BUCKET, OUTPUT_KEY, expiration=DOWNLOAD_URL_EXPIRATION),
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
            print(f"Data source '{TINYBIRD_TABLE_NAME}' does not exist. Please download the NDJSON from the bucket and create a new data source in Tinybird using the UI.")
        else:
            raise Exception(f"HTTP request to Tinybird failed with status code {response.status}: {error_message}")
    else:
        # Log the response from Tinybird (for debugging)
        print(response.status)
        print(response.data.decode('utf-8'))

def parse_dynamodb_attribute(attr):
    """Recursively parse DynamoDB attribute to a Python object."""
    if isinstance(attr, dict):
        # String type
        if 'S' in attr:
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
        # Handle other types if needed
        else:
            return attr
    else:
        return attr

def convert_dynamodb_item_to_plain_json(dynamodb_item):
    """Convert a DynamoDB item to plain JSON."""
    return {k: parse_dynamodb_attribute(v) for k, v in dynamodb_item.items()}


def lambda_handler(event, context):
    upload_id = initiate_multipart_upload(OUTPUT_BUCKET, OUTPUT_KEY)
    parts = []
    part_number = 0

    try:
        ndjson_buffer = BytesIO()  # Create a buffer to accumulate NDJSON data
        
        # Use Paginator to efficiently iterate over large datasets
        paginator = dynamodb_client.get_paginator('scan')
        for page in paginator.paginate(TableName=DYNAMO_TABLE_NAME):
            for item in page['Items']:
                plain_item = convert_dynamodb_item_to_plain_json(item)
                json_item = json.dumps(plain_item, ensure_ascii=False)
                ndjson_buffer.write((json_item + '\n').encode('utf-8'))
        
                # Check buffer size after a batch of writes
                if ndjson_buffer.tell() >= 5 * 1024 * 1024:  # 5MB
                    part_number += 1
                    etag = upload_part(OUTPUT_BUCKET, OUTPUT_KEY, upload_id, part_number, ndjson_buffer.getvalue())
                    parts.append({'PartNumber': part_number, 'ETag': etag})
                    ndjson_buffer.truncate(0)  # Clear the buffer
                    ndjson_buffer.seek(0)  # Reset buffer position

        # Upload any remaining data in the buffer
        if ndjson_buffer.tell() > 0:
            part_number += 1
            etag = upload_part(OUTPUT_BUCKET, OUTPUT_KEY, upload_id, part_number, ndjson_buffer.getvalue())
            parts.append({'PartNumber': part_number, 'ETag': etag})

        complete_multipart_upload(OUTPUT_BUCKET, OUTPUT_KEY, upload_id, parts)

        # Create or replace the Tinybird data source
        replace_tinybird_datasource()

    except Exception as e:
        print(f"Error during processing: {e}")
        abort_multipart_upload(OUTPUT_BUCKET, OUTPUT_KEY, upload_id)
        raise e

    finally:
        pass
