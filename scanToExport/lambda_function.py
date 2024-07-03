import os
from decimal import Decimal
import urllib3
from urllib.parse import urlencode
import boto3
from boto3.dynamodb.types import TypeDeserializer, Binary
import json

# Fetch environment variables
TINYBIRD_API_KEY = os.environ['TB_DS_ADMIN_TOKEN']
TINYBIRD_API_ENDPOINT = os.environ.get('TB_API_ENDPOINT', 'https://api.tinybird.co/v0/datasources')
DDB_TABLES_TO_EXPORT = os.environ['DDB_TABLES_TO_EXPORT'].split(',')
DDB_REGION = os.environ.get('DDB_REGION', 'us-east-1')
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
DOWNLOAD_URL_EXPIRATION = int(os.environ.get('DOWNLOAD_URL_EXPIRATION', 1800))

# Initialize clients
s3_client = boto3.client('s3')
ddb_client = boto3.client('dynamodb', region_name=DDB_REGION)
deserializer = TypeDeserializer()
BUFFER_SIZE = 5 * 1024 * 1024  # 5MB, Amazon's minimum part size

class DDBEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, Binary):
            return bytes(o)
        return super().default(o)

def generate_presigned_url(bucket_name, object_key, expiration=1800):
    """Generate a presigned URL for the S3 object"""
    return s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket_name, 'Key': object_key}, ExpiresIn=expiration)

def replace_tinybird_datasource(tinybird_table_name, output_bucket, output_key):
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',
    }
    params = {
        'name': tinybird_table_name,
        'mode': 'replace',
        'format': 'ndjson',
        'url': generate_presigned_url(output_bucket, output_key, expiration=DOWNLOAD_URL_EXPIRATION),
    }
    with urllib3.PoolManager() as http:
        response = http.request(
            'POST',
            f"{TINYBIRD_API_ENDPOINT}?{urlencode(params)}",
            headers=headers,
        )
        if response.status >= 400:
            error_message = response.data.decode('utf-8')
            if "can not replace a non existing Data Source" in error_message:
                print(f"Data source '{tinybird_table_name}' does not exist. Please create it manually.")
            else:
                raise Exception(f"Tinybird API request failed: {response.status} - {error_message}")
        else:
            print(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")

def stream_ddb_table_to_s3(ddb_table_name, output_bucket, output_key):
    s3_object = s3_client.create_multipart_upload(Bucket=output_bucket, Key=output_key)
    upload_id = s3_object['UploadId']
    parts = []
    part_number = 1
    buffer = bytearray()

    try:
        paginator = ddb_client.get_paginator('scan')
        for page in paginator.paginate(TableName=ddb_table_name):
            for item in page['Items']:
                plain_item = {k: deserializer.deserialize(v) for k, v in item.items()}
                json_line = json.dumps(plain_item, ensure_ascii=False, cls=DDBEncoder).encode('utf-8') + b'\n'
                buffer.extend(json_line)

                if len(buffer) >= BUFFER_SIZE:
                    part = s3_client.upload_part(Body=buffer, Bucket=output_bucket, Key=output_key, PartNumber=part_number, UploadId=upload_id)
                    parts.append({"PartNumber": part_number, "ETag": part['ETag']})
                    part_number += 1
                    buffer = bytearray()

        # Upload any remaining data
        if buffer:
            part = s3_client.upload_part(Body=buffer, Bucket=output_bucket, Key=output_key, PartNumber=part_number, UploadId=upload_id)
            parts.append({"PartNumber": part_number, "ETag": part['ETag']})

        # Complete the multipart upload
        s3_client.complete_multipart_upload(Bucket=output_bucket, Key=output_key, UploadId=upload_id, MultipartUpload={"Parts": parts})
    except Exception as e:
        s3_client.abort_multipart_upload(Bucket=output_bucket, Key=output_key, UploadId=upload_id)
        raise e

def lambda_handler(event, context):
    for ddb_table_name in DDB_TABLES_TO_EXPORT:
        output_key = f'{ddb_table_name}.ndjson'
        print(f"Exporting '{ddb_table_name}' to '{output_key}' in '{S3_BUCKET_NAME}'")
        
        try:
            stream_ddb_table_to_s3(ddb_table_name, S3_BUCKET_NAME, output_key)
            print(f"Updating Tinybird data source '{ddb_table_name}'")
            replace_tinybird_datasource(ddb_table_name, S3_BUCKET_NAME, output_key)
        except Exception as e:
            print(f"Error processing {ddb_table_name}: {e}")
            raise

    print("Lambda execution completed.")