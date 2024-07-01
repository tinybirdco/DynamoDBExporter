import os
import json
import gzip
import uuid
from io import BytesIO
from datetime import datetime
from logging import INFO, getLogger
import urllib3
from urllib.parse import urlencode, urljoin
import boto3
from boto3.dynamodb.types import TypeDeserializer, Binary
from decimal import Decimal

getLogger().setLevel(os.environ.get("LOGGING_LEVEL") or INFO)
s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
deserializer = TypeDeserializer()

TINYBIRD_API_KEY = os.environ['TB_DS_ADMIN_TOKEN']
TINYBIRD_API_ENDPOINT = os.environ.get('TB_API_ENDPOINT', 'https://api.tinybird.co/v0/')
TINYBIRD_DS_PREFIX = os.environ.get('TB_DS_PREFIX') or 'ddb_'
TINYBIRD_SKIP_TABLE_CHECK = os.environ.get('TB_SKIP_TABLE_CHECK') or False
BATCH_SIZE = os.environ.get('TB_BATCH_SIZE') or 250

REGION = os.environ['AWS_REGION']
MAX_PAYLOAD_SIZE = 5 * 1024 * 1024  # 5MB.
TABLE_KEY_SCHEMAS = {}

class DDBEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, Binary):
            return str(o)
        return super().default(o)

def lambda_handler(event, context):
    getLogger().debug(f"Processing event {json.dumps(event)}")

    if 'Records' in event:
        if 's3' in event['Records'][0]:
            process_s3_event(event, context)
        elif 'dynamodb' in event['Records'][0]:
            process_dynamodb_stream_event(event)
        else:
            getLogger().error("Unsupported event type")
            raise ValueError("Unsupported event type")

    return {"statusCode": 200, "body": json.dumps("Processing complete")}

def process_s3_event(event, context):
    account_id = context.invoked_function_arn.split(":")[4]
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        if key.endswith('.gz'):
            process_export_file(bucket, key, account_id)
        else:
            getLogger().info(f"Skipping non-gzipped file: {key}")

def process_dynamodb_stream_event(event):
    if records := event.get("Records"):
        post_records_batch(create_ndjson_records(records))
    else:
        getLogger().info("No records to process")

def extract_table_name_from_s3_key(key):
    parts = key.split('/')
    try:
        aws_dynamodb_index = parts.index('AWSDynamoDB')
        if aws_dynamodb_index > 0:
            return parts[aws_dynamodb_index - 1]
    except ValueError:
        pass  # 'AWSDynamoDB' not found in the key
    
    getLogger().warning(f"Unable to extract table name from S3 key: {key}")
    return None

def get_table_key_schema(table_name):
    if table_name in TABLE_KEY_SCHEMAS:
        return TABLE_KEY_SCHEMAS[table_name]
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        getLogger().debug(f"DDB Table details: {response}")
        key_schema = response['Table']['KeySchema']
        parsed_key_schema = [item['AttributeName'] for item in key_schema]
        getLogger().debug(f"Key schema for table {table_name}: {parsed_key_schema}")
        TABLE_KEY_SCHEMAS[table_name] = parsed_key_schema
        return parsed_key_schema
    except Exception as e:
        getLogger().error(f"Error getting key schema for table {table_name}: {str(e)}")
        raise e

def process_export_file(bucket, key, account_id):
    getLogger().debug(f"Processing file {key} for bucket {bucket}")
    response = s3.get_object(Bucket=bucket, Key=key)
    export_time = get_export_time(response)
    table_name = extract_table_name_from_s3_key(key)
    getLogger().debug(f"Details for S3 Export time: {export_time}, Table name: {table_name}")
    
    batch = []
    batch_size = 0
    
    try:
        with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gzipfile:
            for line in gzipfile:
                getLogger().debug(f"Processing line: {line}")
                item = json.loads(line)
                event = create_stream_event(item, account_id, export_time, table_name)
                event_size = len(json.dumps(event))
                
                if len(batch) >= BATCH_SIZE or batch_size + event_size > MAX_PAYLOAD_SIZE:
                    post_records_batch(create_ndjson_records(batch))
                    batch = []
                    batch_size = 0
                
                batch.append(event)
                batch_size += event_size
        
        if batch:
            post_records_batch(create_ndjson_records(batch))
    except (gzip.BadGzipFile, json.JSONDecodeError) as e:
        getLogger().error(f"Error processing file {key}: {str(e)}")
        raise e
    except Exception as e:
        getLogger().error(f"Unexpected error processing file {key}: {str(e)}")
        raise e

def get_export_time(response):
    export_time = response.get('Metadata', {}).get('exportTime')
    if export_time:
        return int(datetime.strptime(export_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
    return int(response['LastModified'].timestamp())

def create_stream_event(item, account_id, export_time, table_name):
    key_schema = get_table_key_schema(table_name)
    keys = {attr: item['Item'][attr] for attr in key_schema if attr in item['Item']}
    
    return {
        "eventID": str(uuid.uuid4()).replace('-', ''),
        "eventName": "SNAPSHOT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodbSnapshot",
        "awsRegion": REGION,
        "dynamodb": {
            "ApproximateCreationDateTime": export_time,
            "Keys": keys,
            "NewImage": item['Item'],
            "SequenceNumber": "SNAPSHOT",
            "SizeBytes": len(json.dumps(item['Item']).encode('utf-8')),
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        },
        "eventSourceARN": f"arn:aws:dynamodb:{REGION}:{account_id}:table/{table_name}/snapshot/{export_time}"
    }

def create_ndjson_records(records):
    return [{
        "eventID": record["eventID"],
        "eventName": record["eventName"],
        "eventSource": record["eventSource"],
        "eventSourceARN": record["eventSourceARN"],
        "eventVersion": float(record["eventVersion"]),
        "awsRegion": record["awsRegion"],
        "ApproximateCreationDateTime": int(record["dynamodb"].get("ApproximateCreationDateTime")),
        "SequenceNumber": record["dynamodb"].get("SequenceNumber"),
        "SizeBytes": int(record["dynamodb"].get("SizeBytes")),
        "StreamViewType": record["dynamodb"].get("StreamViewType"),
        "Keys": json.dumps({k: deserializer.deserialize(v) for k, v in record["dynamodb"].get("Keys", {}).items()}, cls=DDBEncoder),
        "NewImage": json.dumps({k: deserializer.deserialize(v) for k, v in record["dynamodb"].get("NewImage", {}).items()}, cls=DDBEncoder),
        "OldImage": json.dumps({k: deserializer.deserialize(v) for k, v in record["dynamodb"].get("OldImage", {}).items()}, cls=DDBEncoder)
    } for record in records]

def call_tinybird(method, service, params, data=None):
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',
    }
    url = urljoin(TINYBIRD_API_ENDPOINT, f"{service}?{urlencode(params)}")
    getLogger().debug(f"Calling Tinybird API: {method} - {url}")
    with urllib3.PoolManager() as http:
        return http.request(
            method,
            url,
            body=data.encode('utf-8') if data else None,
            headers=headers,
        )

def ensure_datasource_exists(table_name):
    response = call_tinybird("GET", "datasources", {"attrs": "name"})
    getLogger().debug(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")
    
    if response.status == 200:
        try:
            datasources = json.loads(response.data.decode('utf-8'))['datasources']
            existing_tables = [ds['name'] for ds in datasources]
            
            if table_name in existing_tables:
                getLogger().info(f"Table {table_name} already exists")
                return
            else:
                getLogger().info(f"Table {table_name} does not exist, creating it now")
        except json.JSONDecodeError:
            getLogger().error("Failed to parse Tinybird API response")
            raise Exception("Failed to parse Tinybird API response")
        except KeyError:
            getLogger().error("Unexpected Tinybird API response format")
            raise Exception("Unexpected Tinybird API response format")
    
    # If we've reached here, the table doesn't exist or we couldn't confirm its existence
    # Proceed with creating the datasource
    params = {
        "name": table_name,
        "format": "ndjson",
        "mode": "create",
        "schema": "`eventID` String `json:$.eventID`, `eventName` LowCardinality(String) `json:$.eventName`, `eventSource` LowCardinality(String) `json:$.eventSource`, `eventSourceARN` LowCardinality(String) `json:$.eventSourceARN`, `eventVersion` Float32 `json:$.eventVersion`, `awsRegion` LowCardinality(String) `json:$.awsRegion`, `ApproximateCreationDateTime` Int64 `json:$.ApproximateCreationDateTime`, `SequenceNumber` String `json:$.SequenceNumber`, `SizeBytes` UInt32 `json:$.SizeBytes`, `StreamViewType` LowCardinality(String) `json:$.StreamViewType`, `Keys` String `json:$.Keys`, `NewImage` String `json:$.NewImage`, `OldImage` String `json:$.OldImage`",
        "engine": "MergeTree",
        "engine_sorting_key": "eventName, Keys, ApproximateCreationDateTime"
    }
    create_response = call_tinybird("POST", "datasources", params)
    if create_response.status >= 400:
        error_message = create_response.data.decode('utf-8')
        raise Exception(f"Tinybird API request to create Datasource failed: {create_response.status} - {error_message}")
    else:
        getLogger().info(f"Tinybird API response: {create_response.status} - {create_response.data.decode('utf-8')}") 

def post_records_batch(records):
    table_name = TINYBIRD_DS_PREFIX + records[0]['eventSourceARN'].split(':table/')[1].split('/')[0]
    
    if TINYBIRD_SKIP_TABLE_CHECK:
        getLogger().info("Skipping check to ensure Tinybird Datasource exists")
    else:
        ensure_datasource_exists(table_name)
    
    response = call_tinybird(
        method='POST', 
        service='events', 
        params={'name': table_name, 'format': 'ndjson'}, 
        data='\n'.join(json.dumps(record, ensure_ascii=False, cls=DDBEncoder) for record in records)
    )

    if response.status >= 400:
        error_message = response.data.decode('utf-8')
        raise Exception(f"Tinybird API request failed: {response.status} - {error_message}")
    else:
        getLogger().info(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")
