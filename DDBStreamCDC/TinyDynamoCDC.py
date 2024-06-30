import os
import json
import gzip
from io import BytesIO
from datetime import datetime
from logging import INFO, getLogger
import urllib3
from urllib.parse import urlencode
import boto3
from boto3.dynamodb.types import TypeDeserializer, Binary
from decimal import Decimal

s3 = boto3.client('s3')
getLogger().setLevel(os.environ.get("LOGGING_LEVEL") or INFO)

REGION = os.environ['AWS_REGION']
TINYBIRD_API_KEY = os.environ['TB_DS_ADMIN_TOKEN']
TINYBIRD_API_ENDPOINT = os.environ.get('TB_API_ENDPOINT', 'https://api.tinybird.co/v0/events')
TINYBIRD_DS_NAME = os.environ['TB_DS_NAME']
BATCH_SIZE = os.environ.get('TB_BATCH_SIZE') or 250
MAX_PAYLOAD_SIZE = 5 * 1024 * 1024  # 5MB

deserializer = TypeDeserializer()

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
        ndjson_records = create_ndjson_records(records)
        post_records_batch(ndjson_records)
    else:
        getLogger().info("No records to process")

def process_export_file(bucket, key, account_id):
    response = s3.get_object(Bucket=bucket, Key=key)
    export_time = get_export_time(response)
    
    batch = []
    batch_size = 0
    
    try:
        with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gzipfile:
            for line in gzipfile:
                item = json.loads(line)
                event = create_stream_event(item, account_id, export_time)
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
    except Exception as e:
        getLogger().error(f"Unexpected error processing file {key}: {str(e)}")

def get_export_time(response):
    export_time = response.get('Metadata', {}).get('exportTime')
    if export_time:
        return int(datetime.strptime(export_time, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
    return int(response['LastModified'].timestamp())

def create_stream_event(item, account_id, export_time):
    return {
        "eventID": f"SNAPSHOT-{item['Item']['id']['S']}-{int(datetime.now().timestamp()*1000)}",
        "eventName": "SNAPSHOT",
        "eventVersion": "1.1",
        "eventSource": "aws:dynamodbSnapshot",
        "awsRegion": REGION,
        "dynamodb": {
            "ApproximateCreationDateTime": export_time,
            "Keys": {"id": item['Item']['id']},
            "NewImage": item['Item'],
            "SequenceNumber": "SNAPSHOT",
            "SizeBytes": len(json.dumps(item['Item']).encode('utf-8')),
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        },
        "eventSourceARN": f"arn:aws:dynamodb:{REGION}:{account_id}:table/{item['Item'].get('table_name', 'unknown')}"
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

def post_records_batch(records):
    ndjson_data = '\n'.join(json.dumps(record, ensure_ascii=False, cls=DDBEncoder) for record in records)
    
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',
    }
    
    params = {
        'name': TINYBIRD_DS_NAME,
        'format': 'ndjson',
    }
    
    url = f"{TINYBIRD_API_ENDPOINT}?{urlencode(params)}"
    
    with urllib3.PoolManager() as http:
        response = http.request(
            'POST',
            url,
            body=ndjson_data.encode('utf-8'),
            headers=headers,
        )
        
        if response.status >= 400:
            error_message = response.data.decode('utf-8')
            raise Exception(f"Tinybird API request failed: {response.status} - {error_message}")
        else:
            getLogger().info(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")