import json
import os
import time
from decimal import Decimal
from logging import INFO, getLogger
import base64
import re

import boto3
from boto3.dynamodb.types import TypeDeserializer, Binary
from botocore.exceptions import ClientError
import urllib3
from urllib.parse import urlencode, urljoin

# Conditional imports for modules only used on the S3 pathway
def s3_conditional_setup():
    global gzip, uuid, BytesIO, datetime, s3, dynamodb
    import gzip
    import uuid
    from io import BytesIO
    from datetime import datetime
    s3 = boto3.client('s3')
    dynamodb = boto3.client('dynamodb')

# Setup Logging
logger = getLogger('DDBStreamCDC')
logger.setLevel(os.environ.get("LOGGING_LEVEL") or INFO)

deserializer = TypeDeserializer()

TINYBIRD_API_KEY_FROM_ENV = os.environ.get('TB_CREATE_DS_TOKEN')  # DATASOURCE:CREATE and DATASOURCE:APPEND permissions
TINYBIRD_API_KEY_FROM_SECRET = os.environ.get('TB_CREATE_DS_TOKEN_SECRET_NAME')
TINYBIRD_API_KEY_SECRET_KEY_NAME = os.environ.get('TB_CREATE_DS_TOKEN_SECRET_KEY_NAME', 'TB_CREATE_DS_TOKEN')
TINYBIRD_API_ENDPOINT = os.environ.get('TB_API_ENDPOINT', 'https://api.tinybird.co/v0/')
TINYBIRD_DS_PREFIX = os.environ.get('TB_DS_PREFIX') or 'ddb_'
TINYBIRD_SKIP_TABLE_CHECK = os.environ.get('TB_SKIP_TABLE_CHECK', 'False').lower() in ['true', '1', 't', 'yes', 'y']
TINYBIRD_KEY_PREFIX = os.environ.get('TB_KEY_PREFIX', 'key_')
BATCH_SIZE = int(os.environ.get('TB_BATCH_SIZE', 250))  # Conservative default
REGION = os.environ.get('AWS_REGION')
MAX_PAYLOAD_SIZE = 10 * 1024 * 1024  # 10MB to Tinybird. Max from DDB is about 6Mb.
MAX_NAME_LENGTH = 128 # Maximum length for a Tinybird Datasource name
TABLE_KEY_SCHEMAS = {}

# Global variables for caching
# The API Key and Tinybird Landing Table Existence are cached for the duration of the Lambda execution
GLOBAL_CACHE = {}
CACHE_EXPIRATION = 300  # 5 minutes

def get_from_cache(key):
    if key in GLOBAL_CACHE:
        value, timestamp = GLOBAL_CACHE[key]
        if time.time() - timestamp < CACHE_EXPIRATION:
            return value
    return None

def set_in_cache(key, value):
    GLOBAL_CACHE[key] = (value, time.time())

class DDBEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, Binary):
            # DynamoDB stream base64-encodes all binary types, must base64 decode first
            return base64.b64encode(o.value).decode('utf-8')
        elif isinstance(o, bytes):
            return base64.b64encode(o).decode('utf-8')
        return super().default(o)

def lambda_handler(event, context):
    logger.info(f"Lambda Handler start, initialization complete")
    try:
        global TINYBIRD_API_KEY
        TINYBIRD_API_KEY = get_tinybird_api_key()
        # Check Region is set
        if not REGION:
            raise ValueError("AWS Region not set in environment variables")
        if 'Records' in event:
            if 's3' in event['Records'][0]:
                logger.debug(f"Processing event type S3: {json.dumps(event)}")
                s3_conditional_setup()
                process_s3_event(event)
            elif 'dynamodb' in event['Records'][0]:
                logger.debug(f"Processing event type DDBStream: {json.dumps(event)}")
                process_dynamodb_stream_event(event)
            else:
                logger.error("Unsupported event type")
                raise ValueError("Unsupported event type")
        return {"statusCode": 200, "body": json.dumps("Processing complete")}
    except Exception as e:
        logger.error(f"Error processing event: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps(str(e))}

def get_tinybird_api_key():
    # First, check if the API key is set as an environment variable
    if TINYBIRD_API_KEY_FROM_ENV:
        logger.info("Using Tinybird API key from environment variable")
        return TINYBIRD_API_KEY_FROM_ENV
    elif not TINYBIRD_API_KEY_FROM_SECRET:
        raise ValueError("Tinybird API key or Secret Name not found in environment variables. See readme for instructions.")

    # If not found in environment variables, fetch from Secrets Manager
    if cached_secret := get_from_cache(TINYBIRD_API_KEY_SECRET_KEY_NAME):
        logger.info("Using Tinybird API key from cache")
        return cached_secret
    else:
        try:
            secrets_client = boto3.client('secretsmanager')
            logger.info(f"Fetching Tinybird API key from Secrets Manager: {TINYBIRD_API_KEY_FROM_SECRET}")
            response = secrets_client.get_secret_value(SecretId=TINYBIRD_API_KEY_FROM_SECRET)
            if 'SecretString' in response:
                secret = json.loads(response['SecretString'])
                api_key = secret.get(TINYBIRD_API_KEY_SECRET_KEY_NAME)
                if not api_key:
                    logger.error("API key not found in the secret")
                    raise ValueError("API key not found in the secret")
                logger.info("Using Tinybird API key from Secrets Manager - updating cache and returning")
                set_in_cache(TINYBIRD_API_KEY_SECRET_KEY_NAME, api_key)
                return api_key
            else:
                logger.error("Secret value not found in the expected format")
                raise ValueError("Secret value not found in the expected format")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.error(f"The secret {TINYBIRD_API_KEY_FROM_SECRET} was not found")
            else:
                logger.error(f"Error fetching secret: {str(e)}")
            raise

def process_s3_event(event):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # If the customer hasn't filtered by .gz in the Lambda Trigger, this will catch it.
        if key.endswith('.gz'):
            process_export_file(bucket, key)
        else:
            logger.info(f"Skipping non-gzipped file: {key}")

def process_export_file(bucket, key):
    logger.debug(f"Processing {bucket} having {key}")
    response = s3.get_object(Bucket=bucket, Key=key)
    # We need to get the table name from the manifest file as it's not in the data
    manifest_key = get_manifest_key(bucket, key)
    table_arn, export_time = get_snapshot_info_from_manifest(bucket, manifest_key)
    table_name, account_id = get_ddb_table_info_from_arn(table_arn)
    batch = []
    batch_size = 0
    key_types = {}
    
    try:
        with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gzipfile:
            for line in gzipfile:
                item = json.loads(line)
                event = create_stream_event(item, account_id, export_time, table_name)
                ndjson_records, new_key_types = create_ndjson_records([event])
                key_types.update(new_key_types)
                event_size = len(json.dumps(ndjson_records[0]))
                batch += ndjson_records
                
                if len(batch) >= BATCH_SIZE or batch_size + event_size > MAX_PAYLOAD_SIZE:
                    post_records_batch(batch, key_types)
                    batch = []
                    batch_size = 0
                    key_types = {}
        
        if batch:
            post_records_batch(batch, key_types)
        logger.info(f"Processed file {key}")
    except (gzip.BadGzipFile, json.JSONDecodeError) as e:
        logger.error(f"Error processing file {key}: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error processing file {key}: {str(e)}")
        raise e

def get_snapshot_info_from_manifest(bucket, manifest_key):
    try:
        response = s3.get_object(Bucket=bucket, Key=manifest_key)
    except Exception as e:
        logger.error(f"Error reading manifest file {manifest_key}: {str(e)}")
        raise e
    manifest = json.loads(response['Body'].read().decode('utf-8'))
    table_arn = manifest.get('tableArn')
    export_datetime = manifest.get('exportTime')
    export_timestamp = int(datetime.strptime(export_datetime, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
    logger.debug(f"Extracted Table ARN: {table_arn}, Export Time: {export_datetime}")
    return table_arn, export_timestamp

def get_manifest_key(bucket, data_file_key, max_retries=5, delay=1):
    manifest_key = '/'.join(data_file_key.split('/')[:-2] + ['manifest-summary.json'])
    logger.debug(f"Checking for manifest file: {manifest_key}")
    
    for attempt in range(max_retries):
        try:
            s3.head_object(Bucket=bucket, Key=manifest_key)
            logger.debug(f"Manifest file found for data file: {data_file_key}")
            return manifest_key
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.warning(f"Attempt {attempt + 1}/{max_retries}: Manifest file not found for data file: {data_file_key}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (2 ** attempt))  # Exponential backoff
                continue
            else:
                logger.error(f"Error checking for manifest file: {str(e)}")
                raise

    logger.error(f"Manifest file not found after {max_retries} attempts for data file: {data_file_key}")
    return None

def get_ddb_table_info_from_arn(table_arn):
    # arn like arn:aws:dynamodb:eu-west-2:819314934727:table/PeopleTable
    table_name = table_arn.split("/")[-1]
    account_id = table_arn.split(":")[4]
    logger.debug(f"Extracted Table name: {table_name}, Account ID: {account_id}")
    if table_name is None or account_id is None:
        raise ValueError(f"Table name or Account ID cannot be empty, could not extract from {table_arn}")
    return table_name, account_id

def extract_table_name_from_record(record):
    value = record['eventSourceARN'].split(':table/')[1].split('/')[0]
    if not value:
        raise ValueError(f"DynamoDB Table name cannot be empty, could not extract from {record['eventSourceARN']}")
    return value

def process_dynamodb_stream_event(event):
    if records := event.get("Records"):
        ndjson_records, key_types = create_ndjson_records(records)
        post_records_batch(ndjson_records, key_types)
    else:
        logger.info("No records to process")

def from_dynamodb_to_json(item):
    return {k: __deserialize(v) for k, v in item.items()}

def __deserialize(value):
    dynamodb_type = list(value.keys())[0]
    
    # DynamoDB stream base64-encodes all binary types, must base64 decode first for consistency
    if dynamodb_type == "B" and isinstance(value["B"], str):
        value = {"B": base64.b64decode(value["B"])}
    
    return deserializer.deserialize(value)

def get_ddb_table_schema(table_name):
    """Used for the S3 pathway to get Key information for generating the events."""
    if table_name in TABLE_KEY_SCHEMAS:
        return TABLE_KEY_SCHEMAS[table_name]
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        table_info = {
            'KeySchema': [item['AttributeName'] for item in response['Table']['KeySchema']]
        }
        TABLE_KEY_SCHEMAS[table_name] = table_info
        return table_info
    except Exception as e:
        logger.error(f"Error getting table info for {table_name}: {str(e)}")
        raise e

def create_stream_event(item, account_id, export_time, table_name):
    # For S3 pathway, we need to get the table info to extract the keys
    table_info = get_ddb_table_schema(table_name)
    keys = {attr_name: item['Item'][attr_name] for attr_name in table_info['KeySchema'] if attr_name in item['Item']}
    
    event = {
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
    logger.debug(f"Created event: {json.dumps(event)}")
    return event

def create_ndjson_records(records):
    if not records:
        return [], {}

    key_types = {}
    ndjson_records = []

    for record in records:
        keys = record["dynamodb"].get("Keys", {})
        extracted_keys = {}
        for k, v in keys.items():
            key_name = ddb_name_to_tinybird_name(k, TINYBIRD_KEY_PREFIX)
            if key_name not in key_types:
                key_types[key_name] = list(v.keys())[0]
            extracted_keys[key_name] = __deserialize(v)

        ndjson_record = {
            "eventID": record["eventID"],
            "eventName": record["eventName"],
            "eventSource": record["eventSource"],
            "eventSourceARN": record["eventSourceARN"],
            "eventVersion": float(record["eventVersion"]),
            "awsRegion": record["awsRegion"],
            "ApproximateCreationDateTime": int(record["dynamodb"].get("ApproximateCreationDateTime", 0)),
            "SequenceNumber": record["dynamodb"].get("SequenceNumber", ""),
            "SizeBytes": int(record["dynamodb"].get("SizeBytes", 0)),
            "StreamViewType": record["dynamodb"].get("StreamViewType", ""),
            "Keys": json.dumps(from_dynamodb_to_json(record["dynamodb"].get("Keys", {})), cls=DDBEncoder),
            "NewImage": json.dumps(from_dynamodb_to_json(record["dynamodb"].get("NewImage", {})), cls=DDBEncoder),
            "OldImage": json.dumps(from_dynamodb_to_json(record["dynamodb"].get("OldImage", {})), cls=DDBEncoder),
            **extracted_keys
        }
        ndjson_records.append(ndjson_record)
        
    logger.debug(f"Created NDJSON records: {json.dumps(ndjson_records)} with key types: {key_types}")

    return ndjson_records, key_types

def normalize_tinybird_endpoint(endpoint):
    # Remove any trailing slashes, strip whitespace, and convert to lowercase
    endpoint = endpoint.rstrip('/').strip().lower()
    
    # Define the regex pattern to match the domain starting with 'api'
    pattern = r'^(https?://)?(api\.([a-z0-9-]+\.)*tinybird\.co)(/.*)?$'
    
    # Try to match the pattern
    match = re.search(pattern, endpoint)
    
    if match:
        # Extract the core domain
        core_domain = match.group(2)
        
        # Construct the normalized URL always using https
        normalized_endpoint = f'https://{core_domain}/v0/'
        
        return normalized_endpoint
    else:
        raise ValueError(f"Invalid Tinybird API endpoint: {endpoint}")

def call_tinybird(method, service, params, data=None):
    headers = {
        'Authorization': f'Bearer {TINYBIRD_API_KEY}',
        'Content-Type': 'application/json',
    }
    normalized_url = normalize_tinybird_endpoint(TINYBIRD_API_ENDPOINT)
    url = urljoin(normalized_url, f"{service}?{urlencode(params)}")
    logger.debug(f"Calling Tinybird API: {method} - {url}")
    with urllib3.PoolManager() as http:
        return http.request(
            method,
            url,
            body=data.encode('utf-8') if data else None,
            headers=headers,
        )

def ddb_type_to_clickhouse_type(ddb_type):
    if ddb_type == 'S':
        return 'String'
    elif ddb_type == 'N':
        return 'Float64'  # Using Float64 to cover both integer and decimal numbers
    elif ddb_type == 'B':
        return 'String'  # We'll store binary data as base64 encoded strings
    else:
        return 'String'  # Default to String for unknown types

def get_default_value(ch_type):
    if ch_type == 'String':
        return "DEFAULT ''"
    elif ch_type == 'Float64':
        return "DEFAULT 0"
    else:
        return "DEFAULT ''"  # Default for any other type

def ensure_datasource_exists(full_table_name, key_types):
    cache_key = f"tinybird_table_{full_table_name}"
    cached_status = get_from_cache(cache_key)
    if cached_status:
        logger.info(f"Skipping Table exists check for {full_table_name} - Cache hit")
        return
    response = call_tinybird("GET", "datasources", {"attrs": "name"})
    logger.debug(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")
    
    if response.status == 200:
        try:
            datasources = json.loads(response.data.decode('utf-8'))['datasources']
            existing_tables = [ds['name'] for ds in datasources]
            
            if full_table_name in existing_tables:
                logger.info(f"Table {full_table_name} already exists - updating cache and returning")
                set_in_cache(cache_key, "datasource_exists")
                return
            else:
                logger.info(f"Table {full_table_name} not found in existing table list {existing_tables}, creating it now")
        except json.JSONDecodeError:
            logger.error("Failed to parse Tinybird API response")
            raise Exception("Failed to parse Tinybird API response")
        except KeyError:
            logger.error("Unexpected Tinybird API response format")
            raise Exception("Unexpected Tinybird API response format")
    
    # If we've reached here, the table doesn't exist or we couldn't confirm its existence
    # Proceed with creating the datasource
    key_schema = []
    sorting_key = ["eventName",]

    for key, ddb_type in key_types.items():
        ch_type = ddb_type_to_clickhouse_type(ddb_type)
        key_schema.append(f"`{key}` {ch_type} `json:$.{key}` {get_default_value(ch_type)}")
        sorting_key.append(key)
    sorting_key.append("ApproximateCreationDateTime")

    schema = (
        "`eventID` String `json:$.eventID`, "
        "`eventName` LowCardinality(String) `json:$.eventName`, "
        "`eventSource` LowCardinality(String) `json:$.eventSource`, "
        "`eventSourceARN` LowCardinality(String) `json:$.eventSourceARN`, "
        "`eventVersion` Float32 `json:$.eventVersion`, "
        "`awsRegion` LowCardinality(String) `json:$.awsRegion`, "
        "`ApproximateCreationDateTime` Int64 `json:$.ApproximateCreationDateTime`, "
        "`SequenceNumber` String `json:$.SequenceNumber`, "
        "`SizeBytes` UInt32 `json:$.SizeBytes`, "
        "`StreamViewType` LowCardinality(String) `json:$.StreamViewType`, "
        "`Keys` String `json:$.Keys`, "
        "`NewImage` String `json:$.NewImage`, "
        "`OldImage` String `json:$.OldImage`, "
    ) + ', '.join(key_schema)

    params = {
        "name": full_table_name,
        "format": "ndjson",
        "mode": "create",
        "schema": schema,
        "engine": "MergeTree",
        "engine_sorting_key": ','.join(sorting_key)
    }
    logger.debug(f"Calling Tinybird API to create Datasource {full_table_name} with schema: {schema} params: {params}")
    create_response = call_tinybird("POST", "datasources", params)
    if create_response.status >= 400:
        error_message = create_response.data.decode('utf-8')
        raise Exception(f"Tinybird API request to create Datasource failed: {create_response.status} - {error_message}")
    else:
        logger.info(f"Tinybird API response: {create_response.status} - {create_response.data.decode('utf-8')}")

def ddb_name_to_tinybird_name(ddb_value, tb_prefix=None):
    # Tinybird requires a Datasource name to start with a letter and contain only lowercase letters, numbers, and underscores
    # it also cannot be a reserved word, which the prefix will prevent
    # DynamoDb additionally allows some characters like . and - in names, but Tinybird does not.
    # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html

    # Replace invalid characters with underscores
    out_value = re.sub(r'[^a-zA-Z0-9_]', '_', ddb_value)
    
    if tb_prefix:
        # Ensure prefix isn't empty and starts with a letter and uses valid characters, or use default prefix
        if not tb_prefix or not tb_prefix[0].isalpha():
            logger.warning(f"Configured Prefix {tb_prefix} does not start with a letter, using default 'ddb_'")
            prefix = 'ddb_'
        else:
            prefix = re.sub(r'[^a-zA-Z0-9_]', '_', tb_prefix)
            # ensure the prefix has a trailing underscore
            prefix = f"{prefix}_" if prefix[-1] != '_' else prefix
        out_value = f"{prefix}{out_value}"
    
    # Remove consecutive underscores and trailing underscores and truncate to maximum allowed length
    out_value = re.sub(r'_+', '_', out_value).rstrip('_')[:MAX_NAME_LENGTH]
    logger.debug(f"Converted DynamoDB Name {ddb_value} with prefix {tb_prefix} to Tinybird name {out_value}")
    return out_value

def post_records_batch(records, key_types):
    base_table_name = extract_table_name_from_record(records[0])
    full_table_name = ddb_name_to_tinybird_name(base_table_name, TINYBIRD_DS_PREFIX)
    
    if not TINYBIRD_SKIP_TABLE_CHECK:
        ensure_datasource_exists(full_table_name, key_types)
    
    response = call_tinybird(
        method='POST', 
        service='events', 
        params={'name': full_table_name, 'format': 'ndjson'}, 
        data='\n'.join(json.dumps(record, ensure_ascii=False, cls=DDBEncoder) for record in records)
    )

    if response.status >= 400:
        error_message = response.data.decode('utf-8')
        raise Exception(f"Tinybird API request failed: {response.status} - {error_message}")
    else:
        logger.info(f"Tinybird API response: {response.status} - {response.data.decode('utf-8')}")
