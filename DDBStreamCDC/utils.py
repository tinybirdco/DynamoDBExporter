import boto3
import io
import json
import random
import string
import requests
from faker import Faker
from decimal import Decimal
import argparse
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError
import logging
import time

# Initialize Faker and boto3 clients
fake = Faker()
serializer = TypeSerializer()
dynamodb_res = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
lambda_client = boto3.client('lambda')

# Table name and structure
DEFAULT_TABLE_NAME = "PeopleTable"
DEFAULT_REGION = "eu-west-2"
DEFAULT_S3_BUCKET = "tinybird-test-dynamodb-export"
DEFAULT_S3_PREFIX = "DDBStreamCDC"
DEFAULT_BATCH_SIZE = 250
DEFAULT_STREAM_STARTING_POSITION = "TRIM_HORIZON"
DEFAULT_TINYBIRD_TABLE_PREFIX = "ddb_"
RANDOM_SCAN_LIMIT = 50

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_tinybird_info():
    """Read the Tinybird token and endpoint from the .tinyb file."""
    try:
        with open('.tinyb') as f:
            tinyb_data = json.load(f)
            return tinyb_data['token'], tinyb_data['host']
    except FileNotFoundError:
        logger.error("Please run `tb auth` Tinybird Admin token")
        raise

def enable_pitr(table_name, max_retries=5, initial_delay=1):
    for attempt in range(max_retries):
        try:
            dynamodb_res.meta.client.update_continuous_backups(
                TableName=table_name,
                PointInTimeRecoverySpecification={
                    'PointInTimeRecoveryEnabled': True
                }
            )
            logger.info(f"Point-in-time recovery enabled for table {table_name}")
            return
        except ClientError as e:
            if e.response['Error']['Code'] == 'ContinuousBackupsUnavailableException':
                delay = initial_delay * (2 ** attempt)
                logger.warning(f"PITR not ready, retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise

    logger.error(f"Failed to enable PITR after {max_retries} attempts")

def setup_dynamodb_trigger(table_name, lambda_arn, region):
    sts_client = boto3.client('sts')
    streams_client = boto3.client('dynamodbstreams')

    account_id = sts_client.get_caller_identity()["Account"]

    try:
        table_description = dynamodb_client.describe_table(TableName=table_name)
        stream_specification = table_description['Table'].get('StreamSpecification', {})
        
        if not stream_specification.get('StreamEnabled', False):
            logger.info(f"DynamoDB Streams not enabled for table {table_name}. Enabling now.")
            _ = dynamodb_client.update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
            )
            logger.info(f"Enabled DynamoDB Streams for table {table_name}")
        
        # Re-fetch table description to get the latest stream ARN
        table_description = dynamodb_client.describe_table(TableName=table_name)
        current_stream_arn = table_description['Table'].get('LatestStreamArn')

        if not current_stream_arn:
            logger.error(f"Failed to get stream ARN for table {table_name}")
            return

        # Wait for the stream to be active
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=table_name)

        # Verify stream status
        stream_description = streams_client.describe_stream(StreamArn=current_stream_arn)['StreamDescription']
        if stream_description['StreamStatus'] != 'ENABLED':
            logger.error(f"Stream for table {table_name} is not in ENABLED state. Current state: {stream_description['StreamStatus']}")
            return

    except dynamodb_client.exceptions.ResourceNotFoundException:
        logger.error(f"Table {table_name} not found")
        return
    except Exception as e:
        logger.error(f"Error setting up DynamoDB stream: {str(e)}")
        return

    # Check existing event source mappings
    try:
        existing_mappings = lambda_client.list_event_source_mappings(FunctionName=lambda_arn)['EventSourceMappings']
        for mapping in existing_mappings:
            if mapping['EventSourceArn'].startswith(f"arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/stream/"):
                if mapping['EventSourceArn'] == current_stream_arn:
                    logger.info(f"Correct DynamoDB Stream trigger already exists for Lambda function {lambda_arn}")
                    return
                else:
                    # Delete the old mapping
                    lambda_client.delete_event_source_mapping(UUID=mapping['UUID'])
                    logger.info(f"Deleted old event source mapping for table {table_name}")

        # Create a new mapping
        lambda_client.create_event_source_mapping(
            EventSourceArn=current_stream_arn,
            FunctionName=lambda_arn,
            StartingPosition=DEFAULT_STREAM_STARTING_POSITION,
            BatchSize=DEFAULT_BATCH_SIZE,
            MaximumBatchingWindowInSeconds=5
        )
        logger.info(f"Created new DynamoDB Stream trigger for Lambda function {lambda_arn}")
    except Exception as e:
        logger.error(f"Error managing event source mapping: {str(e)}")

    logger.info(f"DynamoDB Stream trigger setup complete for table {table_name}")

def remove_dynamodb_trigger(table_name, region):
    dynamodb_client = boto3.client('dynamodb')
    lambda_client = boto3.client('lambda')
    sts_client = boto3.client('sts')

    # Get AWS account ID
    account_id = sts_client.get_caller_identity()["Account"]

    # Get the stream ARN
    try:
        response = dynamodb_client.describe_table(TableName=table_name)
        stream_arn = response['Table'].get('LatestStreamArn')
    except dynamodb_client.exceptions.ResourceNotFoundException:
        logger.info(f"Table {table_name} not found. No trigger to remove.")
        return

    if not stream_arn:
        logger.info(f"No stream found for table {table_name}. No trigger to remove.")
        return

    # List all event source mappings
    paginator = lambda_client.get_paginator('list_event_source_mappings')
    removed_count = 0

    for page in paginator.paginate():
        for mapping in page['EventSourceMappings']:
            # Check if the mapping is for our table's stream
            if mapping['EventSourceArn'].startswith(f"arn:aws:dynamodb:{region}:{account_id}:table/{table_name}/stream/"):
                try:
                    logger.info(f"Removing DynamoDB Stream trigger {mapping['UUID']} for table {table_name}")
                    lambda_client.delete_event_source_mapping(UUID=mapping['UUID'])
                    logger.info(f"Removed DynamoDB Stream trigger {mapping['UUID']} for table {table_name}")
                    removed_count += 1
                except lambda_client.exceptions.ResourceNotFoundException:
                    logger.warning(f"Event source mapping {mapping['UUID']} not found. It may have been deleted already.")
                except Exception as e:
                    logger.error(f"Error removing event source mapping {mapping['UUID']}: {str(e)}")

    if removed_count > 0:
        logger.info(f"Removed {removed_count} DynamoDB Stream trigger(s) for table {table_name}")
    else:
        logger.info(f"No DynamoDB Stream triggers found for table {table_name}")

def create_table(table_name, overwrite=False, lambda_arn=None):
    start_time = time.time()
    if overwrite:
        try:
            if lambda_arn:
                remove_dynamodb_trigger(table_name, lambda_arn)
            dynamodb_res.Table(table_name).delete()
            logger.info(f"Deleted existing table {table_name}")
            waiter = dynamodb_res.meta.client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
        except ClientError:
            pass
    else:
        existing_tables = dynamodb_res.meta.client.list_tables()['TableNames']
        if table_name in existing_tables:
            logger.error(f"Table {table_name} already exists. Use --overwrite to delete it first.")
            return None

    try:
        table = dynamodb_res.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'UserId', 'KeyType': 'HASH'},
                {'AttributeName': 'Email', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'UserId', 'AttributeType': 'S'},
                {'AttributeName': 'Email', 'AttributeType': 'S'},
                {'AttributeName': 'LastName', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'LastNameIndex',
                    'KeySchema': [
                        {'AttributeName': 'LastName', 'KeyType': 'HASH'},
                    ],
                    'Projection': {'ProjectionType': 'ALL'}
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            StreamSpecification={
                'StreamEnabled': True,
                'StreamViewType': 'NEW_AND_OLD_IMAGES'
            }
        )
        logger.info(f"Requested creation of {table_name}. Waiting for it to become active...")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        logger.info(f"Table {table_name} created successfully")
        
        # Enable point-in-time recovery
        logger.info(f"Enabling point-in-time recovery for table {table_name}...")
        enable_pitr(table_name)
        
        end_time = time.time()
        logger.info(f"Table {table_name} is now active with Streams and PITR enabled. Creation time: {end_time - start_time:.2f} seconds")
        return table
    except ClientError as e:
        logger.error(f"Error creating table: {e}")
        raise

def generate_user_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def maybe_none(chance_of_none=0.1):
    return None if random.random() < chance_of_none else True

def maybe_empty_list(chance_of_empty=0.1):
    return [] if random.random() < chance_of_empty else [fake.word() for _ in range(random.randint(1, 5))]

def generate_fake_person():
    person = {
        'UserId': generate_user_id(),
        'Email': fake.email(),
        'LastName': fake.last_name(),  # Ensure LastName is always present for the GSI
        'FirstName': fake.first_name() if maybe_none() else None,
        'Age': random.randint(18, 90) if maybe_none() else None,
        'Address': {
            'Street': fake.street_address() if maybe_none() else None,
            'City': fake.city() if maybe_none() else None,
            'ZipCode': fake.zipcode() if maybe_none() else None
        } if maybe_none() else None,
        'PhoneNumber': fake.phone_number() if maybe_none() else None,
        'Salary': Decimal(str(round(random.uniform(20000, 150000), 2))) if maybe_none() else None,
        'IsEmployed': random.choice([True, False, None]),
        'Hobbies': maybe_empty_list(),
        'Score': random.randint(0, 100) if maybe_none() else None,
        'DecimalValue': Decimal(str(round(random.uniform(0, 1), 4))) if maybe_none() else None,
        'BinaryData': b'binary data' if maybe_none() else None,
        'ListOfDicts': [
            {'Key': fake.word(), 'Value': random.randint(1, 100)}
            for _ in range(random.randint(0, 3))
        ],
        'DictWithList': {
            'ListKey': maybe_empty_list(),
            'OtherKey': random.randint(1, 10) if maybe_none() else None
        } if maybe_none() else None,
        'NullableRange': {
            'Min': random.randint(1, 50) if maybe_none() else None,
            'Max': random.randint(51, 100) if maybe_none() else None
        } if maybe_none() else None
    }
    # Remove any top-level null values
    return {k: v for k, v in person.items() if v is not None}

def upload_to_dynamodb(table, items):
    start_time = time.time()
    uploaded_ids = []
    failed_items = []
    
    for item in items:
        try:
            table.put_item(Item=item)
            uploaded_ids.append(item['UserId'])
            logger.info(f"Successfully uploaded record for UserId: {item['UserId']}")
        except ClientError as e:
            logger.error(f"Error uploading item: {e}")
            logger.error(f"Problematic item: {item}")
            failed_items.append(item)

    end_time = time.time()
    logger.info(f"Uploaded {len(uploaded_ids)} records to DynamoDB. Upload time: {end_time - start_time:.2f} seconds")
    logger.info(f"Uploaded UserIds: {', '.join(uploaded_ids)}")
    
    if failed_items:
        logger.error(f"Failed to upload {len(failed_items)} records.")
    else:
        logger.info("All records successfully uploaded.")
    
    return uploaded_ids, failed_items

def update_record(table, user_id, email, update_data):
    update_expression = "SET " + ", ".join(f"#{k}=:{k}" for k in update_data.keys())
    expression_attribute_names = {f"#{k}": k for k in update_data.keys()}
    expression_attribute_values = {f":{k}": v for k, v in update_data.items()}

    try:
        response = table.update_item(
            Key={'UserId': user_id, 'Email': email},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        print(f"Updated record for UserId: {user_id}, Email: {email}")
        print("Updated attributes:", response['Attributes'])
    except ClientError as e:
        print(f"Error updating record: {e}")

def delete_record(table, user_id, email):
    try:
        table.delete_item(Key={'UserId': user_id, 'Email': email})
        print(f"Deleted record for UserId: {user_id}, Email: {email}")
    except ClientError as e:
        print(f"Error deleting record: {e}")

def get_random_record(table, limit=RANDOM_SCAN_LIMIT):
    response = table.scan(Limit=limit)
    items = response.get('Items', [])
    if items:
        return random.choice(items)
    return None

def update_random_record(table,record):
    start_time = time.time()
    if record:
        user_id = record['UserId']
        email = record['Email']
        # Log the before state of the fields we're going to update
        logger.info(f"Updating record for UserId: {user_id}, Email: {email}, with starting values Age: {record.get('Age')}, IsEmployed: {record.get('IsEmployed')}, Score: {record.get('Score')}")

        update_data = {
            'Age': random.randint(18, 90),
            'IsEmployed': random.choice([True, False]),
            'Score': random.randint(0, 100)
        }
        update_expression = "SET " + ", ".join(f"#{k}=:{k}" for k in update_data.keys())
        expression_attribute_names = {f"#{k}": k for k in update_data.keys()}
        expression_attribute_values = {f":{k}": v for k, v in update_data.items()}

        try:
            response = table.update_item(
                Key={'UserId': user_id, 'Email': email},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW"
            )
            end_time = time.time()
            logger.info(f"Updated record for UserId: {user_id}, Email: {email}")
            logger.info(f"Updated attributes: {response['Attributes']}")
            logger.info(f"Update time: {end_time - start_time:.2f} seconds")
        except ClientError as e:
            logger.error(f"Error updating record: {e}")
    else:
        logger.warning("No records found to update.")

def delete_random_record(table, record):
    start_time = time.time()
    if record:
        user_id = record['UserId']
        email = record['Email']
        try:
            table.delete_item(Key={'UserId': user_id, 'Email': email})
            end_time = time.time()
            logger.info(f"Deleted record for UserId: {user_id}, Email: {email}")
            logger.info(f"Deletion time: {end_time - start_time:.2f} seconds")
        except ClientError as e:
            logger.error(f"Error deleting record: {e}")
    else:
        logger.warning("No records found to delete.")

def start_export_to_s3(table_name, s3_bucket, s3_prefix):
    timestamp = int(time.time())
    logger.info(f"Starting export to S3 for table: {table_name} to S3 bucket: {s3_bucket}, with prefix: {s3_prefix} in DYNAMODB_JSON format.")
    try:
        response = dynamodb_client.export_table_to_point_in_time(
            TableArn=f"arn:aws:dynamodb:{DEFAULT_REGION}:{boto3.client('sts').get_caller_identity().get('Account')}:table/{table_name}",
            S3Bucket=s3_bucket,
            S3Prefix=f"{s3_prefix}/{table_name}",
            ExportType='FULL_EXPORT',
            ExportFormat='DYNAMODB_JSON'
        )
        export_arn = response['ExportDescription']['ExportArn']
        logger.info(f"Export to S3 started. Export ARN: {export_arn}")
        return export_arn, timestamp
    except ClientError as e:
        logger.error(f"Error starting export to S3: {e}")
        return None

def monitor_export(export_arn, start_time):
    while True:
        try:
            response = dynamodb_client.describe_export(
                ExportArn=export_arn
            )
            status = response['ExportDescription']['ExportStatus']
            
            if status == 'IN_PROGRESS':
                logger.info("Export still in progress...")
                time.sleep(30)  # Wait for 30 seconds before checking again
            elif status == 'COMPLETED':
                logger.info("Export completed successfully!")
                logger.info(f"Export time: {time.time() - start_time:.2f} seconds")
                break
            else:
                logger.error(f"Unexpected export status: {status}")
                break
        except ClientError as e:
            logger.error(f"Error monitoring export: {e}")
            break

def table_has_items(table):
    """Check if a DynamoDB table has items."""
    try:
        response = table.scan(Limit=1)
        items = response.get('Items', [])
        if len(items) > 0:
            return True
        return False
    except ClientError as e:
        logger.error(f"Error checking item count for table: {e}")
        raise

def call_tinybird(url, method="GET", headers=None, params=None, files=None, data=None, ignore=None):
    token, host = get_tinybird_info()
    headers = headers or {"Authorization": f"Bearer {token}"}
    full_url = '/'.join([host, "v0", url])
    logger.info(f"Calling Tinybird API: {method} {full_url}")
    try:
        response = requests.request(method, full_url, headers=headers, params=params, files=files, data=data)
        ignore = ignore or []
        if response.status_code >= 400 and response.status_code not in ignore:
            logger.error(f"Tinybird returned an Error (Status {response.status_code}): {response.text}")
            response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to call Tinybird API: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        raise

def post_file_to_tinybird(file_name, file_content):
    url = f"datafiles?filenames={file_name}"
    file_data = {file_name: io.StringIO(file_content)}
    return call_tinybird(url, method="POST", files=file_data)

def populate_mv_pipe(mv_info):
    url = f"pipes/{mv_info['mv_pipe_name']}/nodes/mv/population"
    return call_tinybird(url, method="POST")

def describe_tinybird_table_schema(table_name):
    url = "datasources"
    response = call_tinybird(url, params={"attrs": "name,columns"})
    datasources = response.json().get("datasources", [])
    schema = next((ds for ds in datasources if ds['name'] == table_name), None)
    
    if not schema:
        logger.error(f"Table {table_name} not found in Tinybird datasources")
        return []
    
    return [{'name': col['name'], 'type': col['type']} for col in schema.get('columns', [])]

def create_mv_ds(mv_info):
    # Define the new Data Source with ReplacingMergeTree
    datasource_definition = f"""
SCHEMA >
    {mv_info['key_cols_schema']},
    `eventName` LowCardinality(String),
    `ApproximateCreationDateTime` Int64,
    `Keys` String,
    `NewImage` String,
    `OldImage` String

ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "{mv_info['key_cols_str']}"
ENGINE_VER "ApproximateCreationDateTime"
"""
    # Create the new Data Source
    logger.debug(f"Creating datasource {mv_info['mv_table_name']} with schema: {datasource_definition}")
    try:
        response = post_file_to_tinybird(f"{mv_info['mv_table_name']}.datasource", datasource_definition)
        logger.info(f"Successfully created datasource {mv_info['mv_table_name']}")
    except Exception as e:
        logger.error(f"Failed to create datasource {mv_info['mv_table_name']}: {str(e)}")
        return

def create_mv_write_pipe(mv_info):
    # Define the Pipe to create the Materialized View
    
    mv_pipe_definition = f"""
NODE mv
SQL >

    SELECT {mv_info['key_cols_str']}, eventName, ApproximateCreationDateTime, Keys, NewImage, OldImage
    FROM {mv_info['landing_table_name']}

TYPE materialized
DATASOURCE {mv_info['mv_table_name']}
"""
    response = post_file_to_tinybird(f"{mv_info['mv_pipe_name']}.pipe", mv_pipe_definition)
    logger.info(f"Materialized view pipe created successfully for table {mv_info['landing_table_name']}")
  
def create_mv_read_pipe(mv_info):
    # Create dynamic WHERE clauses for each key
    where_clauses = []
    for col in mv_info['schema']:
        if col['name'].startswith('key_'):
            where_clauses.append("{% if defined(" + col['name'] + ") %} AND " + col['name'] + " = {{" + col['type'] + "(" + col['name'] + ")}} {% end %}")

    where_clause = "\n".join(where_clauses)
    
    read_pipe_definition = f"""
TOKEN "{mv_info['read_pipe_name']}_endpoint_read_1234" READ

NODE endpoint
SQL >
%
SELECT {mv_info['key_cols_str']}, NewImage
FROM {mv_info['mv_table_name']} FINAL
WHERE eventName != 'REMOVE'
{where_clause}
"""
    response = post_file_to_tinybird(f"{mv_info['read_pipe_name']}.pipe", read_pipe_definition)
    logger.info(f"Read pipe created successfully for table {mv_info['landing_table_name']} with response {response.text}")

def main():
    parser = argparse.ArgumentParser(description="DynamoDB Streams CDC Test Table Manager")
    parser.add_argument("--create-table", action="store_true", help="Create a test DynamoDB table")
    parser.add_argument("--overwrite", action="store_true", help="Delete the test table if it exists before creating")
    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME, 
                        help=f"Name of the DynamoDB table (default: {DEFAULT_TABLE_NAME})")
    parser.add_argument("--region", default=DEFAULT_REGION, 
                        help=f"AWS region of the DynamoDB table (default: {DEFAULT_REGION})")
    parser.add_argument("--upload-batch", type=int, metavar="N",
                        help=f"Upload N fake records to the table")
    parser.add_argument("--modify-record", action="store_true", help="Modify a random record")
    parser.add_argument("--create-snapshot", action="store_true", help="Create a snapshot of the table")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, help=f"S3 bucket for export (default: {DEFAULT_S3_BUCKET})")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help=f"S3 prefix for export (default: {DEFAULT_S3_PREFIX})")
    parser.add_argument("--remove-record", action="store_true", help="Remove a random record")
    parser.add_argument("--lambda-arn", help="ARN of the Lambda function for DynamoDB streams")
    parser.add_argument("--create-trigger", action="store_true", help="Create DynamoDB Stream trigger for Lambda")
    parser.add_argument("--remove-trigger", action="store_true", help="Remove DynamoDB Stream trigger for Lambda")
    parser.add_argument("--create-mv", action="store_true", help="Create example Materialized View in Tinybird")
    parser.add_argument("--remove-mv", action="store_true", help="Remove example Materialized View in Tinybird")

    args = parser.parse_args()

    if args.create_table:
        table = create_table(args.table_name, args.overwrite, args.lambda_arn if args.overwrite else None)
    else:
        table = dynamodb_res.Table(args.table_name)

    if args.create_trigger:
        if not args.lambda_arn:
            logger.error("Lambda ARN is required to create a trigger")
        else:
            setup_dynamodb_trigger(args.table_name, args.lambda_arn, args.region)

    if args.remove_trigger:
        remove_dynamodb_trigger(args.table_name, args.region)

    if args.upload_batch is not None:
        batch = [generate_fake_person() for _ in range(args.upload_batch)]
        uploaded_ids, failed_items = upload_to_dynamodb(table, batch)
        logger.info(f"Successfully uploaded {len(uploaded_ids)} out of {args.upload_batch} fake records to DynamoDB.")
        
        if failed_items:
            logger.error(f"Failed to upload {len(failed_items)} records.")

    if args.modify_record or args.remove_record:
        record = get_random_record(table)
        if args.modify_record:
            update_random_record(table, record)

        if args.remove_record:
            delete_random_record(table, record)
    
    if args.create_snapshot:
        if not args.s3_bucket:
            logger.error("S3 bucket must be specified for export")
        else:
            if not table_has_items(table):
                logger.info(f"Table {args.table_name} is empty. Skipping export.")
            export_arn, start_time = start_export_to_s3(args.table_name, args.s3_bucket, args.s3_prefix)
            if export_arn:
                monitor_export(export_arn, start_time)
    
    if args.create_mv or args.remove_mv:
        if not args.table_name:
                logger.error("Table name must be specified to manage a Materialized View")
        landing_table_name = f"{DEFAULT_TINYBIRD_TABLE_PREFIX}{args.table_name}"
        schema = describe_tinybird_table_schema(landing_table_name)
        mv_info = {
            'landing_table_name': landing_table_name,
            'mv_table_name': f"mat_{landing_table_name}",
            'mv_pipe_name': f"mv_{landing_table_name}",
            'read_pipe_name': f"{landing_table_name}_by_Keys",
            'schema': schema,
            'key_cols_str': ", ".join([col['name'] for col in schema if col['name'].startswith('key_')]),
            'key_cols_schema': ',\n    '.join(f'`{col["name"]}` {col["type"]}' for col in schema if col["name"].startswith('key_'))
        }

        if args.remove_mv:
            # "https://api.tinybird.co/v0/pipes/:pipe/nodes/:node/materialization"
            # "https://api.tinybird.co/v0/datasources/:name?force=true"
            # "https://api.tinybird.co/v0/pipes/:pipe"
            urls = [
                f"pipes/{mv_info['mv_pipe_name']}",
                f"datasources/{mv_info['mv_table_name']}",
                f"pipes/{mv_info['read_pipe_name']}"
            ]
            [call_tinybird(url, method="DELETE", params={"force": "true"}, ignore=[404]) for url in urls]
        if args.create_mv:
            if schema:
                create_mv_ds(mv_info)
                create_mv_write_pipe(mv_info)
                # Run Populate on the Pipe
                _ = populate_mv_pipe(mv_info)
                logger.info(f"Materialized view pipe populated successfully for table {mv_info['landing_table_name']}")
                create_mv_read_pipe(mv_info)
                logger.info(f"Materialized view created successfully for table {mv_info['landing_table_name']}")
            else:
                logger.error(f"Failed to retrieve schema for table {mv_info['landing_table_name']}")

if __name__ == "__main__":
    main()