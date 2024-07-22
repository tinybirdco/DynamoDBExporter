import boto3
import io
import json
from os import path, urandom
import random
import string
import requests
import argparse
from botocore.exceptions import ClientError
from boto3.dynamodb.types import Binary
import logging
import time
import zipfile
from faker import Faker
from decimal import Decimal

# Global dictionary to store clients and resources
clients = {}
resources = {}

def set_aws_region(region):
    global AWS_SESSION, AWS_REGION
    AWS_REGION = region
    AWS_SESSION = boto3.Session(region_name=AWS_REGION)

def get_client(service_name):
    if service_name not in clients:
        clients[service_name] = AWS_SESSION.client(service_name)
    return clients[service_name]

def get_resource(service_name):
    if service_name not in resources:
        resources[service_name] = AWS_SESSION.resource(service_name)
    return resources[service_name]

# Lazy initialization for Faker
_fake = None

def get_fake():
    global _fake
    if _fake is None:
        _fake = Faker()
    return _fake

# Table name and structure
DEFAULT_TABLE_NAME = "PeopleTable"
DEFAULT_REGION = "eu-west-2"
DEFAULT_S3_BUCKET = "tinybird-test-dynamodb-export"
DEFAULT_S3_PREFIX = "DDBStreamCDC"
DEFAULT_LAMBDA_ROLE_NAME = "DDBStreamCDC-LambdaRole"
DEFAULT_LAMBDA_FUNCTION_NAME = "DDBStreamCDC-LambdaFunction"
DEFAULT_SECRET_NAME = "DDBStreamCDC-TinybirdSecret"
DEFAULT_BATCH_SIZE = 500
DEFAULT_STREAM_STARTING_POSITION = "TRIM_HORIZON"
DEFAULT_TINYBIRD_TABLE_PREFIX = "ddb_"
RANDOM_SCAN_LIMIT = 50
DEFAULT_LAMBDA_TIMEOUT = 300
DEFAULT_BATCH_WINDOW = 5

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

def manage_aws_secret(secret_name, operation='create', overwrite=False):
    if operation == 'create':
        # Call Tinybird API to get the 'create datasource token'
        create_ds_token = get_create_datasource_token()
        try:
            get_client('secretsmanager').create_secret(
                Name=secret_name,
                SecretString=json.dumps({'TB_CREATE_DS_TOKEN': create_ds_token})
            )
            logger.info(f"Created secret: {secret_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceExistsException':
                if overwrite:
                    logger.warning(f"Secret {secret_name} already exists. Updating it.")
                    get_client('secretsmanager').update_secret(
                        SecretId=secret_name,
                        SecretString=json.dumps({'TB_CREATE_DS_TOKEN': create_ds_token})
                    )
                    logger.info(f"Updated secret: {secret_name}")
                else:
                    logger.warning(f"Secret {secret_name} already exists and overwrite is False. Skipping creation.")
            elif e.response['Error']['Code'] == 'InvalidRequestException' and 'already scheduled for deletion' in str(e):
                logger.warning(f"Secret {secret_name} was previously deleted. Attempting to restore.")
                try:
                    get_client('secretsmanager').restore_secret(SecretId=secret_name)
                    logger.info(f"Restored secret: {secret_name}")
                    # Update the restored secret with the new token
                    get_client('secretsmanager').update_secret(
                        SecretId=secret_name,
                        SecretString=json.dumps({'TB_CREATE_DS_TOKEN': create_ds_token})
                    )
                    logger.info(f"Updated restored secret: {secret_name}")
                except ClientError as restore_error:
                    logger.error(f"Failed to restore secret {secret_name}: {restore_error}")
                    raise
            else:
                raise
    elif operation == 'delete':
        try:
            get_client('secretsmanager').delete_secret(
                SecretId=secret_name,
                ForceDeleteWithoutRecovery=True
            )
            logger.info(f"Deleted secret: {secret_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.warning(f"Secret {secret_name} not found. Skipping deletion.")
            else:
                raise

def get_create_datasource_token():
    # Call Tinybird API to get tokens
    response = call_tinybird('tokens')
    tokens = response.json().get('tokens', [])
    
    # Filter for the 'create datasource token'
    create_ds_token = next((token['token'] for token in tokens if token['name'] == 'create datasource token'), None)
    
    if not create_ds_token:
        raise ValueError("Could not find 'create datasource token'")
    
    return create_ds_token

def create_lambda_role(role_name):
    logger.info(f"Creating IAM role: {role_name}")
    assume_role_policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "lambda.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    })

    try:
        response = get_client('iam').create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy_document
        )
        role_arn = response['Role']['Arn']
        logger.info(f"IAM role created: {role_arn}")

        # Attach necessary policies
        manage_iam_role_policies(role_name, 'create')

        # Wait for the role to be available
        waiter = get_client('iam').get_waiter('role_exists')
        waiter.wait(RoleName=role_name)

        return role_arn
    except get_client('iam').exceptions.EntityAlreadyExistsException:
        logger.warning(f"IAM role {role_name} already exists")
        return get_client('iam').get_role(RoleName=role_name)['Role']['Arn']

def create_lambda_zip(source_dir, output_filename):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipped:
        lambda_file = path.join(source_dir, 'lambda_function.py')
        if path.exists(lambda_file):
            zipped.write(lambda_file, 'lambda_function.py')
        else:
            logger.error(f"lambda_function.py not found in {source_dir}")
            raise FileNotFoundError(f"lambda_function.py not found in {source_dir}")

    logger.info(f"Created Lambda zip file: {output_filename}")

def remove_lambda_function(function_name, role_name, secret_name):
    # Delete the Lambda function
    try:
        get_client('lambda').delete_function(FunctionName=function_name)
        logger.info(f"Lambda function {function_name} deleted")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning(f"Lambda function {function_name} not found")
        else:
            logger.error(f"Error removing Lambda Function: {e}")

    # Delete the IAM role
    try:
        # Detach policies
        manage_iam_role_policies(role_name, 'delete')
        
        # Delete role
        get_client('iam').delete_role(RoleName=role_name)
        logger.info(f"IAM role {role_name} deleted")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntityException':
            logger.warning(f"IAM role {role_name} not found")
        else:
            logger.error(f"Error Deleting IAM Role: {e}")

    # Delete the secret
    manage_aws_secret(secret_name, 'delete')

def create_lambda_function(function_name, handler, runtime, role_name, code_dir, secret_name, env_vars=None, overwrite=False, timeout=300):
    # Create IAM role
    role_arn = create_lambda_role(role_name)

    # Prepare the Lambda function code
    zip_file = '/tmp/lambda_function.zip'
    create_lambda_zip(code_dir, zip_file)

    # Create or update the secret
    manage_aws_secret(secret_name, 'create')

    # Set up environment variables
    env_vars = env_vars or {}
    env_vars.update({
        'TB_CREATE_DS_TOKEN_SECRET_NAME': secret_name,
        'TB_SKIP_TABLE_CHECK': 'false',
        'LOGGING_LEVEL': 'INFO'
    })

    logger.info(f"Creating Lambda function: {function_name}")
    with open(zip_file, 'rb') as f:
        zipped_code = f.read()
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = get_client('lambda').create_function(
                FunctionName=function_name,
                Runtime=runtime,
                Role=role_arn,
                Handler=handler,
                Code=dict(ZipFile=zipped_code),
                Timeout=timeout,
                MemorySize=128,
                Environment={'Variables': env_vars}
            )
            logger.info(f"Lambda function created: {response['FunctionArn']}")
            return response['FunctionArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                if overwrite:
                    logger.warning(f"Lambda function {function_name} already exists. Updating it.")
                    return update_lambda_function(function_name, code_dir)
                else:
                    logger.warning(f"Lambda function {function_name} already exists and overwrite is False. Skipping creation.")
                    return get_lambda_arn(function_name)
            if e.response['Error']['Code'] == 'InvalidParameterValueException':
                if "The role defined for the function cannot be assumed by Lambda" in str(e) and attempt < max_retries - 1:
                    logger.warning(f"Role not ready, retrying in 10 seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(5)
            else:
                logger.error(f"Error creating Lambda function: {e}")
                raise

def update_lambda_function(function_name, code_dir):
    zip_file = '/tmp/lambda_function.zip'
    create_lambda_zip(code_dir, zip_file)

    with open(zip_file, 'rb') as f:
        zipped_code = f.read()

    try:
        response = get_client('lambda').update_function_code(
            FunctionName=function_name,
            ZipFile=zipped_code
        )
        logger.info(f"Lambda function {function_name} code updated")
        return response['FunctionArn']
    except ClientError as e:
        logger.error(f"Error updating Lambda function code: {e}")
        raise

def get_lambda_arn(function_name):
    try:
        response = get_client('lambda').get_function(FunctionName=function_name)
        return response['Configuration']['FunctionArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning(f"Lambda function {function_name} not found.")
        else:
            logger.error(f"Error getting Lambda function: {e}")
        return None

def manage_iam_role_policies(role_name, action='create'):
    policies = [
        'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
        'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess',
        'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess',
        'arn:aws:iam::aws:policy/SecretsManagerReadWrite'
    ]

    if action == 'create':
        for policy in policies:
            try:
                get_client('iam').attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy
                )
                logger.info(f"Attached policy {policy} to role {role_name}")
            except ClientError as e:
                logger.error(f"Error attaching policy {policy} to role {role_name}: {e}")
    elif action == 'delete':
        for policy in policies:
            try:
                get_client('iam').detach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy
                )
                logger.info(f"Detached policy {policy} from role {role_name}")
            except ClientError as e:
                logger.error(f"Error detaching policy {policy} from role {role_name}: {e}")

def enable_pitr(table_name, max_retries=5, initial_delay=1):
    for attempt in range(max_retries):
        try:
            get_resource('dynamodb').meta.client.update_continuous_backups(
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

def setup_dynamodb_trigger(table_name, lambda_arn):
    account_id = get_client('sts').get_caller_identity()["Account"]

    try:
        table_description = get_client('dynamodb').describe_table(TableName=table_name)
        stream_specification = table_description['Table'].get('StreamSpecification', {})
        
        if not stream_specification.get('StreamEnabled', False):
            logger.info(f"DynamoDB Streams not enabled for table {table_name}. Enabling now.")
            _ = get_client('dynamodb').update_table(
                TableName=table_name,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
            )
            logger.info(f"Enabled DynamoDB Streams for table {table_name}")
        
        # Re-fetch table description to get the latest stream ARN
        table_description = get_client('dynamodb').describe_table(TableName=table_name)
        current_stream_arn = table_description['Table'].get('LatestStreamArn')

        if not current_stream_arn:
            logger.error(f"Failed to get stream ARN for table {table_name}")
            return

        # Wait for the stream to be active
        waiter = get_client('dynamodb').get_waiter('table_exists')
        waiter.wait(TableName=table_name)

        # Verify stream status
        stream_description = get_client('dynamodbstreams').describe_stream(StreamArn=current_stream_arn)['StreamDescription']
        if stream_description['StreamStatus'] != 'ENABLED':
            logger.error(f"Stream for table {table_name} is not in ENABLED state. Current state: {stream_description['StreamStatus']}")
            return
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(f"Table {table_name} not found")
            return
        else:
            logger.error(f"Error setting up DynamoDB stream: {e}")
        return None

    # Check existing event source mappings
    try:
        existing_mappings = get_client('lambda').list_event_source_mappings(FunctionName=lambda_arn)['EventSourceMappings']
        for mapping in existing_mappings:
            if mapping['EventSourceArn'].startswith(f"arn:aws:dynamodb:{AWS_REGION}:{account_id}:table/{table_name}/stream/"):
                if mapping['EventSourceArn'] == current_stream_arn:
                    logger.info(f"Correct DynamoDB Stream trigger already exists for Lambda function {lambda_arn}")
                    return
                else:
                    # Delete the old mapping
                    get_client('lambda').delete_event_source_mapping(UUID=mapping['UUID'])
                    logger.info(f"Deleted old event source mapping for table {table_name}")

        # Create a new mapping
        get_client('lambda').create_event_source_mapping(
            EventSourceArn=current_stream_arn,
            FunctionName=lambda_arn,
            StartingPosition=DEFAULT_STREAM_STARTING_POSITION,
            BatchSize=DEFAULT_BATCH_SIZE,
            MaximumBatchingWindowInSeconds=DEFAULT_BATCH_WINDOW
        )
        logger.info(f"Created new DynamoDB Stream trigger for Lambda function {lambda_arn}")
    except Exception as e:
        logger.error(f"Error managing event source mapping: {str(e)}")

    logger.info(f"DynamoDB Stream trigger setup complete for table {table_name}")

def remove_dynamodb_trigger(table_name):
    account_id = get_client('sts').get_caller_identity()["Account"]
    # Get the stream ARN
    try:
        response = get_client('dynamodb').describe_table(TableName=table_name)
        stream_arn = response['Table'].get('LatestStreamArn')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning(f"Table {table_name} not found. No trigger to remove.")
            return
        else:
            logger.error(f"Error removing DynamoDB Trigger: {e}")
        return None

    if not stream_arn:
        logger.warning(f"No stream found for table {table_name}. No trigger to remove.")
        return

    # List all event source mappings
    paginator = get_client('lambda').get_paginator('list_event_source_mappings')
    removed_count = 0

    for page in paginator.paginate():
        for mapping in page['EventSourceMappings']:
            # Check if the mapping is for our table's stream
            if mapping['EventSourceArn'].startswith(f"arn:aws:dynamodb:{AWS_REGION}:{account_id}:table/{table_name}/stream/"):
                try:
                    logger.info(f"Removing DynamoDB Stream trigger {mapping['UUID']} for table {table_name}")
                    get_client('lambda').delete_event_source_mapping(UUID=mapping['UUID'])
                    logger.info(f"Removed DynamoDB Stream trigger {mapping['UUID']} for table {table_name}")
                    removed_count += 1
                except ClientError as e:
                    if e.response['Error']['Code'] == 'ResourceNotFoundException':
                        logger.warning(f"Event source mapping {mapping['UUID']} not found. It may have been deleted already.")
                    else:
                        logger.error(f"Error removing event source mapping {mapping['UUID']}: {str(e)}")

    if removed_count > 0:
        logger.info(f"Removed {removed_count} DynamoDB Stream trigger(s) for table {table_name}")
    else:
        logger.warning(f"No DynamoDB Stream triggers found for table {table_name}")

def setup_s3_trigger(bucket_name, lambda_arn, s3_prefix):
    # First, add permission to Lambda to allow S3 to invoke it
    try:
        get_client('lambda').add_permission(
            FunctionName=lambda_arn,
            StatementId=f'S3-Invoke-{bucket_name}',
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{bucket_name}'
        )
        logger.info(f"Added permission for S3 bucket {bucket_name} to invoke Lambda function {lambda_arn}")
    except get_client('lambda').exceptions.ResourceConflictException:
        logger.warning(f"Permission for S3 bucket {bucket_name} to invoke Lambda function {lambda_arn} already exists")

    # Now, set up the S3 event notification
    try:
        get_client('s3').put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                'LambdaFunctionConfigurations': [
                    {
                        'LambdaFunctionArn': lambda_arn,
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {
                                        'Name': 'prefix',
                                        'Value': s3_prefix
                                    },
                                    {
                                        'Name': 'suffix',
                                        'Value': '.gz'
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        )
        logger.info(f"Set up S3 event notification for bucket {bucket_name} to trigger Lambda function {lambda_arn}")
    except Exception as e:
        logger.error(f"Error setting up S3 event notification: {str(e)}")
        raise

def remove_s3_trigger(bucket_name, lambda_arn):
    # Remove the specific S3 event notification
    try:
        current_config = get_client('s3').get_bucket_notification_configuration(Bucket=bucket_name)
        
        # Remove ResponseMetadata if it exists
        current_config.pop('ResponseMetadata', None)
        
        # Filter out the Lambda configuration for the specified ARN
        if 'LambdaFunctionConfigurations' in current_config:
            new_lambda_configs = [
                config for config in current_config['LambdaFunctionConfigurations']
                if config['LambdaFunctionArn'] != lambda_arn
            ]
            
            if len(new_lambda_configs) < len(current_config['LambdaFunctionConfigurations']):
                # Update the configuration if we removed something
                if new_lambda_configs:
                    current_config['LambdaFunctionConfigurations'] = new_lambda_configs
                else:
                    current_config.pop('LambdaFunctionConfigurations', None)
                
                get_client('s3').put_bucket_notification_configuration(
                    Bucket=bucket_name,
                    NotificationConfiguration=current_config
                )
                logger.info(f"Removed S3 event notification for Lambda {lambda_arn} from bucket {bucket_name}")
            else:
                logger.warning(f"No S3 event notification found for Lambda {lambda_arn} in bucket {bucket_name}")
        else:
            logger.warning(f"No Lambda function configurations found for bucket {bucket_name}")
    
    except Exception as e:
        logger.error(f"Error modifying S3 event notification: {str(e)}")

    # Remove Lambda permission
    try:
        get_client('lambda').remove_permission(
            FunctionName=lambda_arn,
            StatementId=f'S3-Invoke-{bucket_name}'
        )
        logger.info(f"Removed permission for S3 bucket {bucket_name} to invoke Lambda function {lambda_arn}")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning(f"Permission for S3 bucket {bucket_name} to invoke Lambda function {lambda_arn} not found, skipping action.")
        else:
            logger.error(f"Error removing Lambda Permission for S3 Trigger: {str(e)}")

def manage_table(action, table_name, overwrite=False, lambda_arn=None):
    if action == 'remove':
        try:
            table = get_resource('dynamodb').Table(table_name)
            table.delete()
            logger.info(f"Requested deletion of table {table_name}, waiting for completion...")
            waiter = get_resource('dynamodb').meta.client.get_waiter('table_not_exists')
            waiter.wait(TableName=table_name)
            logger.info(f"Table {table_name} deleted successfully")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.warning(f"Table {table_name} not found. Skipping deletion.")
            else:
                logger.error(f"Error deleting table: {e}")

    if action == 'create':
        start_time = time.time()
        if overwrite:
            try:
                if lambda_arn:
                    remove_dynamodb_trigger(table_name, lambda_arn)
                get_resource('dynamodb').Table(table_name).delete()
                logger.info(f"Deleted existing table {table_name}")
                waiter = get_resource('dynamodb').meta.client.get_waiter('table_not_exists')
                waiter.wait(TableName=table_name)
            except ClientError:
                pass
        else:
            existing_tables = get_resource('dynamodb').meta.client.list_tables()['TableNames']
            if table_name in existing_tables:
                logger.error(f"Table {table_name} already exists. Use --overwrite to delete it first.")
                return None

        try:
            table = get_resource('dynamodb').create_table(
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
    return [] if random.random() < chance_of_empty else [get_fake().word() for _ in range(random.randint(1, 5))]

def generate_fake_person():
    fake = get_fake()
    
    def maybe_none(chance_of_none=0.1):
        return None if random.random() < chance_of_none else True

    def generate_binary_data():
        return Binary(b'\x00\x01' + urandom(random.randint(10, 20)))

    person = {
        'UserId': generate_user_id(),
        'Email': fake.email(),
        'LastName': fake.last_name(),
        'FirstName': fake.first_name() if maybe_none() else None,
        'Age': random.randint(18, 90) if maybe_none() else None,
        'Address': {
            'Street': fake.street_address() if maybe_none() else None,
            'City': fake.city() if maybe_none() else None,
            'State': fake.state_abbr() if maybe_none() else None,
            'ZipCode': fake.zipcode() if maybe_none() else None,
            'Country': fake.country() if maybe_none() else None
        } if maybe_none() else None,
        'PhoneNumber': fake.phone_number() if maybe_none() else None,
        'Salary': Decimal(str(random.randint(20000, 150000))) if maybe_none() else None,
        'IsEmployed': random.choice([True, False, None]),
        'Hobbies': set(fake.words(nb=random.randint(1, 5))) if maybe_none() else None,
        'Score': random.randint(0, 100) if maybe_none() else None,
        'DecimalValue': Decimal(str(random.randint(1, 10000)) + "." + str(random.randint(0, 9999)).zfill(4)) if maybe_none() else None,
        'BinaryData': generate_binary_data() if maybe_none() else None,
        'DateJoined': fake.date_time_this_decade().isoformat() if maybe_none() else None,
        'Tags': set(fake.words(nb=random.randint(1, 5))),
        'Preferences': {
            'Theme': random.choice(['Light', 'Dark', 'System']),
            'Notifications': random.choice([True, False]),
            'Language': fake.language_code()
        } if maybe_none() else None,
        'PerformanceRatings': [random.randint(1, 5) for _ in range(random.randint(0, 5))],
        'LastLogin': Decimal(str(int(time.time()))) if maybe_none() else None,
        'AccountStatus': random.choice(['Active', 'Inactive', 'Suspended', None]),
        'EmergencyContact': {
            'Name': fake.name(),
            'Relationship': fake.word(),
            'Phone': fake.phone_number()
        } if maybe_none(0.7) else None,
        'NullField': None,
        'EmptyString': '' if maybe_none(0.3) else fake.word(),
        'LargeNumber': Decimal(str(random.randint(1000000, 9999999999))) if maybe_none() else None,
        'SmallNumber': Decimal(str(random.randint(1, 1000)) + "." + str(random.randint(0, 9999)).zfill(4)) if maybe_none() else None,
        'BooleanList': [random.choice([True, False]) for _ in range(random.randint(0, 5))],
        'MixedTypeList': [
            random.choice([
                fake.word(),
                Decimal(str(random.randint(1, 100))),
                random.choice([True, False]),
                None
            ]) for _ in range(random.randint(0, 5))
        ],
        'NestedStructure': {
            'Level1': {
                'Level2': {
                    'Level3': fake.sentence() if maybe_none() else None
                } if maybe_none() else None
            } if maybe_none() else None
        } if maybe_none() else None,
        'Interests': set(fake.words(nb=random.randint(1, 5))) if maybe_none() else None,  # String Set
        'LuckyNumbers': set(Decimal(str(random.randint(1, 100))) for _ in range(random.randint(1, 5))) if maybe_none() else None,  # Number Set
        'Certificates': set(generate_binary_data() for _ in range(random.randint(1, 3))) if maybe_none() else None,  # Binary Set
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
        response = get_client('dynamodb').export_table_to_point_in_time(
            TableArn=f"arn:aws:dynamodb:{AWS_REGION}:{get_client('sts').get_caller_identity().get('Account')}:table/{table_name}",
            S3Bucket=s3_bucket,
            S3Prefix=f"{s3_prefix}",
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
            response = get_client('dynamodb').describe_export(
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
            if method == "POST" and "already exists" in response.text:
                logger.warning(f"Tinybird component already exists: {response.text}")
            else:
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

def create_mv_ds(mv_info, mv_schema):
    # Define the new Data Source with ReplacingMergeTree
    datasource_definition = f"""
SCHEMA >
    {mv_schema['key_cols_schema']},
    `eventName` LowCardinality(String),
    `ApproximateCreationDateTime` Int64,
    `Keys` String,
    `NewImage` String,
    `OldImage` String

ENGINE "ReplacingMergeTree"
ENGINE_SORTING_KEY "{mv_schema['key_cols_str']}"
ENGINE_VER "ApproximateCreationDateTime"
"""
    # Create the new Data Source
    logger.debug(f"Creating datasource {mv_info['mv_table_name']} with schema: {datasource_definition}")
    try:
        _ = post_file_to_tinybird(f"{mv_info['mv_table_name']}.datasource", datasource_definition)
        logger.info(f"Successfully created datasource {mv_info['mv_table_name']}")
    except Exception as e:
        logger.error(f"Failed to create datasource {mv_info['mv_table_name']}: {str(e)}")
        return

def create_mv_write_pipe(mv_info, mv_schema):
    # Define the Pipe to create the Materialized View
    
    mv_pipe_definition = f"""
NODE mv
SQL >

    SELECT {mv_schema['key_cols_str']}, eventName, ApproximateCreationDateTime, Keys, NewImage, OldImage
    FROM {mv_info['landing_table_name']}

TYPE materialized
DATASOURCE {mv_info['mv_table_name']}
"""
    _ = post_file_to_tinybird(f"{mv_info['mv_pipe_name']}.pipe", mv_pipe_definition)
    logger.info(f"Materialized view pipe created successfully for table {mv_info['landing_table_name']}")
  
def create_mv_read_pipe(mv_info, mv_schema):
    # Create dynamic WHERE clauses for each key
    where_clauses = []
    for col in mv_schema['schema']:
        if col['name'].startswith('key_'):
            where_clauses.append("{% if defined(" + col['name'] + ") %} AND " + col['name'] + " = {{" + col['type'] + "(" + col['name'] + ")}} {% end %}")

    where_clause = "\n".join(where_clauses)
    
    read_pipe_definition = f"""
TOKEN "{mv_info['read_pipe_name']}_endpoint_read_1234" READ

NODE endpoint
SQL >
%
SELECT {mv_schema['key_cols_str']}, NewImage
FROM {mv_info['mv_table_name']} FINAL
WHERE eventName != 'REMOVE'
{where_clause}
"""
    _ = post_file_to_tinybird(f"{mv_info['read_pipe_name']}.pipe", read_pipe_definition)
    logger.info(f"Read pipe created successfully for table {mv_info['landing_table_name']}")

def prepare_mv_info(table_name):
    landing_table_name = f"{DEFAULT_TINYBIRD_TABLE_PREFIX}{table_name}"
    return {
        'landing_table_name': landing_table_name,
        'mv_table_name': f"mat_{landing_table_name}",
        'mv_pipe_name': f"mv_{landing_table_name}",
        'read_pipe_name': f"{landing_table_name}_by_Keys"
    }

def prepare_mv_schema(landing_table_name, infer=False):
    if infer:
        schema = describe_tinybird_table_schema(landing_table_name)
        if schema:
            return {
                'schema': schema,
                'key_cols_str': ", ".join([col['name'] for col in schema if col['name'].startswith('key_')]),
                'key_cols_schema': ',\n    '.join(f'`{col["name"]}` {col["type"]}' for col in schema if col["name"].startswith('key_'))
            }
        else:
            logger.error(f"Failed to infer schema for table {landing_table_name}")
            return None
    else:
        # This schema is matched to the records we generate for the example PeopleTable
        # It is faster to skip inference when setting up a test
        return {
                'schema': [
                    {'name': 'key_UserId', 'type': 'String'},
                    {'name': 'key_Email', 'type': 'String'}
                ],
                'key_cols_str': "key_UserId, key_Email",
                'key_cols_schema': "`key_UserId` String,\n    `key_Email` String"
            }

def remove_mv_from_tinybird(mv_info):
    urls = [
        f"pipes/{mv_info['mv_pipe_name']}",
        f"datasources/{mv_info['mv_table_name']}",
        f"pipes/{mv_info['read_pipe_name']}",
        f"datasources/{mv_info['landing_table_name']}"
    ]
    for url in urls:
        response = call_tinybird(url, method="DELETE", params={"force": "true"}, ignore=[404])
        if response.status_code == 404:
            logger.warning(f"Tinybird component not found: {url}")
        else:
            logger.info(f"Removed Tinybird component: {url}")

def create_mv_in_tinybird(mv_info, mv_schema):
    create_mv_ds(mv_info, mv_schema)
    create_mv_write_pipe(mv_info, mv_schema)
    # Run Populate on the Pipe
    _ = populate_mv_pipe(mv_info)
    logger.info(f"Materialized view pipe populated successfully for table {mv_info['landing_table_name']}")
    create_mv_read_pipe(mv_info, mv_schema)
    logger.info(f"Materialized View created successfully for table {mv_info['landing_table_name']}")

def check_ddb_table_in_region(table_name):
    """
    Check if the specified DynamoDB table exists in the current region.
    
    :param table_name: Name of the DynamoDB table
    :return: True if table exists in current region, False otherwise
    """
    try:
        dynamodb = get_client('dynamodb')
        dynamodb.describe_table(TableName=table_name)
        logger.info(f"Table '{table_name}' found in the current region ({AWS_REGION}).")
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        logger.error(f"Table '{table_name}' not found in the current region ({AWS_REGION}), please check your Region or create the Table before using this function.")
        return False
    except Exception as e:
        logger.error(f"Error checking table '{table_name}' in region {AWS_REGION}: {str(e)}")
        return False

def check_s3_in_region(bucket_name):
    """
    Check if the specified S3 bucket exists in the current region.
    
    :param bucket_name: Name of the S3 bucket
    :return: True if bucket exists in current region, False otherwise
    """
    try:
        s3 = get_client('s3')
        # Also check if the bucket is in the current region
        bucket_region = s3.get_bucket_location(Bucket=bucket_name).get('LocationConstraint', AWS_REGION)
        if bucket_region != AWS_REGION:
            logger.error(f"Bucket '{bucket_name}' found in a different region ({bucket_region}) than the current region ({AWS_REGION}).")
            return False
        logger.info(f"Bucket '{bucket_name}' found in this region ({AWS_REGION}).")
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.error(f"Bucket '{bucket_name}' not found in the current region ({AWS_REGION}), please check your Region or create the Bucket before using this function.")
        else:
            logger.error(f"Error checking bucket '{bucket_name}' in region {AWS_REGION}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error checking bucket '{bucket_name}' in region {AWS_REGION}: {str(e)}")
        return False

def manage_infrastructure(args):
    # Use provided values or defaults
    table_name = args.table_name or DEFAULT_TABLE_NAME
    s3_bucket = args.s3_bucket or DEFAULT_S3_BUCKET
    s3_prefix = args.s3_prefix or DEFAULT_S3_PREFIX
    lambda_name = args.lambda_name or DEFAULT_LAMBDA_FUNCTION_NAME
    lambda_role = args.lambda_role or DEFAULT_LAMBDA_ROLE_NAME
    secret_name = args.lambda_secret or DEFAULT_SECRET_NAME

    if args.remove or (args.create and args.overwrite):
        logger.info("Starting infrastructure teardown...")
        # 1. Remove Tinybird Materialized View
        logger.info("Removing Materialized View from Tinybird...")
        mv_info = prepare_mv_info(table_name)
        remove_mv_from_tinybird(mv_info)

        # Get Lambda ARN if not provided
        lambda_arn = args.lambda_arn or get_lambda_arn(lambda_name)

        if lambda_arn:
            logger.info("Removing Lambda and Triggers from AWS...")
            # 2. Remove S3 trigger
            remove_s3_trigger(s3_bucket, lambda_arn)
            
            # 3. Remove DynamoDB Stream trigger
            remove_dynamodb_trigger(table_name)
            
            # 4. Remove Lambda function
            remove_lambda_function(lambda_name, lambda_role, secret_name)
        else:
            logger.warning(f"Lambda function {lambda_name} not found. Skipping Lambda-related removals.")

        logger.info("Removing DynamoDB table...")
        # 5. Remove DynamoDB table
        manage_table('remove', table_name)
        
        logger.info("Infrastructure teardown complete.")

    if args.create:
        logger.info("Starting infrastructure setup...")
        # 0. Check if the s3 bucket exists in the target region
        if not check_s3_in_region(s3_bucket):
            logger.error(f"S3 Bucket '{s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated. Skipping further setup.")
            return
        # 1. Create DynamoDB table
        logger.info("Creating DynamoDB table...")
        _ = manage_table('create', table_name, args.overwrite)
        if not check_ddb_table_in_region(table_name):
            logger.error(f"DynamoDB Table '{table_name}' not found in the current region. Skipping further setup.")
            return
        
        # 2. Create Lambda function
        logger.info("Creating Lambda function with default role and policies...")
        lambda_arn = create_lambda_function(
            lambda_name,
            'lambda_function.lambda_handler',
            'python3.10',
            lambda_role,
            path.dirname(path.abspath(__file__)),
            secret_name,
            timeout=args.lambda_timeout
        )
        
        # 3. Set up DynamoDB Stream trigger
        logger.info("Setting up DynamoDB Stream trigger for Lambda...")
        setup_dynamodb_trigger(table_name, lambda_arn)
        
        # 4. Set up S3 trigger
        logger.info("Setting up S3 trigger for Lambda...")
        setup_s3_trigger(s3_bucket, lambda_arn, s3_prefix)
        
        # 5. Create Tinybird Materialized View (directed pathway)
        logger.info("Creating Materialized View in Tinybird...")
        mv_info = prepare_mv_info(table_name)
        if not describe_tinybird_table_schema(mv_info['landing_table_name']):
            logger.warning("Landing table not yet found in Tinybird. Skipping Materialized View setup.")
            return
        mv_schema = prepare_mv_schema(mv_info['landing_table_name'], infer=False)
        create_mv_in_tinybird(mv_info, mv_schema)
        
        logger.info("Infrastructure setup complete. Try sending some data to the table!")

def main():
    parser = argparse.ArgumentParser(description="DynamoDB Streams CDC Test Table Manager")
    parser.add_argument("--create", action="store_true", help="Create the entire Test Infrastructure with default settings")
    parser.add_argument("--remove", action="store_true", help="Remove the entire Test Infrastructure")
    parser.add_argument("--overwrite", action="store_true", help="Delete and recreate objects if they already exist")
    parser.add_argument("--create-snapshot", action="store_true", help="Create a snapshot of the table")

    parser.add_argument("--upload-batch", type=int, metavar="Int",help=f"Upload (Int) fake records to the table")
    parser.add_argument("--modify-record", action="store_true", help="Modify a random record")
    parser.add_argument("--remove-record", action="store_true", help="Remove a random record")

    parser.add_argument("--create-ddb-table", action="store_true", help="Create a test DynamoDB table")
    parser.add_argument("--remove-ddb-table", action="store_true", help="Remove a test DynamoDB table")
    parser.add_argument("--create-ddb-trigger", action="store_true", help="Create DynamoDB Stream trigger for Lambda")
    parser.add_argument("--remove-ddb-trigger", action="store_true", help="Remove DynamoDB Stream trigger for Lambda")
    parser.add_argument("--create-s3-trigger", action="store_true", help="Create S3 trigger for Lambda")
    parser.add_argument("--remove-s3-trigger", action="store_true", help="Remove S3 trigger for Lambda")
    parser.add_argument("--create-lambda", action="store_true", help="Create Lambda function")
    parser.add_argument("--remove-lambda", action="store_true", help="Remove Lambda function")
    parser.add_argument("--update-lambda", action="store_true", help="Update Lambda function code")
    parser.add_argument("--create-mv", action="store_true", help="Create example Materialized View in Tinybird")
    parser.add_argument("--remove-mv", action="store_true", help="Remove example Materialized View in Tinybird")
    parser.add_argument("--infer-schema", action="store_true", help="Infer schema for Materialized View from landing Datasource (default False)")

    parser.add_argument("--table-name", default=DEFAULT_TABLE_NAME, metavar="Str", help=f"Name of the DynamoDB table (default: {DEFAULT_TABLE_NAME})")
    parser.add_argument("--region", default=DEFAULT_REGION, metavar="Str", help=f"AWS region of the DynamoDB table (default: {DEFAULT_REGION})")
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, metavar="Str", help=f"S3 bucket for export (default: {DEFAULT_S3_BUCKET})")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, metavar="Str", help=f"S3 prefix for export (default: {DEFAULT_S3_PREFIX})")
    parser.add_argument("--lambda-arn", metavar="Str", help="ARN of the Lambda function for DynamoDB streams")
    parser.add_argument("--lambda-name", default=DEFAULT_LAMBDA_FUNCTION_NAME, metavar="Str", help=f"Name of the Lambda function (default: {DEFAULT_LAMBDA_FUNCTION_NAME})")
    parser.add_argument("--lambda-role", default=DEFAULT_LAMBDA_ROLE_NAME, metavar="Str", help=f"Name of the IAM role for Lambda (default: {DEFAULT_LAMBDA_ROLE_NAME})")
    parser.add_argument("--lambda-secret", default=DEFAULT_SECRET_NAME, metavar="Str", help=f"Name of the secret for Tinybird API key (default: {DEFAULT_SECRET_NAME})")
    parser.add_argument("--lambda-timeout", type=int, default=DEFAULT_LAMBDA_TIMEOUT, metavar="Int", help=f"Timeout in seconds for the Lambda function (default: {DEFAULT_LAMBDA_TIMEOUT})")

    args = parser.parse_args()
    # Set AWS Region for all client connections
    set_aws_region(args.region)

    if args.create or args.remove:
        manage_infrastructure(args)
    else:

        if args.remove_ddb_table:
            manage_table('remove', args.table_name)

        if args.create_ddb_table:
            if not check_s3_in_region(args.s3_bucket):
                logger.error(f"S3 Bucket '{args.s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated. Skipping further setup.")
                return
            table = manage_table('create', args.table_name, args.overwrite, args.lambda_arn if args.overwrite else None)
        else:
            table = get_resource('dynamodb').Table(args.table_name)

        if args.remove_ddb_trigger:
            remove_dynamodb_trigger(args.table_name)

        if args.create_ddb_trigger:
            if not args.lambda_arn:
                logger.error("Lambda ARN is required to create a trigger")
            else:
                if not check_s3_in_region(args.s3_bucket):
                    logger.error(f"S3 Bucket '{args.s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated. Skipping further setup.")
                    return
                if not check_ddb_table_in_region(args.table_name):
                    logger.error(f"DynamoDB Table '{args.table_name}' not found in the current region, Lambda trigger would fail. Skipping trigger setup.")
                    return
                setup_dynamodb_trigger(args.table_name, args.lambda_arn)

        if args.remove_s3_trigger:
            if not args.lambda_arn or not args.s3_bucket:
                logger.error("Lambda ARN and S3 bucket must be specified to remove an S3 trigger")
            else:
                remove_s3_trigger(args.s3_bucket, args.lambda_arn)

        if args.create_s3_trigger:
            if not args.lambda_arn or not args.s3_bucket:
                logger.error("Lambda ARN and S3 bucket must be specified to create an S3 trigger")
            else:
                if not check_s3_in_region(args.s3_bucket):
                    logger.error(f"S3 Bucket '{args.s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated. Skipping further setup.")
                    return
                if not check_ddb_table_in_region(args.table_name):
                    logger.error(f"DynamoDB Table '{args.table_name}' not found in the current region, Lambda trigger would fail. Skipping trigger setup.")
                    return
                setup_s3_trigger(args.s3_bucket, args.lambda_arn, args.s3_prefix)

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
                if not check_s3_in_region(args.s3_bucket):
                    logger.warning(f"S3 Bucket '{args.s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated.")
                export_arn, start_time = start_export_to_s3(args.table_name, args.s3_bucket, args.s3_prefix)
                if export_arn:
                    monitor_export(export_arn, start_time)
        
        if args.create_mv or args.remove_mv:
            if not args.table_name:
                    logger.error("Table name must be specified to manage a Materialized View")        
            mv_info = prepare_mv_info(args.table_name)
            if args.remove_mv:
                remove_mv_from_tinybird(mv_info)
            if args.create_mv:
                if mv_schema:= prepare_mv_schema(mv_info['landing_table_name'], infer=args.infer_schema):
                    create_mv_in_tinybird(mv_info, mv_schema)
                else:
                    logger.error(f"Failed to retrieve schema for table {mv_info['landing_table_name']} so could not create Materialized View.")

        if args.remove_lambda:
            if not args.lambda_name:
                logger.error("Lambda name must be specified to remove the function")
            else:
                if get_lambda_arn(args.lambda_name):
                    logger.info(f"Removing Lambda function {args.lambda_name}...")
                    remove_lambda_function(args.lambda_name, args.lambda_role, args.lambda_secret)
                else:
                    logger.info(f"Lambda function {args.lambda_name} not found to remove.")

        if args.create_lambda:
            if not check_s3_in_region(args.s3_bucket):
                logger.error(f"S3 Bucket '{args.s3_bucket}' not found in the current region, and Lambda/Table/Bucket must be colocated. Skipping further setup.")
                return
            if not check_ddb_table_in_region(args.table_name):
                logger.warning(f"DynamoDB Table '{args.table_name}' not found in the current region, Lambda triggers require it to be in the same region.")
                return
            lambda_arn = create_lambda_function(
                args.lambda_name,
                'lambda_function.lambda_handler',
                'python3.10',
                args.lambda_role,
                path.dirname(path.abspath(__file__)),  # Current directory
                args.lambda_secret,
                timeout=args.lambda_timeout
            )
            logger.info(f"Created Lambda function: {lambda_arn}")
        
        if args.update_lambda:
            if not args.lambda_name:
                logger.error("Lambda name must be specified to update the function")
            else:
                if lambda_arn := get_lambda_arn(args.lambda_name):
                    logger.info(f"Updating Lambda function {args.lambda_name}...")
                    update_lambda_function(args.lambda_name, path.dirname(path.abspath(__file__)))
                else:
                    logger.info(f"Lambda function {args.lambda_name} not found to update.")


if __name__ == "__main__":
    main()