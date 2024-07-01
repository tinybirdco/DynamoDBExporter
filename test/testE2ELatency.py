import boto3
import requests
import time
from datetime import datetime, timezone
import random
from botocore.exceptions import ClientError

# AWS Configuration
AWS_REGION = "eu-west-2"  # Replace with your region
TABLE_NAME = "export_test"  # Replace with your DynamoDB table name

# Tinybird API Configuration
TINYBIRD_ENDPOINT = "https://api.tinybird.co/v0/pipes/latest_export_by_key.json"
TINYBIRD_TOKEN = ""
TEST_KEY = "user_age"
POLL_INTERVAL = .5  # seconds
MAX_WAIT = 300  # 5 minutes

if TINYBIRD_TOKEN == "":
    raise ValueError("Please provide a valid Tinybird token")

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(TABLE_NAME)

def get_random_key_from_tinybird():
    """Fetch all rows from Tinybird and select a random key."""
    params = {'token': TINYBIRD_TOKEN}
    response = requests.get(TINYBIRD_ENDPOINT, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        if data:
            random_item = random.choice(data)
            return random_item['id'], random_item['ApproximateCreationDateTime'], random_item[TEST_KEY]
    else:
        print(f"Failed to fetch data from Tinybird: {response.status_code} - {response.text}")
    return None, None, None

def update_dynamodb_item(key_value, starting_value):
    """Update a DynamoDB item with a new test key."""
    timestamp = datetime.now(timezone.utc)
    new_age = starting_value
    while new_age == starting_value:
        new_age = random.randint(10, 100)
    
    try:
        response = table.update_item(
            Key={'id': key_value},
            UpdateExpression="SET age = :val, update_timestamp = :ts",
            ExpressionAttributeValues={
                ':val': new_age,
                ':ts': timestamp.isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        
        # Check if the update was successful
        if 'Attributes' in response:
            updated_age = response['Attributes'].get('age')
            if updated_age == new_age:
                print(f"Successfully updated item. New age: {updated_age}")
                return timestamp.timestamp(), new_age
            else:
                print(f"Update may have failed. Expected age: {new_age}, Got: {updated_age}")
        else:
            print("Update failed. No 'Attributes' in the response.")
        
    except ClientError as e:
        print(f"An error occurred: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    
    return None, None

def format_timestamp(timestamp):
    """Convert a timestamp to a human-readable format."""
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def measure_latency():
    """Measure the latency of the CDC pipeline."""
    key_value, initial_timestamp, starting_value = get_random_key_from_tinybird()
    if not key_value:
        print("Failed to get a random key from Tinybird")
        return None
    
    print(f"Selected key: {key_value} with initial timestamp: {initial_timestamp} and initial value of {starting_value}")
    
    # Parse the initial_timestamp string to a datetime object
    initial_datetime = datetime.strptime(initial_timestamp, "%Y-%m-%d %H:%M:%S")
    
    print("Updating DynamoDB item...")
    update_timestamp, new_value = update_dynamodb_item(key_value, starting_value)
    if update_timestamp is None:
        print("Failed to update DynamoDB item. Aborting latency test.")
        return None
    
    update_time_str = format_timestamp(update_timestamp)
    print(f"Item updated at: {update_time_str} with new value: {new_value}")
    
    start_time = time.time()
    
    print("Polling Tinybird for update...")
    poll_count = 0
    while time.time() - start_time < MAX_WAIT:
        poll_count += 1
        current_time = format_timestamp(time.time())
        print(f"Poll {poll_count} at {current_time}")
        
        tinybird_data = get_tinybird_data(key_value)
        if tinybird_data:
            tinybird_timestamp = tinybird_data['ApproximateCreationDateTime']
            tinybird_age = int(tinybird_data.get(TEST_KEY))
            print(f"Tinybird timestamp: {tinybird_timestamp}, Tinybird age: {tinybird_age}")
            
            tinybird_datetime = datetime.strptime(tinybird_timestamp, "%Y-%m-%d %H:%M:%S")
            
            if tinybird_datetime > initial_datetime and tinybird_age == new_value:
                end_time = time.time()
                latency = end_time - start_time
                print(f"Update detected in Tinybird after {latency:.2f} seconds")
                print(f"Final Tinybird timestamp: {tinybird_timestamp}, age: {tinybird_age}")
                return latency
        else:
            print("No data returned from Tinybird for this key")
        
        time.sleep(POLL_INTERVAL)
    
    print(f"Timeout: Update not detected in Tinybird within {MAX_WAIT} seconds")
    return None

def get_tinybird_data(key_value):
    """Fetch data for a specific key from Tinybird."""
    params = {
        'token': TINYBIRD_TOKEN,
        'ddbid': key_value
    }
    response = requests.get(TINYBIRD_ENDPOINT, params=params)
    if response.status_code == 200:
        data = response.json()['data']
        if data:
            return data[0]
    return None

if __name__ == "__main__":
    latency = measure_latency()
    if latency is not None:
        print(f"CDC Pipeline Latency: {latency:.2f} seconds")
    else:
        print("Failed to measure latency")