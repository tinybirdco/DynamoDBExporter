import json
import random
import string
import os
from decimal import Decimal
import boto3
from boto3.dynamodb.types import TypeSerializer

TABLE_NAME = "export_test"

serializer = TypeSerializer()

def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

def maybe_none(chance_of_none=0.1):
    """Return either a random string or None."""
    return None if random.random() < chance_of_none else random_string(5)

def maybe_empty_list(chance_of_empty=0.1):
    """Return either an empty list or a list with random strings."""
    return [] if random.random() < chance_of_empty else [random_string(5) for _ in range(random.randint(1, 5))]

def generate_test_data(output_directory, filename_prefix, num_records, records_per_file):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    count = 0
    file_count = 0
    output = {TABLE_NAME: []}

    for _ in range(num_records):
        count += 1
        data = {
            "id": random_string(10),
            "name": maybe_none(),
            "age": random.randint(20, 60),
            "decimal_value": Decimal(str(round(random.uniform(10, 100), 1))),
            "address": {
                "street": maybe_none(),
                "city": maybe_none(),
                "zipcode": maybe_none()
            },
            "is_active": random.choice([True, False]),
            "purchases": maybe_empty_list(),
            "list_of_dicts": [
                {"key1": maybe_none(), "key2": random.randint(1, 10)},
                {"key1": maybe_none(), "key2": random.randint(1, 10)}
            ],
            "dict_with_list": {
                "list_key": maybe_empty_list(),
                "other_key": random.randint(1, 10)
            },
            "loans_range_min": None if random.choice([True, False]) else random.randint(1000, 5000)
        }
        
        # Remove any top-level null values
        data = {k: v for k, v in data.items() if v is not None}
        
        # Use boto3's TypeSerializer to convert to DynamoDB format
        dynamodb_item = {k: serializer.serialize(v) for k, v in data.items()}
        
        output[TABLE_NAME].append({"PutRequest": {"Item": dynamodb_item}})
        
        # If count reaches the limit per file or it's the last item
        if count == records_per_file or _ == num_records - 1:
            file_name = os.path.join(output_directory, f"{filename_prefix}_{file_count}.json")
            with open(file_name, 'w') as f:
                json.dump(output, f)
            output[TABLE_NAME] = []
            count = 0
            file_count += 1

# This will generate 10,000 records split over multiple files
generate_test_data("chunks", "chunk", 10000, 25)