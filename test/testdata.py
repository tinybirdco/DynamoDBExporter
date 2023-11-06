import json
import random
import string
import os
from decimal import Decimal

def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))

def maybe_none(chance_of_none=0.1):
    """Return either a random string or None."""
    return None if random.random() < chance_of_none else random_string(5)

def maybe_empty_list(chance_of_empty=0.1):
    """Return either an empty list or a list with random strings."""
    return [] if random.random() < chance_of_empty else [{"S": random_string(5)} for _ in range(random.randint(1, 5))]

def generate_test_data(output_directory, filename_prefix, num_records, records_per_file):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    count = 0
    file_count = 0
    output = {"export_test": []}

    for _ in range(num_records):
        count += 1
        data = {
            "id": {"S": random_string(10)},
            "name": {"S": maybe_none()},
            "age": {"N": str(random.randint(20, 60))},
            "decimal_value": {"N": str(Decimal(random.randint(100, 1000)) / 10)},
            "address": {
                "M": {
                    "street": {"S": maybe_none()},
                    "city": {"S": maybe_none()},
                    "zipcode": {"S": maybe_none()}
                }
            },
            "is_active": {"BOOL": random.choice([True, False])},
            "purchases": {"L": maybe_empty_list()},
            "list_of_dicts": {"L": [
                {"M": {"key1": {"S": maybe_none()}, "key2": {"N": str(random.randint(1, 10))}}},
                {"M": {"key1": {"S": maybe_none()}, "key2": {"N": str(random.randint(1, 10))}}}
            ]},
            "dict_with_list": {"M": {
                "list_key": {"L": maybe_empty_list()},
                "other_key": {"N": str(random.randint(1, 10))}
            }},
            # Including a field that might be null
            "loans_range_min": {"NULL": True} if random.choice([True, False]) else {"N": str(random.randint(1000, 5000))}
        }
        output["export_test"].append({"PutRequest": {"Item": data}})
        
        # If count reaches the limit per file or it's the last item
        if count == records_per_file or _ == num_records - 1:
            file_name = os.path.join(output_directory, f"{filename_prefix}_{file_count}.json")
            with open(file_name, 'w') as f:
                f.write(json.dumps(output))
            output["export_test"] = []
            count = 0
            file_count += 1

# This will generate 10,000 records split over multiple files
generate_test_data("chunks", "chunk", 10000, 25)
