# TinyDynamoCDC: DynamoDB to Tinybird Change Data Capture

This README outlines the process of setting up a Change Data Capture (CDC) system from Amazon DynamoDB to Tinybird using AWS Lambda.

Once configured the Lambda will automatically forward any updates to the DynamoDB table to the specified Tinybird Datasource.

## Limitations

The DynamoDB Streams service only keeps events for 24hours. If this service is somehow down for longer than that, your data will be stale and you should create a new Snapshot to reinitialize the Tinybird table. Some users choose to do this nightly as a precautionary measure.

## 0. Setup DynamoDB Table
1. Your DynamoDB table needs to be configured with both DDBStreams activated, and point-in-time recovery to allow Snapshots to be created.
2. DDBStreams should be turned on before you do an initial snapshot, otherwise you may miss any changes between the snapshot completion and streams activation.


## 1. Create IAM Role

1. Navigate to IAM in AWS Console.
2. Create a new role for Lambda.
3. Use the following policy JSON as a starting point:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::<bucket_name>",
                "arn:aws:s3:::<bucket_name>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:DescribeStream",
                "dynamodb:GetRecords",
                "dynamodb:GetShardIterator",
                "dynamodb:ListStreams"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/*/stream/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}

```
4. Update the policy with your specific S3 bucket name.

## 2. Create Lambda Function

1. Go to AWS Lambda and create a new function.
2. Name it "TinyDynamoCDC".
3. Choose Python 3.10 as the runtime.
4. Set the architecture to x86_64.
5. Set the timeout to 1 minute.
6. Assign the IAM role created in step 1.
7. Use the code from TinyDynamoCDC.py for the function.
8. Deploy the function.

### Configure Environment Variables
In the Lambda configuration, add these environment variables:

1. TB_DS_ADMIN_TOKEN: Your Tinybird Data Source Admin Token (Create this in Tinybird)
2. TB_DS_NAME: The name of your target table in Tinybird
3. TB_API_ENDPOINT: The URL of your Tinybird Workspace, if different than the default. You can easily find this in the Add Datasource > Events API example in the UI.

## 3. Add S3 Trigger

1. In the Lambda function, add a new trigger.
2. Choose S3 as the source.
3. Select your S3 bucket.
4. Set the event type to "All object create events".
5. Add .gz as the suffix so non-data files do not trigger the lambda
6. Add the prefix `DDBStreamCDC` to collect all your exports into a common folder. Note that this prefix is also used in the createSnapshot.sh script if you want to change it.
7. Set the Destination as your Lambda Function ARN.

## 4. Add DynamoDB Trigger

1. Add another trigger to the Lambda function.
2. Choose DynamoDB as the source.
3. Select your DynamoDB table.
4. Configure the following settings:
    * Batch size: 100
    * Batch window: 5 seconds
    * Retry attempts: 5
    * Split batch on error: Yes

## 5. Create Tinybird Landing Table

1. In Tinybird, use the landingSchema.datasource to create a new table.
2. Rename this table to match the TB_DS_NAME you set in the Lambda environment variables.
3. You can additionally use the matViewRMT and Target Pipe/Datasource to create your Upserted table.
4. you can then also use the querApiExample Pipe to get the latest values as an REST API Endpoint.

## 6. Set Up Alerting

1. Create an SNS Topic (e.g., "TinyDynamoCDC").
2. Add a subscription to your preferred endpoint (Email, another Tinybird table, etc.).
3. Create a CloudWatch Alarm:
    * Choose the Lambda function metric for Errors.
    * Set the statistic to Sum and the period to 5 minutes.
    * Set the condition to "Greater/Equal than 1".
    * For alarm state trigger, choose your SNS topic.

7. ## Create initial Snapshot
1. Use the UI or the provided createSnapshot.sh script to generate an initial snapshot of the DynamoDB table to Tinybird. This can also act as an initial deployment test.

## 7. Testing
After setting up the system, you can test it by:

* Making changes to your DynamoDB table.
* Checking the Tinybird table for updates.
* Monitoring CloudWatch logs for any errors.
* Sending test events through the Lambda to check for error handling.

There are several scripts in the test folder to assist you:
* createDdbTable.sh creates a dynamo DB table with a schema designed to test various common types and structures
* generateChunks.py batch-generates thousands of test rows which you can upload to seed the table
* testE2eLatency.py provides a basis to test how long it takes a single updated key to be served by Tinybird.

## Troubleshooting

* Check Lambda function logs in CloudWatch for detailed error messages.
* Ensure all IAM permissions are correctly set.
* Verify that the Tinybird API token has the necessary permissions.
