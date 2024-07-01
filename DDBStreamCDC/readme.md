# TinyDynamoCDC: DynamoDB to Tinybird Change Data Capture

This README outlines the process of setting up a Change Data Capture (CDC) system from Amazon DynamoDB to Tinybird using AWS Lambda.

Once configured, the Lambda will automatically forward any updates to the included DynamoDB tables to Tinybird Datasources matching the same name with an optional prefix. The Tinybird Datasource will be automatically created with an appropriate schema. You can create a DynamoDB Snapshot to (re)initialize the export, and the Streams configuration will keep it up to date.

## Limitations

The DynamoDB Streams service only keeps events for 24hours. If this service is somehow down for longer than that, your data will be stale and you should create a new Snapshot to reinitialize the Tinybird table. Some users choose to do this nightly as a precautionary measure.

## 0. Setup DynamoDB Table
1. Your DynamoDB table needs to be configured with both DDBStreams activated to send changes, and point-in-time recovery to allow Snapshots to be created.
2. DDBStreams should be turned on before you do an initial snapshot, otherwise you may miss any changes between the snapshot completion and streams activation.

## 1. Create a Bucket for your Snapshots
You need a Bucket that the Snapshots can be stored it, it can be a separate bucket or you can use a prefix in the Snapshot configuration (recommended anyway) to keep the data exports contained to one area. This bucket is not exposed publicly and is only used to get 'free' snapshots from DynamoDB without using a full Scan.

1. Create a bucket, retain the name for use in later configurations.

## 2. Create IAM Role

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

## 3. Create Lambda Function

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

1. TB_DS_ADMIN_TOKEN: Your Tinybird Data Source Admin Token (Create this in Tinybird). It requires the `DATASOURCE:APPEND` and `DATASOURCE:CREATE` scopes.
2. TB_DS_PREFIX: The prefix to apply to the DynamoDB table names when creating them in Tinybird, it defaults to `ddb_`
3. TB_API_ENDPOINT: The URL of your Tinybird Workspace, if different than the default. You can easily find this in the Add Datasource > Events API example in the UI. It defaults to `https://api.tinybird.co/v0/`

## 4. Add S3 Trigger

1. In the Lambda function, add a new trigger.
2. Choose S3 as the source.
3. Select your S3 bucket.
4. Set the event type to "All object create events".
5. Add .gz as the suffix so non-data files do not trigger the lambda
6. Add the prefix `DDBStreamCDC` to collect all your exports into a common folder. Note that this prefix is also used in the createSnapshot.sh script if you want to change it.
7. Set the Destination as your Lambda Function ARN.

## 5. Add DynamoDB Trigger(s)

1. Add another trigger to the Lambda function.
2. Choose DynamoDB as the source.
3. Select your DynamoDB table.
4. Configure the following settings:
    * Batch size: 100
    * Starting Position: Trim Horizon
    * Batch window: 5 seconds
    * Retry attempts: 5
    * Split batch on error: Yes
5. Repeat for each source DynamoDB Table to be replicated.

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
