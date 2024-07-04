# TinyDynamoCDC: DynamoDB to Tinybird Change Data Capture

This README outlines the process of setting up a Change Data Capture (CDC) system from Amazon DynamoDB to Tinybird using AWS Lambda.


## Features
* Once configured, the Lambda will automatically forward any updates to the included DynamoDB tables to Tinybird Datasources matching the same name with an optional prefix. 
* The Tinybird Datasource will be automatically created with an appropriate schema. 
* You can trigger a DynamoDB Snapshot to (re)initialize the export, and the Streams configuration will keep it up to date.
* The Tinybird Datasource will automatically index the existing DynamoDB Primary Keys and Sorting Keys, the rest of each Record will be provided as plain JSON ready for further processing.
* You can deploy an example Materialized View to query the test data
* Structure: DynamoDB, IAM (Role and Policy) >> S3, Lambda, Secrets Manager >> Tinybird
* Management functions are provided in utils.py to cover a large number of deployment and operational requirements, including full deploy or teardown of a test case.

## Limitations

1. The DynamoDB Streams service only keeps events for 24hours. If this service is somehow down for longer than that, your data will be stale and you should create a new Snapshot to reinitialize the Tinybird table. Some users choose to do this nightly as a precautionary measure.
2. Binary data in the DynamoDB table is exported in base64 encoded form as we cannot have certainty that it will be String-like or some other format. The user may make determination in processing downstream.
3. We use the exportTime of the Export to S3 Point-in-time Backup from the DynamoDB table to simulate the ApproximateCreationDateTime you get from a DynamoDB Stream CDC event. This should ensure that any changes made after the Snapshot is issued will be applied in order to the destination Tinybird Datasource during deduplication.
4. Batch sizes are largely configurable in the Lambda Trigger for the number of DDBStreams events to collect in each Trigger, and the amount of time to wait before invoking if enough events haven't arrived. This should give users good control over latency vs cost efficiency during the CDC replication stage of the processing.
5. We do not automatically index DynamoDB GSIs. This is largely because they are not provided in the Stream event, and it would be an expensive operation to fetch the DynamoDB Table Description for them during every Invocation.
6. The example Materialized View is deliberately simple to aid education, please see [Tinybird docs](https://www.tinybird.co/docs) or [contact us](hi@tinybird.co) if you want to discuss more complex or high volume use cases.
7. We use Lambda Triggers to receive and process data from the DynamoDB Stream feature. As of writing this only works if all services are within the same AWS Account and Region.

## Manual Deployment

### 0. Create a Tinybird Workspace
1. You will need a Tinybird Workspace to ship the DynamoDB Data to, either an existing one or create a new one.
2. In that Workspace, go to the Tokens UI and find the `create datasource token`, this has the permissions to create and append data to Datasources that we'll need later.

#### Optional AWS Secret Manager
Later you will need to supply your Tinybird Token to the Lambda, you can either do this by putting it into AWS Secrets Manager or into an Environment Variable in the Lambda Configuration. If you want to use the Secret Manager option, it is good to do it at this stage so you have everything you need when it comes time to create the Lambda.

To create the secret:
1. Navigate in AWS to the Secrets Manager UI
2. Store a new Secret
3. Select Other Type of Secret
4. Set the Key as: `TB_CREATE_DS_TOKEN`
5. Set the Value as the Token from `create datasource token`which you copied from Tinybird; it is usually a long alphanumeric String starting with `p.`
6. On the next page, name the Secret something like `TinybirdAPIKey-test`. Consider using a meaningful prefix here so that you can easily set it in your Access Policy in later steps, something like `TinybirdAPIKey-` with any suffix would be fine.

### 1. Setup DynamoDB Table
1. Your DynamoDB table needs to be configured with both DDBStreams activated to send changes, and point-in-time recovery to allow Snapshots to be created. The utils script contains functions to help you with this.
2. DDBStreams should be turned on before you do an initial snapshot, otherwise you may miss any changes between the snapshot completion and streams activation. If you enable DDBStreams when you first create the table you should have no problems.

### 2. Create an S3 Bucket for your Snapshots
You need an S3 Bucket that the Snapshots can be stored in, it can be a separate bucket just for this purpose, or you can use a prefix in the Snapshot configuration (recommended anyway) to keep the data exports contained to one area of some other bucket. This bucket is not exposed publicly and is only used to get 'free' snapshots from DynamoDB without using a full Scan.

1. Create a bucket, retain the name for use in later configurations.

### 3. Create IAM Policy
Policy controls are required to allow the Role to take necessary actions within your account.
You can either use a set of standard AWS Policies, or create your own from the provided template.

#### Use default Policies
  * arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole,
  * arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess,
  * arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess,
  * arn:aws:iam::aws:policy/SecretsManagerReadWrite

Note that the provided utils script uses the Default AWS policies.

#### Create your own Policy
1. Navigate to IAM in the AWS Console.
2. Create a new Policy for the Lambda.
3. Use the [provided](DDBStreamCDC/lambda_policy.json) policy JSON as a starting point.
4. Update the policy with your specific S3 bucket name.
5. Update the policy with your Tinybird API Key Secret prefix or name, if using that feature.

### 4. Create IAM Role

1. Navigate to IAM in AWS Console.
2. Create a new role for Lambda.
3. Attach the Policy or Policies you have selected.
4. Retain the Policy name for when you create the Lambda.

### 5. Create Lambda Function

1. Go to AWS Lambda and create a new function.
2. Select 'Author from Scratch' option
2. Name it something memorable, such as "TinybirdDynamoCDC".
3. Choose Python 3.10 as the runtime.
4. Set the architecture to x86_64.
5. Set the timeout to 5 minutes.
6. Assign the IAM role created in step 1.
7. Use the code from TinyDynamoCDC.py for the function.
8. Deploy the function.

#### Configure Environment Variables
In the Lambda configuration, add these environment variables:

##### Required
The Lambda requires a Tinybird Token to push the data into your Workspace. It requires the `DATASOURCE:APPEND` and `DATASOURCE:CREATE` scopes. By default the `create datasource token` in Tinybird has these scopes already.
You have two options:
* TB_CREATE_DS_TOKEN: Set this environment variable with the Token directly if you wish. This will override the Secret option below if set. 

  **OR**
* TB_CREATE_DS_TOKEN_SECRET_NAME: The name of a Secret in AWS Secrets Manager which contains the necessary Token. This is the default and preferred behavior. We recommend you name it with a standard prefix like `TinybirdAPIKey-` to make it easy to set in your access policy for retrieval.

##### Optional
1. TB_DS_PREFIX: The prefix to apply to the DynamoDB table names when creating them in Tinybird, it defaults to `ddb_`
2. TB_API_ENDPOINT: The URL of your Tinybird Workspace, if different than the default. You can easily find this in the Add Datasource > Events API example in the UI. It defaults to `https://api.tinybird.co/v0/`
3. TB_SKIP_TABLE_CHECK: You can set this to True if you want the script to skip checking if the target Tinybird Datasource already exists. The script will create it with a good schema by default, but once it's running you may want to set this to skip the extra API call every invocation.
4. BATCH_SIZE: This sets the number of records to be micro-batched and sent to Tinybird. It defaults to 250 which is very conservative.
5. MAX_PAYLOAD_SIZE: Set this to control the maximum payload size sent to Tinybird. It defaults to 10Mb.
6. REGION: Set this to override the AWS Region used for processing if necessary.
7. LOGGING_LEVEL: Controls the detailed logging level of the Lambda. Defaults to `INFO`, can be set to `DEBUG` for more verbosity.
8. TB_CREATE_DS_TOKEN_SECRET_KEY_NAME: You can use this to override the name of the Key in the AWS Secret Manager that stores the Tinybird API Token. Defaults to `TB_CREATE_DS_TOKEN`.

### 6. Add S3 Trigger

1. In the Lambda function, add a new trigger.
2. Choose S3 as the source.
3. Select your S3 bucket.
4. Set the event type to "All object create events".
5. Add .gz as the suffix so non-data files do not trigger the lambda
6. Add the prefix `DDBStreamCDC` to collect all your exports into a common folder. Note that this prefix is also used in the utils script if you want to change it.
7. Set the Destination as your Lambda Function ARN.

### 7. Add DynamoDB Trigger(s)

1. Add another trigger to the Lambda function.
2. Choose DynamoDB as the source.
3. Select your DynamoDB table.
4. Configure the following settings:
    * Batch size: 250
    * Starting Position: Trim Horizon
    * Batch window: 5 seconds
    * Retry attempts: 5
    * Split batch on error: Yes
5. Repeat for each source DynamoDB Table to be replicated.

Note that the Lambda will send data whenever the first limit is breeched, which could be Batch Size, Batch Window, Data size over 6Mb or Lambda Timeout.
Reduce the Window for reduced latency at higher costs.

### 8. Create Initial Snapshot
1. Use the UI or the provided utils.py script to generate an initial snapshot of the DynamoDB table. This can also act as an initial deployment test.

### 10. [Optional] Set Up Alerting

1. Create an SNS Topic (e.g., "TinyDynamoCDC").
2. Add a subscription to your preferred endpoint (Email, another Tinybird table, etc.).
3. Create a CloudWatch Alarm:
    * Choose the Lambda function metric for Errors.
    * Set the statistic to Sum and the period to 5 minutes.
    * Set the condition to "Greater/Equal than 1".
    * For alarm state trigger, choose your SNS topic.

## Automated Deployment
We have spent time on a number of deployment and operational utilities to aid you in setting up this Integration within the utils.py script.
**These utilities are currently experimental, though quite straight forward in their operation. Please test them carefully before use.**

1. Clone this repo, and change directory into this sub-folder.
2. Get your User Admin Token from your Tinybird Workspace, and use it with `tb auth`.
3. Ensure you are authenticated with AWS CLI.
4. Install the requirements.txt within your Python interpreter or VirtualEnv.
5. Execute `python3 utils.py --help` to see the options.
6. Many defaults are set at the top of utils.py, or can be passed in as args.

```bash
DynamoDB Streams CDC Test Table Manager

options:
  -h, --help            show this help message and exit
  --create              Create the entire Test Infrastructure with default settings
  --remove              Remove the entire Test Infrastructure
  --overwrite           Delete and recreate objects if they already exist
  --create-snapshot     Create a snapshot of the table
  --upload-batch Int    Upload (Int) fake records to the table
  --modify-record       Modify a random record
  --remove-record       Remove a random record
  --create-ddb-table    Create a test DynamoDB table
  --remove-ddb-table    Remove a test DynamoDB table
  --create-ddb-trigger  Create DynamoDB Stream trigger for Lambda
  --remove-ddb-trigger  Remove DynamoDB Stream trigger for Lambda
  --create-s3-trigger   Create S3 trigger for Lambda
  --remove-s3-trigger   Remove S3 trigger for Lambda
  --create-lambda       Create Lambda function
  --remove-lambda       Remove Lambda function
  --update-lambda       Update Lambda function code
  --create-mv           Create example Materialized View in Tinybird
  --remove-mv           Remove example Materialized View in Tinybird
  --infer-schema        Infer schema for Materialized View from landing Datasource (default False)
  --table-name Str      Name of the DynamoDB table (default: PeopleTable)
  --region Str          AWS region of the DynamoDB table (default: eu-west-2)
  --s3-bucket Str       S3 bucket for export (default: tinybird-test-dynamodb-export)
  --s3-prefix Str       S3 prefix for export (default: DDBStreamCDC)
  --lambda-arn Str      ARN of the Lambda function for DynamoDB streams
  --lambda-name Str     Name of the Lambda function (default: DDBStreamCDC-LambdaFunction)
  --lambda-role Str     Name of the IAM role for Lambda (default: DDBStreamCDC-LambdaRole)
  --lambda-secret Str   Name of the secret for Tinybird API key (default: DDBStreamCDC-TinybirdSecret)
  --lambda-timeout Int  Timeout in seconds for the Lambda function (default: 300)
```

## Testing
After setting up the system, you can test it by:

* Ensuring your initial Snapshot finished in the DynamoDB > Tables > Exports and streams view
* Ensure your Lambda completed processing it by reviewing it's Cloudwatch Logs
* Making INSERT, MODIFY and REMOVE changes to your DynamoDB table.
* Checking the Tinybird table for the updates.
* Monitoring CloudWatch logs for any errors.
* Sending test events through the Lambda to check for error handling.

There are many functions to aid in this in the provided utils.py script

## Troubleshooting

* Check Lambda function logs in CloudWatch for detailed error messages.
* Ensure all IAM permissions are correctly set.
* Verify that the Tinybird API token has the necessary permissions.
