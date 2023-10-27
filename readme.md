# DynamoDB to Tinybird Exporter

## Overview

This repository contains tools and scripts designed to automate the process of exporting data from a DynamoDB table into an S3 bucket in NDJSON format. Once the data is exported, it then uses it to replace a named Datasource in Tinybird.

## How it Works

1. **DynamoDB Data Export**: The primary script, executed as an AWS Lambda function, scans the specified DynamoDB table.
2. **Data Transformation**: The DynamoDB item is converted to plain JSON.
3. **NDJSON Creation**: The transformed data is accumulated into an NDJSON formatted file.
4. **S3 Storage**: The NDJSON file is uploaded in chunks to a pre-defined S3 bucket.
5. **Tinybird Integration**: With the data in S3, then pushed in a *replace* operation to Tinybird.

The Lambda may be easily modified to handle multiple tables and Tinybird Datasources with the addition of a simple loop.

## Setting Up

This script is designed to be run as an AWS Lambda function. It requires a DynamoDB table, an S3 bucket, and a Tinybird Workspace. You will need rights to set IAM permissions in AWS and Admin the Tinybird Workspace if they are not already configured for you.

### S3 Configuration

In the S3 bucket:

- Set your bucket policy from `bucket_policy.json`, update the placeholder values as required.
- It is not necessary to open up the bucket to public access.


### Lambda Configuration
In the Lambda Configuration:

- Create an Environment Variable `TB_DS_ADMIN_TOKEN` with a valid Tinybird API token with rights to create and replace datasources.
- Set the Runtime to `Python 3.11`.
- Add the Access Policy in `lambda_policy.json` to the Lambda execution role, and update the placeholder values as required.

Inside the `lambda_function.py`:

- Set `DYNAMO_TABLE_NAME` to the name of your DynamoDB table.
- Configure `DYNAMO_REGION` to match the AWS region of your DynamoDB instance.
- Update `OUTPUT_BUCKET` with the name of your desired S3 bucket.
- Assign `OUTPUT_KEY` with the path and filename where the NDJSON file will be stored in S3.
- Update `TINYBIRD_TABLE_NAME` with the name you want for your Tinybird table.
- Set `DOWNLOAD_URL_EXPIRATION` if you want to to be other than 30mins

### Automate with a Cloud Trigger

To execute the Lambda function on a regular schedule:

1. Go to the AWS Lambda console and select your function.
2. In the designer section, click on `Add trigger`.
3. Choose `CloudWatch Events`.
4. Set up your desired schedule (e.g., every day, every hour, etc.).
5. Save your changes.

## Testing the Setup

We have provided some helper functions in the `/test` folder to generate a DynamoDB table with some nested dummy data suitable for testing.

To verify everything works:

1. **Create Test Table**: Use provided `create_table.sh` script to set up a `test_export` DynamoDB table.
2. **Generate Test Data**: Execute the `testdata.py` script to generate a subdir with about 10K items to be uploaded to DynamoDB.
3. **Upload Test Data**: Use the `uploadchunks.sh` script to upload the test data to DynamoDB.
4. **Configure the Lambda**: Set the `DYNAMO_TABLE_NAME` variable in the Lambda function to `test_export`.
5. **Run Lambda**: Manually trigger the Lambda function. This will export the data from the test table to S3.
6. **Tinybird Datasource Creation**: Download the NDJSON file from your S3 bucket, and import it into Tinybird using the UI to create the Datasource. This will allow Tinybird to infer the schema for the datasource to save you doing it manually.
7. **Test the Lambda**: Now you can test the Lambda again to check that it replaces the Datasource as expected.

## Conclusion

This setup allows for a streamlined process to move data from DynamoDB into Tinybird. Whether for testing or production, this pipeline ensures your data is where you need it, when you need it.