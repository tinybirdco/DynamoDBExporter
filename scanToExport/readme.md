# DynamoDB to Tinybird Exporter

## Overview

This folder contains tools and scripts designed to help in automating the process of exporting data from DynamoDB table(s) into an S3 bucket in NDJSON format. Once the data is exported, it then uses it to replace matching named Datasource(s) in Tinybird, e.g. `mytable` in DynamoDB will replace `mytable` in Tinybird. There is an additional test script to generate a DynamoDB table with some nested dummy data suitable for testing.

This is provided as an example of how to automate the process of pushing data from DynamoDB into Tinybird. It is not intended to be a production-ready solution, but rather a starting point for your own implementation. Please note that this tool is not guaranteed nor officially supported by Tinybird.

This process uses a DynamoDB Scan, and then the Tinybird Replace function. Be mindful of costs.

## How it Works

1. **DynamoDB Data Export**: The primary script, executed as an AWS Lambda function, scans the specified DynamoDB table(s).
2. **Data Transformation**: Each DynamoDB item is converted from DynamoDB notation to plain JSON.
3. **NDJSON Creation**: The transformed data is accumulated into an NDJSON formatted file.
4. **S3 Storage**: The NDJSON file is uploaded in chunks to a pre-defined S3 bucket.
5. **Tinybird Integration**: With the data in S3, then pushed in a **replace** operation to Tinybird.

## Setting Up

This script is designed to be run as an AWS Lambda function. It requires at least one target DynamoDB table, an S3 bucket, and a Tinybird Workspace. 

You will need rights to set IAM permissions in AWS and Admin the Tinybird Workspace if they are not already configured for you.

You are advised to run the Lambda at least once to test it is writing expected output to your S3 file. You can then download that file and import it into Tinybird to create the Datasource with your preferred schema using the schema inference tooling. Once you have done this, you can then configure the Lambda to regularly replace the Datasource in Tinybird automatically.

### S3 Configuration

In the S3 bucket:

- Set your bucket policy from `bucket_policy.json`, update the placeholder values as required.
- It is **not** necessary to open up the bucket to public access.
- Your security posture is that a time-limited pre-signed URL may be used to download the NDJSON file(s) from the bucket, this signed URI will be pushed to Tinybird as using an API command which will then launch an ingest job to load the data into the Datasource.

### Lambda Configuration
In the Lambda Configuration:

- Set the Runtime to `Python 3.11`.
- Set the Timeout to suit your table size. For a table with 10K simple items, 30 seconds is observed to be sufficient; A table with 100K complex items took 5 mins.
- Set the maximum Memory to suit your table size. 256MB was observed to be sufficient in our tests at 10K and 100K items.
- Add the Access Policy in `lambda_policy.json` to the Lambda execution role, and update the placeholder values as required.
- Create the mandatory Environment Variables to control the Lambda's behavior
    - `TB_DS_ADMIN_TOKEN` with a valid Tinybird API token with rights to replace datasources.
    - `DDB_TABLES_TO_EXPORT` with a comma-separated list of DynamoDB tables to export.
    - `S3_BUCKET_NAME` with the name of the S3 bucket to use for the NDJSON file(s).
- You may also create these optional Environment Variables to control other behavior:
    - `DDB_REGION` with the AWS region of your DynamoDB instance, if different from the Lambda's region.
    - `TINYBIRD_API_ENDPOINT` with the Tinybird API endpoint to use, if different from the default of `api.tinybird.co`.
    - `DOWNLOAD_URL_EXPIRATION` with the expiration time for the pre-signed URL, in seconds. Default is 30 minutes.
    - `NULL_VALUE` with the value to use for null values in the NDJSON file. Default is an empty string as ClickHouse prefers this to `null` values.

### Automate with a Cloud Trigger

You can trigger the Lambda manually by submitting an empty event `{}`, but it is more useful to automate the process with a CloudWatch Event.

To execute the Lambda function on a regular schedule:

1. Go to the AWS Lambda console and select your function.
2. In the designer section, click on `Add trigger`.
3. Choose `CloudWatch Events`.
4. Set up your desired schedule (e.g., every day, every hour, etc.).
5. Save your changes.

## Testing the Setup

We have provided some helper functions in the `/test` folder to generate a DynamoDB table with some nested dummy data suitable for testing.

**Ensure you have authenticated in AWSCLI so these commands work**

To verify everything works:

1. **Create Test Table**: Use provided `create_table.sh` script to set up a `export_test` DynamoDB table.
2. **Generate Test Data**: Execute the `testdata.py` script to generate a subdir with about 10K items to be uploaded to DynamoDB.
3. **Upload Test Data**: Use the `uploadchunks.sh` script to upload the test data to DynamoDB.
4. **Check the Upload**: You can use the command `aws dynamodb scan --table-name export_test --limit 1` to check that some rows were successfully loaded into the table.
5. **Configure the Lambda**: Set the `DDB_TABLES_TO_EXPORT` Environment Variable in the Lambda configuration to `export_test`.
6. **Set S3 Bucket**: Set the `S3_BUCKET_NAME` Environment Variable in the Lambda configuration to the name of your S3 bucket.
7. **Run Lambda**: Manually trigger the Lambda function with an empty test event `{}`. This will export the data from the test table to S3.
8. **Tinybird Datasource Creation**: Download the NDJSON file from your S3 bucket, and import it into Tinybird using the UI to create the Datasource. This will allow Tinybird to infer the schema for the datasource to save you doing it manually.
9. **Test the Lambda**: Now you can test the Lambda again to check that it replaces the Datasource as expected.

## Conclusion

This setup allows for a streamlined process to move data from DynamoDB into Tinybird. Whether for testing or production, this pipeline ensures your data is where you need it, when you need it.