# Tinybird DynamoDB Export Utilities

This repository contains example solutions for replicating data out of DynamoDB into Tinybird.

Both solutions, scanToExport and DDBStreamCDC contain their own readme files on usage.

## DDBStreamCDC
This solution leverages AWS DynamoDB Export to S3 & AWS DynamoDB Streams functionalities via a Python Lambda to forward both snapshots and changes to a Tinybird Datasource, which is then deduplicated for the latest values and made available for users.

A simple implementation pathway is provided which may be expanded upon.

## scanToExport

This solution uses a simple DynamoDB Scan to export a file to an S3 bucket, and then uses the Tinybird Datasource Replace functionality, implemented as a Python Lambda.

It is simpler in nature than the CDC solution, though it lacks the low latency updates and you may find a Scan is more expensive in practice.
