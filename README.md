# ETL Process

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using AWS services. The pipeline involves downloading a sample CSV file, uploading it to an S3 bucket, and processing it to store the data in a DynamoDB table.

## Steps Performed
### Step 1: Download Sample File

Download a sample file in CSV format.
### Step 2: Create AWS Account and S3 Bucket

1. Create an AWS account if you don't already have one.
2. Create an S3 bucket:
    - Bucket Name: etlprocess-files

### Step 3: Access the File in S3 via Python Script
#### Requirements

- Python SDK boto3 (as of July 2024)
- Python 3.x

#### Setup Virtual Environment

```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## AWS IAM Configuration
### Create IAM Group and User

1. Create IAM Group (if it does not exist):
    - Go to the IAM console.
    - Create a group with the desired name.
    - Attach the following policies:
        - AmazonS3FullAccess
        - AmazonDynamoDBFullAccess
    - Click the "Create Group" button.

2. Create IAM User:
    - Go to the IAM console.
    - Create a user with the desired username.
    - Add the user to the previously created group.
    - Open the user details and navigate to the "Security credentials" tab.
    - Under "Access keys", create a new access key.
    - Choose "Local code" for key usage.
    - Optionally, write a description tag.
    - Store the access key and secret key securely. Note: The secret key is only viewable once, so store it immediately.

### Step 4: Create DynamoDB Table

1. Go to the DynamoDB console.
2. Click the "Create Table" button.
3. Enter the table name: users.
4. Define the partition key: Name.
5. Optionally, define a sort key: Age.
6. Click the "Create Table" button.

## Running the ETL Scripts
### Run the ETL Script

This script retrieves data from the CSV stored in S3 and pushes the items to DynamoDB.
```
python3 main.py
```

### View Data in DynamoDB

This script retrieves data inserted in DynamoDB from the users table.

```
python3 view_table.py
```

> Notes
>- Ensure that your AWS credentials are configured correctly in your environment.
> - The scripts assume that the necessary AWS services (S3 and DynamoDB) and IAM permissions are correctly set up.