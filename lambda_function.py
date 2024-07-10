import boto3
import os
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from decimal import Decimal
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def convert_to_decimal(item):
    """Convert floats to Decimals for DynamoDB compatibility."""
    for key, value in item.items():
        if isinstance(value, float):
            item[key] = Decimal(str(value))
        elif isinstance(value, list):
            item[key] = [Decimal(str(v)) if isinstance(v, float) else v for v in value]
    return item

def lambda_handler(event, context):
    """Lambda function handler."""
    # Get the S3 bucket and object key from the event
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_name = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file {file_name} from bucket {bucket_name}")
    except KeyError as e:
        logger.error("Error parsing event data: %s", e)
        return {"statusCode": 400, "body": "Invalid S3 event data"}

    # Initialize the S3 client
    s3 = boto3.client('s3')
    
    # Get the object from S3
    try:
        data = s3.get_object(Bucket=bucket_name, Key=file_name)
        contents = data['Body'].read().decode("utf-8")
    except ClientError as e:
        logger.error(f"Error getting object {file_name} from bucket {bucket_name}: {e}")
        return {"statusCode": 500, "body": f"Error getting object {file_name} from bucket {bucket_name}: {e}"}
    
    # Read the CSV data into a pandas DataFrame
    try:
        csv_data = StringIO(contents)
        df = pd.read_csv(csv_data)
    except Exception as e:
        logger.error(f"Error reading CSV data: {e}")
        return {"statusCode": 400, "body": f"Error reading CSV data: {e}"}
    
    if df.empty:
        logger.warning("CSV file is empty")
        return {"statusCode": 400, "body": "CSV file is empty"}

    # Filter out rows with NaN or infinity values
    df = df.replace([pd.NA, float('inf'), float('-inf')], pd.NA).dropna()
    
    # Convert DataFrame to a list of dictionaries
    items = df.to_dict(orient="records")
    
    # Convert all float values in items to Decimal
    items = [convert_to_decimal(item) for item in items]

    # Check for TABLE_NAME environment variable
    table_name = os.getenv('TABLE_NAME')
    if not table_name:
        logger.error("TABLE_NAME environment variable is not set")
        return {"statusCode": 500, "body": "TABLE_NAME environment variable is not set"}
    
    # Reference the DynamoDB table
    table = dynamodb.Table(table_name)
    
    # Store each item in the DynamoDB table
    for item in items:
        try:
            table.put_item(Item=item)
            logger.info(f"Item added: {item}")
        except ClientError as e:
            logger.error(f"Error adding item: {e.response['Error']['Message']}")
            return {"statusCode": 500, "body": f"Error adding item: {e.response['Error']['Message']}"}
    
    logger.info("Items processing completed!")
    return {"statusCode": 200, "body": "Items processing completed successfully"}
