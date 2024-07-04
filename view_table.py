import boto3
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv('env.env')

# Initialize a session using Amazon DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')  # Ensure region is specified
)

# Reference the DynamoDB table
table = dynamodb.Table(os.getenv('TABLE_NAME'))  # Ensure table name matches your DynamoDB table

# Scan the table
response = table.scan()
data = response['Items']

# Print the retrieved data
for item in data:
    print(item)
