import boto3
from dotenv import load_dotenv
import os
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from decimal import Decimal

# Load environment variables from .env file
load_dotenv('env.env')

# Initialize S3 client with credentials from environment variables
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION'))

# Get bucket name and file name from environment variables
bucket_name = os.getenv('BUCKET')
file_name = os.getenv('FILENAME')

# Get the object from S3
data = s3.get_object(Bucket=bucket_name, Key=file_name)
contents = data['Body'].read().decode("utf-8")

# Read the CSV data into a pandas DataFrame
csv_data = StringIO(contents)
df = pd.read_csv(csv_data)

# Filter out rows with NaN or infinity values
df = df.replace([pd.NA, float('inf'), float('-inf')], pd.NA).dropna()

# Convert DataFrame to a list of dictionaries
items = df.to_dict(orient="records")

# Function to convert floats to Decimals
def convert_to_decimal(item):
    for key, value in item.items():
        if isinstance(value, float):
            item[key] = Decimal(str(value))
        elif isinstance(value, list):
            item[key] = [Decimal(str(v)) if isinstance(v, float) else v for v in value]
    return item

# Convert all float values in items to Decimal
items = [convert_to_decimal(item) for item in items]

# Initialize DynamoDB resource with credentials from environment variables
dynamodb = boto3.resource(
    'dynamodb',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')  
)

# Reference the DynamoDB table using environment variable
table = dynamodb.Table(os.getenv('TABLE_NAME'))

# Store each item in the DynamoDB table
for item in items:
    try:
        table.put_item(Item=item)
        print(f"Item added: {item}")
    except ClientError as e:
        print(f"Error adding item: {e.response['Error']['Message']}")

print("Items processing completed!")
