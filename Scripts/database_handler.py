import json
import boto3
import os
from dotenv import load_dotenv


# Load environment variables for api key
load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
bucket_name = 'your-bucket-name'



#initialize s3 aws storage
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

# Function to check if file exists on S3
def s3_file_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:  
        return False

# Function to upload data to sw3
def upload_to_s3(data, file_name):
    try:
        # Convert data to JSON
        json_data = json.dumps(data, indent=4)
        
        # Upload to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data)
        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")