import json
import boto3
import os
from dotenv import load_dotenv


# Load environment variables for api key
load_dotenv(dotenv_path=os.path.join('config', '.env'))

aws_access_key = os.getenv('AWS_ACCESS_KEY')
aws_secret_key = os.getenv('AWS_SECRET_KEY')
bucket_name = os.getenv('AWS_BUCKET_NAME')


#initialize s3 aws storage
s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name = 'us-east-2')

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

        if not json_data:
            print('No data to upload')
            return
        
        # Upload to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data.encode('utf-8'))
        print(f"Successfully uploaded {file_name} to S3.")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Function to download existing S3 file
def download_existing_s3_data(file_name):
    try:
        if s3_file_exists(bucket_name, file_name):
            print(f"Downloading existing {file_name} from S3...")
            obj = s3.get_object(Bucket=bucket_name, Key=file_name)
            return json.loads(obj['Body'].read().decode('utf-8'))
        else:
            print("No existing data found. Creating a new file.")
            return []
    except Exception as e:
        print(f"Error downloading existing S3 data: {e}")
        return []
    
# Function to upload FILE to S3
def upload_file_to_s3(file_name):
        try:
            s3.upload_file(file_name, Bucket = bucket_name, onject_name = file_name)
            print(f'Uploaded {file_name} successfully')
        except Exception as e:
            print(f'The following error has occured: {e}')