import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
)

response = s3.list_buckets()
print("S3 Buckets:", [bucket['Name'] for bucket in response['Buckets']])
