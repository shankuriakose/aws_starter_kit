import os
from dotenv import load_dotenv
import concurrent.futures
import boto3

load_dotenv()

# Configuration
# BUCKET_NAMES = ['10-20-50', '10-20-50-100', '10-20-50-100-200-500', '10-200-500-20250219t070855z-001', '10-50-100-coin', '10-50-200-500', '100-200-coin', '20-200', '20-coin', '200-20-coin', '50-500-coin',  'coin-100', 'coin-coin-coin-coin', 'security-data-five', 'security-data-five-a', 'security-data-four', 'security-data-four-a', 'security-data-one', 'security-data-one-a', 'security-data-one-b', 'security-data-seven', 'security-data-seven-a', 'security-data-six', 'security-data-six-a', 'security-data-three', 'security-data-three-a', 'security-data-three-b', 'security-data-three-c', 'security-data-two', 'security-data-two-a', 'security-data-two-b', 'security-data-two-c', 'security-data-two-d']
BUCKET_NAMES = ['final-security-data']
print(len(BUCKET_NAMES))
LOCAL_DOWNLOAD_BASE_PATH = "./data"  # Base directory for downloads

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

# Ensure base download path exists
os.makedirs(LOCAL_DOWNLOAD_BASE_PATH, exist_ok=True)

def download_file(bucket, obj, local_bucket_path):
    """Downloads a single file from S3."""
    file_key = obj.key
    local_file_path = os.path.join(local_bucket_path, file_key)

    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

    try:
        print(f"Downloading {file_key} from {bucket.name} to {local_file_path}")
        bucket.download_file(file_key, local_file_path)
        print(f"Downloaded {file_key} from {bucket.name}")
    except Exception as e:
        print(f"Error downloading {file_key} from {bucket.name}: {e}")

def download_all_files_from_bucket(bucket_name):
    """Downloads all files from a given S3 bucket using multithreading."""
    try:
        bucket = s3.Bucket(bucket_name)
        objects = list(bucket.objects.all())  # Get all objects in the bucket
        if objects:
            local_bucket_path = os.path.join(LOCAL_DOWNLOAD_BASE_PATH, bucket_name)
            os.makedirs(local_bucket_path, exist_ok=True)

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
                executor.map(lambda obj: download_file(bucket, obj, local_bucket_path), objects)
        else:
            print(f"No files found in bucket {bucket_name}")
    except Exception as e:
        print(f"Error with bucket {bucket_name}: {e}")

def download_all_files_from_all_buckets(bucket_names):
    """Downloads all files from all provided S3 buckets using multithreading."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(bucket_names)) as executor:
        executor.map(download_all_files_from_bucket, bucket_names)

if __name__ == "__main__":
    download_all_files_from_all_buckets(BUCKET_NAMES)
