import os
from dotenv import load_dotenv
import boto3


load_dotenv()

# Configuration
BUCKET_NAMES = ['final-security-data']  # List of target buckets
SUBFOLDER_NAME = "10/obb"  # Specify the subfolder to download

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')

# Initialize S3 client
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

def download_file(bucket, obj, local_bucket_path):
    """Downloads a single file from S3."""
    file_key = obj.key
    
    
    if not file_key.startswith(SUBFOLDER_NAME):
        return
    
    
    relative_file_path = file_key[len(SUBFOLDER_NAME):].lstrip('/')
    local_file_path = os.path.join(local_bucket_path, relative_file_path)
    
    
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

    try:
        print(f"Downloading {file_key} from {bucket.name} to {local_file_path}")
        bucket.download_file(file_key, local_file_path)
        print(f"Downloaded {file_key} from {bucket.name}")
    except Exception as e:
        print(f"Error downloading {file_key} from {bucket.name}: {e}")

def download_subfolder_from_bucket(bucket_name):
    """Downloads only a specific subfolder from a given S3 bucket."""
    try:
        bucket = s3.Bucket(bucket_name)
        objects = [obj for obj in bucket.objects.filter(Prefix=SUBFOLDER_NAME)]
        
        if objects:
            
            local_bucket_path = os.path.join(os.getcwd(), bucket_name)
            os.makedirs(local_bucket_path, exist_ok=True)

            for obj in objects:
                download_file(bucket, obj, local_bucket_path)
        else:
            print(f"No files found in subfolder {SUBFOLDER_NAME} in bucket {bucket_name}")
    except Exception as e:
        print(f"Error with bucket {bucket_name}: {e}")

def download_subfolder_from_all_buckets(bucket_names):
    """Downloads the specific subfolder from all provided S3 buckets."""
    for bucket_name in bucket_names:
        download_subfolder_from_bucket(bucket_name)

if __name__ == "__main__":
    download_subfolder_from_all_buckets(BUCKET_NAMES)
