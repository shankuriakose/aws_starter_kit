import boto3
import mimetypes
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from tqdm import tqdm
import threading
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_s3_client():
    """Creates an S3 client using environment credentials."""
    return boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
)

def upload_file_to_s3(s3_client, local_path, bucket_name, s3_path, progress_bar, lock):
    """Uploads a single file to S3 and updates progress bar."""
    try:
        content_type, _ = mimetypes.guess_type(local_path)
        extra_args = {'ContentType': content_type} if content_type else {}

        s3_client.upload_file(str(local_path), bucket_name, s3_path, ExtraArgs=extra_args)
        logging.info(f"Uploaded: {local_path} to s3://{bucket_name}/{s3_path}")

    except Exception as e:
        logging.error(f"Error uploading {local_path}: {e}")

    finally:
        with lock:
            progress_bar.update(1)

def upload_directory_to_s3(local_directory, bucket_name, s3_prefix='', num_threads=10):
    """Uploads a local directory to an S3 bucket using a thread pool with a progress tracker."""
    s3_client = create_s3_client()
    local_directory = Path(local_directory)

    files = [f for f in local_directory.rglob('*') if f.is_file()]

    
    progress_bar = tqdm(total=len(files), desc="Uploading Files", unit="file", ncols=80)
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = {}
        for local_path in files:
            relative_path = local_path.relative_to(local_directory)
            s3_path = f"{s3_prefix}/{relative_path}".replace("\\", "/") if s3_prefix else str(relative_path).replace("\\", "/")

            futures[executor.submit(upload_file_to_s3, s3_client, local_path, bucket_name, s3_path, progress_bar, lock)] = local_path

        
        for future in as_completed(futures):
            future.result()  

    progress_bar.close()  

if __name__ == "__main__":
    local_directory = './runs'  
    bucket_name = 'final-security-data' 
    s3_prefix = "model-weights" 
    num_threads = 10  

    upload_directory_to_s3(local_directory, bucket_name, num_threads)
