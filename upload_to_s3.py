"""
Dallas Real Estate Pipeline - S3 Upload
Uploads pipeline data to AWS S3.
Author: Issa Amjadi
"""

import boto3
import os
from datetime import datetime


def upload_to_s3(local_dir="data", bucket_name="dallas-real-estate-pipeline-issa"):
    """Upload data files to S3 bucket."""
    
    s3 = boto3.client("s3")
    
    files_to_upload = {
        "dallas_listings_clean.csv": "clean/dallas_listings_clean.csv",
        "dallas_listings_spark.csv": "clean/dallas_listings_spark.csv",
        "location_stats.csv": "clean/location_stats.csv",
    }
    
    # Upload raw files
    for filename in os.listdir(local_dir):
        if filename.startswith("dallas_listings_2") and filename.endswith(".csv"):
            s3_key = f"raw/{filename}"
            local_path = os.path.join(local_dir, filename)
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"Uploaded {filename} → s3://{bucket_name}/{s3_key}")
    
    # Upload clean/processed files
    for local_name, s3_key in files_to_upload.items():
        local_path = os.path.join(local_dir, local_name)
        if os.path.exists(local_path):
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"Uploaded {local_name} → s3://{bucket_name}/{s3_key}")
    
    print(f"\nAll files uploaded to s3://{bucket_name}/")


if __name__ == "__main__":
    print("=" * 50)
    print("Uploading to AWS S3")
    print("=" * 50)
    upload_to_s3()