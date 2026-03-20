# file: 05_download_from_s3.py
# Purpose: Download specific file from S3 — needed for reprocessing scenarios

import boto3
import os

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
DOWNLOAD_DIR = "/tmp/downloads"


def download_file(bucket, s3_key, local_dir):
    """
    Download single file from S3
    
    WHY THIS MATTERS:
    → EC2 processing scripts may need to re-download
    → Data validation requires local inspection
    → Disaster recovery: restore from S3 backup
    """
    s3_client = boto3.client("s3", region_name=REGION)
    os.makedirs(local_dir, exist_ok=True)

    filename = os.path.basename(s3_key)
    local_path = os.path.join(local_dir, filename)

    print(f"📥 Downloading: s3://{bucket}/{s3_key}")
    print(f"   To: {local_path}")

    s3_client.download_file(
        Bucket=bucket,
        Key=s3_key,
        Filename=local_path
    )

    size_mb = round(os.path.getsize(local_path) / (1024 * 1024), 2)
    print(f"   ✅ Downloaded: {size_mb} MB")
    return local_path


if __name__ == "__main__":
    from datetime import datetime
    today = datetime.now().strftime("%Y%m%d")
    
    download_file(
        bucket=BUCKET_NAME,
        s3_key=f"daily_exports/orders/orders_{today}.csv",
        local_dir=DOWNLOAD_DIR
    )