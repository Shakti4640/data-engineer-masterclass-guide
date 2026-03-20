# file: 03_compress_and_upload.py
# Compresses daily CSVs with GZIP and uploads to S3
# GZIP reduces S3 read I/O by 70-80% during COPY

import boto3
import gzip
import os
import glob
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
LOCAL_EXPORT_DIR = "/tmp/exports"
S3_PREFIX = "daily_exports/orders"


def compress_file(input_path, output_path):
    """Compress CSV to GZIP"""
    with open(input_path, "rb") as f_in:
        with gzip.open(output_path, "wb") as f_out:
            f_out.writelines(f_in)

    original_size = os.path.getsize(input_path)
    compressed_size = os.path.getsize(output_path)
    ratio = round((1 - compressed_size / original_size) * 100, 1)
    print(f"   Compressed: {original_size:,} → {compressed_size:,} bytes ({ratio}% reduction)")
    return output_path


def compress_and_upload_all():
    """Find all order CSVs, compress, upload to S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    # Find all orders CSV files
    csv_files = sorted(glob.glob(os.path.join(LOCAL_EXPORT_DIR, "orders_*.csv")))

    if not csv_files:
        print(f"❌ No orders CSV files found in {LOCAL_EXPORT_DIR}")
        return

    print(f"📦 Found {len(csv_files)} CSV files to compress and upload\n")

    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        gz_filename = filename + ".gz"
        gz_path = os.path.join(LOCAL_EXPORT_DIR, gz_filename)

        print(f"📤 Processing: {filename}")

        # Compress
        compress_file(csv_path, gz_path)

        # Upload compressed file
        s3_key = f"{S3_PREFIX}/{gz_filename}"
        s3_client.upload_file(
            Filename=gz_path,
            Bucket=BUCKET_NAME,
            Key=s3_key,
            ExtraArgs={
                "ContentType": "application/gzip",
                "ContentEncoding": "gzip",
                "ServerSideEncryption": "AES256",
                "Metadata": {
                    "original-format": "csv",
                    "compression": "gzip",
                    "upload-timestamp": datetime.now().isoformat()
                }
            }
        )
        print(f"   ✅ Uploaded: s3://{BUCKET_NAME}/{s3_key}")

        # Cleanup local compressed file
        os.remove(gz_path)
        print()

    print(f"🎯 All {len(csv_files)} files compressed and uploaded")


if __name__ == "__main__":
    compress_and_upload_all()