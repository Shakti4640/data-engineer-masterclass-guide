# file: 04_verify_uploads.py
# Purpose: List all objects in bucket and verify structure

import boto3
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def list_all_objects(bucket_name):
    """
    List all objects with details
    
    IMPORTANT S3 CONCEPT:
    → list_objects_v2 returns MAX 1000 objects per call
    → Must use pagination (ContinuationToken) for more
    → This is why Paginator exists in boto3
    """
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    print(f"\n📦 Contents of s3://{bucket_name}/")
    print("-" * 80)
    print(f"{'Key':<55} {'Size (KB)':>10} {'Last Modified':>15}")
    print("-" * 80)

    total_objects = 0
    total_size = 0

    for page in paginator.paginate(Bucket=bucket_name):
        if "Contents" not in page:
            print("   (empty bucket)")
            return

        for obj in page["Contents"]:
            key = obj["Key"]
            size_kb = round(obj["Size"] / 1024, 1)
            modified = obj["LastModified"].strftime("%Y-%m-%d %H:%M")

            print(f"  {key:<55} {size_kb:>8} KB  {modified}")

            total_objects += 1
            total_size += obj["Size"]

    total_size_mb = round(total_size / (1024 * 1024), 2)
    print("-" * 80)
    print(f"  Total: {total_objects} objects, {total_size_mb} MB")


def inspect_object_metadata(bucket_name, key):
    """Show all metadata for a specific object"""
    s3_client = boto3.client("s3", region_name=REGION)

    response = s3_client.head_object(Bucket=bucket_name, Key=key)

    print(f"\n🔍 Metadata for: s3://{bucket_name}/{key}")
    print("-" * 50)
    print(f"  Content-Type:      {response.get('ContentType', 'N/A')}")
    print(f"  Content-Length:    {response['ContentLength']} bytes")
    print(f"  Last Modified:     {response['LastModified']}")
    print(f"  ETag:              {response['ETag']}")
    print(f"  Storage Class:     {response.get('StorageClass', 'STANDARD')}")
    print(f"  Encryption:        {response.get('ServerSideEncryption', 'None')}")

    # Custom metadata (x-amz-meta-* headers)
    if response.get("Metadata"):
        print(f"  Custom Metadata:")
        for k, v in response["Metadata"].items():
            print(f"    {k}: {v}")


if __name__ == "__main__":
    list_all_objects(BUCKET_NAME)

    # Inspect one specific file
    today = datetime.now().strftime("%Y%m%d")
    sample_key = f"daily_exports/orders/orders_{today}.csv"
    inspect_object_metadata(BUCKET_NAME, sample_key)