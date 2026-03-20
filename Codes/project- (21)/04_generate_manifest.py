# file: 04_generate_manifest.py
# Creates a manifest JSON file listing EXACT files for COPY
# This gives us full control over which files get loaded

import boto3
import json
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
DATA_PREFIX = "daily_exports/orders/"
MANIFEST_KEY = "manifests/orders_full_load_manifest.json"


def generate_manifest():
    """
    Scan S3 for all order files and create manifest
    
    MANIFEST FORMAT (required by Redshift):
    {
      "entries": [
        {"url": "s3://bucket/path/file1.csv.gz", "mandatory": true},
        {"url": "s3://bucket/path/file2.csv.gz", "mandatory": true}
      ]
    }
    
    "mandatory": true → COPY fails if this file is missing
    "mandatory": false → COPY skips missing files silently
    → ALWAYS use true in production (fail loudly > silent data loss)
    """
    s3_client = boto3.client("s3", region_name=REGION)

    # List all .gz files under the orders prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    entries = []

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=DATA_PREFIX):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            # Only include .gz files (skip any non-data files)
            if key.endswith(".csv.gz") or key.endswith(".csv"):
                entries.append({
                    "url": f"s3://{BUCKET_NAME}/{key}",
                    "mandatory": True
                })

    if not entries:
        print(f"❌ No data files found under s3://{BUCKET_NAME}/{DATA_PREFIX}")
        return None

    # Sort for deterministic ordering
    entries.sort(key=lambda x: x["url"])

    manifest = {"entries": entries}

    # Upload manifest to S3
    manifest_json = json.dumps(manifest, indent=2)
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=MANIFEST_KEY,
        Body=manifest_json,
        ContentType="application/json",
        ServerSideEncryption="AES256",
        Metadata={
            "generated-at": datetime.now().isoformat(),
            "file-count": str(len(entries)),
            "purpose": "redshift-copy-full-load"
        }
    )

    manifest_url = f"s3://{BUCKET_NAME}/{MANIFEST_KEY}"
    print(f"✅ Manifest created: {manifest_url}")
    print(f"   Files listed: {len(entries)}")
    print(f"\n📋 Manifest content:")
    print(manifest_json)

    return manifest_url


if __name__ == "__main__":
    url = generate_manifest()
    if url:
        print(f"\n🔑 Use this in COPY command:")
        print(f"   COPY orders FROM '{url}'")
        print(f"   IAM_ROLE '...' MANIFEST CSV IGNOREHEADER 1 GZIP;")