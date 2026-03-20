# file: 02_upload_daily_csvs.py
# Purpose: Upload daily CSV exports to S3 with proper structure

import boto3
import os
import hashlib
from datetime import datetime
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
LOCAL_EXPORT_DIR = "/tmp/exports"       # Where CSVs are generated
S3_PREFIX = "daily_exports"             # Top-level S3 prefix

# Files generated daily by the order system
DAILY_FILES = [
    "orders",
    "customers",
    "products"
]


def calculate_md5(file_path):
    """Calculate MD5 hash of local file for integrity verification"""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def get_file_size_mb(file_path):
    """Get file size in megabytes"""
    size_bytes = os.path.getsize(file_path)
    return round(size_bytes / (1024 * 1024), 2)


def build_s3_key(entity_name, date_str, filename):
    """
    Build organized S3 key path
    
    Pattern: daily_exports/{entity}/{filename}
    Example: daily_exports/orders/orders_20250101.csv
    
    WHY this structure:
    → Groups by entity type for easy browsing
    → Filename contains date for uniqueness
    → Future: can add year=/month=/day= partitions (Project 26)
    """
    return f"{S3_PREFIX}/{entity_name}/{filename}"


def upload_file_to_s3(s3_client, local_path, bucket, s3_key, entity_name):
    """
    Upload single file to S3 with:
    - Metadata tagging
    - Content type setting
    - Integrity verification
    - Error handling with meaningful messages
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_size = get_file_size_mb(local_path)
    local_md5 = calculate_md5(local_path)

    print(f"\n📤 Uploading: {os.path.basename(local_path)}")
    print(f"   Size: {file_size} MB")
    print(f"   Destination: s3://{bucket}/{s3_key}")

    try:
        s3_client.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                # --- CONTENT TYPE ---
                # Tells S3 (and downstream consumers) this is CSV
                "ContentType": "text/csv",

                # --- SERVER SIDE ENCRYPTION ---
                # Belt-and-suspenders: bucket default + explicit
                "ServerSideEncryption": "AES256",

                # --- CUSTOM METADATA ---
                # Prefix "x-amz-meta-" added automatically by SDK
                "Metadata": {
                    "source-system": "order-management-v2",
                    "entity-type": entity_name,
                    "upload-date": today_str,
                    "local-md5": local_md5,
                    "uploaded-by": "daily-export-script"
                }
            }
        )

        # --- VERIFY UPLOAD ---
        response = s3_client.head_object(Bucket=bucket, Key=s3_key)
        remote_size = response["ContentLength"]
        local_size = os.path.getsize(local_path)

        if remote_size == local_size:
            print(f"   ✅ Upload verified — sizes match ({remote_size} bytes)")
        else:
            print(f"   ⚠️  Size mismatch! Local: {local_size}, Remote: {remote_size}")
            print(f"   → Recommend: re-upload this file")
            return False

        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "AccessDenied":
            print(f"   ❌ ACCESS DENIED — check IAM permissions")
            print(f"   → Need: s3:PutObject on arn:aws:s3:::{bucket}/*")
        elif error_code == "NoSuchBucket":
            print(f"   ❌ Bucket '{bucket}' does not exist")
            print(f"   → Run 01_create_bucket.py first")
        else:
            print(f"   ❌ Upload failed: {e}")
        return False

    except FileNotFoundError:
        print(f"   ❌ Local file not found: {local_path}")
        print(f"   → Check if export job ran successfully")
        return False


def run_daily_upload():
    """
    Main orchestrator: find today's files, upload each one
    """
    s3_client = boto3.client("s3", region_name=REGION)
    today = datetime.now().strftime("%Y%m%d")

    print("=" * 60)
    print(f"🚀 DAILY S3 UPLOAD — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = {"success": [], "failed": []}

    for entity in DAILY_FILES:
        filename = f"{entity}_{today}.csv"
        local_path = os.path.join(LOCAL_EXPORT_DIR, filename)

        # --- CHECK FILE EXISTS LOCALLY ---
        if not os.path.exists(local_path):
            print(f"\n⏭️  Skipping {filename} — not found in {LOCAL_EXPORT_DIR}")
            results["failed"].append(filename)
            continue

        # --- BUILD S3 KEY ---
        s3_key = build_s3_key(entity, today, filename)

        # --- UPLOAD ---
        success = upload_file_to_s3(
            s3_client=s3_client,
            local_path=local_path,
            bucket=BUCKET_NAME,
            s3_key=s3_key,
            entity_name=entity
        )

        if success:
            results["success"].append(filename)
        else:
            results["failed"].append(filename)

    # --- SUMMARY ---
    print("\n" + "=" * 60)
    print("📊 UPLOAD SUMMARY")
    print(f"   ✅ Succeeded: {len(results['success'])}")
    for f in results["success"]:
        print(f"      → {f}")
    print(f"   ❌ Failed: {len(results['failed'])}")
    for f in results["failed"]:
        print(f"      → {f}")
    print("=" * 60)

    # Return exit code for cron job monitoring
    return 0 if len(results["failed"]) == 0 else 1


if __name__ == "__main__":
    exit_code = run_daily_upload()
    exit(exit_code)