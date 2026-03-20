# file: 06_05_upload_with_role.py
# Purpose: Prove that Project 1's upload script works WITHOUT any code change

# ================================================================
# KEY INSIGHT:
# The upload script from Project 1 (02_upload_daily_csvs.py)
# works IDENTICALLY with IAM Roles
#
# boto3.client("s3") → searches credential chain → finds Instance Profile
# ZERO code change required
#
# The ONLY change is INFRASTRUCTURE:
# - Old: static keys in ~/.aws/credentials
# - New: IAM Role via Instance Profile
#
# This is the beauty of the boto3 credential chain:
# Application code is DECOUPLED from credential management
# ================================================================

import boto3
import os
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
LOCAL_EXPORT_DIR = "/tmp/exports"
S3_PREFIX = "daily_exports"


def upload_with_role():
    """
    IDENTICAL to Project 1's upload logic
    No access_key_id parameter
    No secret_access_key parameter
    No credential file reference
    
    boto3 handles everything via:
    Instance Profile → IMDS → STS → Temp Credentials
    """
    # This single line triggers the ENTIRE credential chain
    # Priority 7 (Instance Metadata) kicks in automatically
    s3_client = boto3.client("s3", region_name=REGION)

    today = datetime.now().strftime("%Y%m%d")
    entities = ["orders", "customers", "products"]

    print("=" * 60)
    print(f"🚀 UPLOAD WITH IAM ROLE — {datetime.now()}")
    print("=" * 60)

    # Verify credential source first
    sts_client = boto3.client("sts")
    identity = sts_client.get_caller_identity()
    print(f"\n🔐 Running as: {identity['Arn']}")

    if "assumed-role" in identity["Arn"]:
        print(f"   ✅ Confirmed: IAM Role (no static keys)")
    else:
        print(f"   ⚠️  Not using IAM Role — check Instance Profile")

    for entity in entities:
        filename = f"{entity}_{today}.csv"
        local_path = os.path.join(LOCAL_EXPORT_DIR, filename)
        s3_key = f"{S3_PREFIX}/{entity}/{filename}"

        if not os.path.exists(local_path):
            print(f"\n⏭️  {filename} — not found, skipping")
            continue

        print(f"\n📤 Uploading {filename}")
        print(f"   → s3://{BUCKET_NAME}/{s3_key}")

        try:
            s3_client.upload_file(
                Filename=local_path,
                Bucket=BUCKET_NAME,
                Key=s3_key,
                ExtraArgs={
                    "ContentType": "text/csv",
                    "ServerSideEncryption": "AES256",
                    "Metadata": {
                        "source": "order-management",
                        "entity": entity,
                        "upload-date": datetime.now().isoformat(),
                        "credential-method": "iam-role"  # Proof it's using role
                    }
                }
            )

            # Verify — same as Project 1
            head = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            local_size = os.path.getsize(local_path)
            remote_size = head["ContentLength"]

            if local_size == remote_size:
                print(f"   ✅ Verified: {remote_size} bytes")
            else:
                print(f"   ⚠️  Size mismatch: local={local_size}, remote={remote_size}")

        except Exception as e:
            print(f"   ❌ Failed: {e}")

    print(f"\n{'=' * 60}")
    print(f"✅ UPLOAD COMPLETE — all done with IAM Role, zero hardcoded keys")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    upload_with_role()