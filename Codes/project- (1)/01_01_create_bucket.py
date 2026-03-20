# file: 01_create_bucket.py
# Purpose: Create the S3 bucket with proper configuration

import boto3
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
BUCKET_NAME = "quickcart-raw-data-prod"       # Must be globally unique
REGION = "us-east-2"                           # Ohio — same as our EC2

def create_bucket(bucket_name, region):
    """
    Create S3 bucket with:
    - Specified region
    - All public access blocked
    - Server-side encryption enabled
    - Tags for cost tracking
    """
    s3_client = boto3.client("s3", region_name=region)

    try:
        # --- CREATE BUCKET ---
        # us-east-1 does NOT accept LocationConstraint (AWS quirk)
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": region
                }
            )
        print(f"✅ Bucket '{bucket_name}' created in {region}")

        # --- BLOCK ALL PUBLIC ACCESS ---
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True
            }
        )
        print("✅ Public access blocked")

        # --- ENABLE DEFAULT ENCRYPTION (SSE-S3) ---
        s3_client.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        },
                        "BucketKeyEnabled": True
                    }
                ]
            }
        )
        print("✅ Server-side encryption (AES256) enabled")

        # --- TAG THE BUCKET ---
        s3_client.put_bucket_tagging(
            Bucket=bucket_name,
            Tagging={
                "TagSet": [
                    {"Key": "Environment", "Value": "Production"},
                    {"Key": "Team", "Value": "Data-Engineering"},
                    {"Key": "Project", "Value": "QuickCart-DataLake"},
                    {"Key": "CostCenter", "Value": "ENG-001"}
                ]
            }
        )
        print("✅ Bucket tagged for cost tracking")

        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "BucketAlreadyOwnedByYou":
            print(f"ℹ️  Bucket '{bucket_name}' already exists in your account")
            return True
        elif error_code == "BucketAlreadyExists":
            print(f"❌ Bucket '{bucket_name}' is taken by another AWS account")
            print("   → Try: quickcart-raw-data-prod-{your-account-id}")
            return False
        else:
            print(f"❌ Unexpected error: {e}")
            raise


if __name__ == "__main__":
    create_bucket(BUCKET_NAME, REGION)