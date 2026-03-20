# file: 16_01_create_athena_output_bucket.py
# Purpose: Create dedicated S3 bucket for Athena query results
# Dependency: Uses bucket creation pattern from Project 1

import boto3
from botocore.exceptions import ClientError

ATHENA_OUTPUT_BUCKET = "quickcart-athena-results"
REGION = "us-east-2"


def create_athena_output_bucket():
    s3_client = boto3.client("s3", region_name=REGION)

    try:
        # --- CREATE BUCKET ---
        # Same pattern as Project 1 — but for Athena results
        s3_client.create_bucket(
            Bucket=ATHENA_OUTPUT_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Athena output bucket created: {ATHENA_OUTPUT_BUCKET}")

        # --- BLOCK PUBLIC ACCESS (same as Project 1) ---
        s3_client.put_public_access_block(
            Bucket=ATHENA_OUTPUT_BUCKET,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True
            }
        )
        print("✅ Public access blocked")

        # --- LIFECYCLE RULE: Auto-delete results after 7 days ---
        # WHY: Athena writes 2 files per query (result + metadata)
        #      100 queries/day = 200 files/day = 6000 files/month
        #      Old results are rarely needed — auto-cleanup saves cost
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=ATHENA_OUTPUT_BUCKET,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "delete-old-athena-results",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},  # all objects
                        "Expiration": {"Days": 7}
                    }
                ]
            }
        )
        print("✅ Lifecycle rule: auto-delete after 7 days")

        # --- TAG ---
        s3_client.put_bucket_tagging(
            Bucket=ATHENA_OUTPUT_BUCKET,
            Tagging={
                "TagSet": [
                    {"Key": "Purpose", "Value": "Athena-Query-Results"},
                    {"Key": "AutoDelete", "Value": "7-days"},
                    {"Key": "Team", "Value": "Data-Engineering"}
                ]
            }
        )
        print("✅ Bucket tagged")

    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"ℹ️  Bucket already exists: {ATHENA_OUTPUT_BUCKET}")
        else:
            raise


if __name__ == "__main__":
    create_athena_output_bucket()