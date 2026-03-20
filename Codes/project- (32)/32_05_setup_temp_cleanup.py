# file: 32_05_setup_temp_cleanup.py
# Purpose: Auto-delete orphaned temp files from Glue-Redshift staging
# Prevents cost accumulation from failed cleanup steps

import boto3

S3_BUCKET = "quickcart-datalake-prod"
s3 = boto3.client("s3", region_name="us-east-2")


def create_temp_lifecycle_rule():
    """
    S3 Lifecycle Rule: delete temp files after 3 days
    
    WHY:
    → Each Glue → Redshift run writes temp CSVs to S3
    → Normally cleaned up after successful COPY
    → But if job fails mid-way, temp files remain
    → 365 failed runs × 2GB each = 730 GB of orphaned files
    → At $0.023/GB: $16.79/month wasted
    → Lifecycle rule auto-deletes after 3 days
    """
    print("🧹 Setting up temp file auto-cleanup...")

    # Get existing lifecycle rules (preserve them)
    try:
        existing = s3.get_bucket_lifecycle_configuration(Bucket=S3_BUCKET)
        rules = existing.get("Rules", [])
    except s3.exceptions.ClientError:
        rules = []

    # Add temp cleanup rule
    temp_rule = {
        "ID": "cleanup-glue-redshift-temp",
        "Status": "Enabled",
        "Filter": {
            "Prefix": "tmp/glue-redshift/"
        },
        "Expiration": {
            "Days": 3
        }
    }

    # Check if rule already exists
    existing_ids = [r["ID"] for r in rules]
    if temp_rule["ID"] in existing_ids:
        print(f"   ℹ️  Rule already exists: {temp_rule['ID']}")
        return

    rules.append(temp_rule)

    s3.put_bucket_lifecycle_configuration(
        Bucket=S3_BUCKET,
        LifecycleConfiguration={"Rules": rules}
    )

    print(f"   ✅ Lifecycle rule created: delete tmp/glue-redshift/* after 3 days")
    print(f"   → Prevents orphaned temp file accumulation")
    print(f"   → Saves ~$17/month if failures occur regularly")


if __name__ == "__main__":
    create_temp_lifecycle_rule()