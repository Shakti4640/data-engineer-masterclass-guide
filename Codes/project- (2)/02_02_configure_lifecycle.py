# file: 07_configure_lifecycle.py
# Purpose: Add lifecycle rules to manage version costs
# DEPENDS ON: Project 2 Step 1 — versioning must be enabled

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def configure_lifecycle_rules(bucket_name):
    s3_client = boto3.client("s3", region_name=REGION)

    lifecycle_config = {
        "Rules": [
            # ──────────────────────────────────────────────────────
            # RULE 1: Manage non-current versions of daily exports
            # ──────────────────────────────────────────────────────
            {
                "ID": "manage-daily-export-versions",
                "Status": "Enabled",
                "Filter": {
                    "Prefix": "daily_exports/"
                },
                "NoncurrentVersionTransitions": [
                    {
                        # After 30 days: move old versions to cheaper storage
                        # S3 Standard-IA: $0.0125/GB vs $0.023/GB (46% cheaper)
                        # Minimum 30 days before transition allowed
                        "NoncurrentDays": 30,
                        "StorageClass": "STANDARD_IA"
                    },
                    {
                        # After 60 days: move to Glacier Instant Retrieval
                        # $0.004/GB (83% cheaper than Standard)
                        # Still allows millisecond retrieval
                        "NoncurrentDays": 60,
                        "StorageClass": "GLACIER_IR"
                    }
                ],
                "NoncurrentVersionExpiration": {
                    # After 90 days: permanently delete old versions
                    # Keeps 90-day recovery window
                    "NoncurrentDays": 90,

                    # OPTIONAL: keep at least N versions regardless of age
                    # Uncomment if you want to always keep last 3 versions:
                    # "NewerNoncurrentVersions": 3
                }
            },

            # ──────────────────────────────────────────────────────
            # RULE 2: Clean up orphaned delete markers
            # ──────────────────────────────────────────────────────
            {
                "ID": "cleanup-expired-delete-markers",
                "Status": "Enabled",
                "Filter": {
                    "Prefix": ""   # Apply to entire bucket
                },
                "Expiration": {
                    # Delete markers with no non-current versions underneath
                    # are useless — just clutter the version listing
                    "ExpiredObjectDeleteMarker": True
                }
            },

            # ──────────────────────────────────────────────────────
            # RULE 3: Abort incomplete multipart uploads
            # ──────────────────────────────────────────────────────
            {
                "ID": "abort-incomplete-multipart",
                "Status": "Enabled",
                "Filter": {
                    "Prefix": ""   # Apply to entire bucket
                },
                "AbortIncompleteMultipartUpload": {
                    # Failed multipart uploads leave invisible parts
                    # These parts consume storage but are unusable
                    # Common cause of "phantom" storage costs
                    "DaysAfterInitiation": 7
                }
            }
        ]
    }

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        print("✅ Lifecycle rules configured successfully")

        # --- VERIFY ---
        verify_lifecycle_rules(s3_client, bucket_name)
        return True

    except ClientError as e:
        print(f"❌ Failed to configure lifecycle: {e}")
        return False


def verify_lifecycle_rules(s3_client, bucket_name):
    """Read back and display all lifecycle rules"""
    try:
        response = s3_client.get_bucket_lifecycle_configuration(
            Bucket=bucket_name
        )

        print(f"\n📋 Active Lifecycle Rules for '{bucket_name}':")
        print("-" * 70)

        for rule in response["Rules"]:
            print(f"\n  Rule ID: {rule['ID']}")
            print(f"  Status:  {rule['Status']}")

            # Filter
            if "Filter" in rule:
                prefix = rule["Filter"].get("Prefix", "(entire bucket)")
                print(f"  Applies to: {prefix if prefix else '(entire bucket)'}")

            # Non-current version transitions
            if "NoncurrentVersionTransitions" in rule:
                for t in rule["NoncurrentVersionTransitions"]:
                    print(f"  → Transition non-current to {t['StorageClass']} "
                          f"after {t['NoncurrentDays']} days")

            # Non-current version expiration
            if "NoncurrentVersionExpiration" in rule:
                exp = rule["NoncurrentVersionExpiration"]
                days = exp.get("NoncurrentDays", "N/A")
                print(f"  → Delete non-current versions after {days} days")

            # Delete marker cleanup
            if "Expiration" in rule:
                if rule["Expiration"].get("ExpiredObjectDeleteMarker"):
                    print(f"  → Auto-clean expired delete markers")

            # Multipart cleanup
            if "AbortIncompleteMultipartUpload" in rule:
                days = rule["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"]
                print(f"  → Abort incomplete multipart uploads after {days} days")

        print("-" * 70)

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
            print("ℹ️  No lifecycle rules configured yet")
        else:
            raise


if __name__ == "__main__":
    print("=" * 60)
    print("♻️  CONFIGURING S3 LIFECYCLE RULES")
    print(f"   Bucket: {BUCKET_NAME}")
    print("=" * 60)
    configure_lifecycle_rules(BUCKET_NAME)

    print("\n📊 COST IMPACT SUMMARY:")
    print("   Day 0-30:  Non-current versions at Standard ($0.023/GB)")
    print("   Day 30-60: Non-current versions at S3-IA ($0.0125/GB)")
    print("   Day 60-90: Non-current versions at Glacier IR ($0.004/GB)")
    print("   Day 90+:   Non-current versions PERMANENTLY DELETED")
    print("   Always:    Orphaned delete markers auto-cleaned")
    print("   Always:    Failed multipart uploads cleaned after 7 days")