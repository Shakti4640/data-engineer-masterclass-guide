# file: 21_intelligent_tiering.py
# Purpose: Demonstrate S3 Intelligent Tiering as alternative to manual lifecycle
# DEPENDS ON: Understanding of storage classes from Steps 1-4
# USE WHEN: access patterns are UNPREDICTABLE

import boto3
import json
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def configure_intelligent_tiering(bucket_name):
    """
    Configure S3 Intelligent Tiering
    
    TWO WAYS TO USE IT:
    
    Method 1: Upload directly with StorageClass='INTELLIGENT_TIERING'
    → Object auto-managed from upload
    → No lifecycle rule needed
    
    Method 2: Lifecycle rule transitions to INTELLIGENT_TIERING
    → Existing Standard objects moved to IT after N days
    → Then IT auto-manages tier changes
    
    INTELLIGENT TIERING CONFIGURATION:
    → Can configure which archive tiers to opt-in to
    → By default: only Frequent Access and Infrequent Access tiers
    → Opt-in: Archive Instant Access (90 days), Archive Access (180 days),
              Deep Archive Access (180+ days)
    """
    s3_client = boto3.client("s3", region_name=REGION)

    # --- CREATE INTELLIGENT TIERING CONFIGURATION ---
    # This controls which archive tiers IT can use
    config_name = "QuickCart-Auto-Tiering"

    tiering_config = {
        "Id": config_name,
        "Status": "Enabled",
        "Filter": {
            # Apply to specific prefix
            # Use {} for entire bucket
            "Prefix": "marketing_data/"
        },
        "Tierings": [
            {
                # After 90 days no access → move to Archive Instant Access
                # Same as Glacier IR pricing but auto-managed
                "AccessTier": "ARCHIVE_ACCESS",
                "Days": 90
            },
            {
                # After 180 days no access → move to Deep Archive Access
                # Same as Glacier Deep Archive pricing but auto-managed
                "AccessTier": "DEEP_ARCHIVE_ACCESS",
                "Days": 180
            }
        ]
    }

    try:
        s3_client.put_bucket_intelligent_tiering_configuration(
            Bucket=bucket_name,
            Id=config_name,
            IntelligentTieringConfiguration=tiering_config
        )
        print(f"✅ Intelligent Tiering config '{config_name}' created")
        print(f"   Applies to: marketing_data/")
        print(f"   Archive Access tier: after 90 days no access")
        print(f"   Deep Archive Access tier: after 180 days no access")
        return True

    except ClientError as e:
        print(f"❌ Failed: {e}")
        return False


def demonstrate_upload_to_intelligent_tiering(bucket_name):
    """Upload an object directly to Intelligent Tiering class"""
    s3_client = boto3.client("s3", region_name=REGION)

    key = "marketing_data/campaigns/campaign_2025q1.csv"
    content = "campaign_id,name,budget,status\nC001,Summer Sale,50000,active"

    print(f"\n📤 Uploading to Intelligent Tiering: {key}")

    s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/csv",
        StorageClass="INTELLIGENT_TIERING",
        ServerSideEncryption="AES256"
    )

    # Verify
    response = s3_client.head_object(Bucket=bucket_name, Key=key)
    print(f"   Storage Class: {response.get('StorageClass', 'STANDARD')}")
    print(f"   ✅ Object will auto-tier based on access patterns")
    print(f"   → Accessed frequently → stays in Frequent Access tier")
    print(f"   → Not accessed 30 days → moves to Infrequent Access tier")
    print(f"   → Not accessed 90 days → moves to Archive Access tier")
    print(f"   → Accessed once at any tier → moves back to Frequent Access")


def compare_lifecycle_vs_intelligent_tiering():
    """
    Decision matrix: when to use each approach
    INTERVIEW GOLD — frequently asked
    """
    print("\n" + "=" * 80)
    print("📊 LIFECYCLE RULES vs INTELLIGENT TIERING — DECISION MATRIX")
    print("=" * 80)

    comparisons = [
        ("Access pattern",
         "Known/predictable",
         "Unknown/variable"),
        ("Transition trigger",
         "Object age (creation date)",
         "Actual access frequency"),
        ("Monitoring cost",
         "None",
         "$0.0025 per 1000 objects/mo"),
        ("Retrieval cost",
         "Yes (IA: $0.01/GB, Glacier: varies)",
         "None (auto-managed tiers)"),
        ("Archive tiers",
         "All (IA, Glacier IR, Flex, Deep)",
         "Freq, Infreq, Archive, Deep Archive"),
        ("Granularity",
         "Per-prefix or per-tag",
         "Per-object"),
        ("Backward movement",
         "Not possible via lifecycle",
         "Automatic on access"),
        ("Best for",
         "Daily exports, known lifecycle",
         "Ad-hoc data, unpredictable usage"),
        ("Minimum object size",
         "128KB (for IA/Glacier)",
         "128KB (smaller objects stay Frequent)"),
        ("Setup complexity",
         "Manual rule design",
         "Set and forget"),
        ("QuickCart use case",
         "daily_exports/ (known pattern)",
         "marketing_data/ (variable pattern)")
    ]

    print(f"\n  {'Dimension':<25} {'Lifecycle Rules':<30} {'Intelligent Tiering':<30}")
    print("  " + "-" * 85)

    for dimension, lifecycle, it in comparisons:
        print(f"  {dimension:<25} {lifecycle:<30} {it:<30}")

    print("\n  💡 RECOMMENDATION:")
    print("  → Use BOTH — lifecycle for predictable data, IT for unpredictable")
    print("  → QuickCart: lifecycle for daily_exports/, IT for marketing_data/")
    print("=" * 80)


if __name__ == "__main__":
    print("=" * 60)
    print("🧠 S3 INTELLIGENT TIERING")
    print("=" * 60)

    configure_intelligent_tiering(BUCKET_NAME)
    demonstrate_upload_to_intelligent_tiering(BUCKET_NAME)
    compare_lifecycle_vs_intelligent_tiering()