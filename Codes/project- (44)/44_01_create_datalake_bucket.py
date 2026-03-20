# file: 44_01_create_datalake_bucket.py
# Purpose: Create the new data lake bucket with proper structure,
#          encryption, lifecycle, and access configuration

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
DATALAKE_BUCKET = "quickcart-datalake-prod"

# Zone definitions with their properties
ZONES = {
    "bronze": {
        "description": "Raw data — exact copy from source systems",
        "lifecycle_standard_days": 30,
        "lifecycle_ia_days": 90,
        "lifecycle_glacier_days": None,
        "lifecycle_expiry_days": 365,
        "subprefixes": [
            "bronze/mariadb/full_dump/orders/",
            "bronze/mariadb/full_dump/customers/",
            "bronze/mariadb/full_dump/products/",
            "bronze/mariadb/cdc/orders/",
            "bronze/mariadb/cdc/customers/",
            "bronze/mariadb/cdc/products/",
            "bronze/redshift/customer_metrics/",
            "bronze/redshift/customer_recommendations/"
        ]
    },
    "silver": {
        "description": "Cleansed data — typed, deduplicated, Parquet",
        "lifecycle_standard_days": 60,
        "lifecycle_ia_days": 120,
        "lifecycle_glacier_days": None,
        "lifecycle_expiry_days": 180,
        "subprefixes": [
            "silver/orders/",
            "silver/customers/",
            "silver/products/"
        ]
    },
    "gold": {
        "description": "Curated data — business aggregations, ready for consumption",
        "lifecycle_standard_days": 90,
        "lifecycle_ia_days": 180,
        "lifecycle_glacier_days": None,
        "lifecycle_expiry_days": 365,
        "subprefixes": [
            "gold/customer_360/",
            "gold/revenue_daily/",
            "gold/product_performance/",
            "gold/recommendation_scores/"
        ]
    },
    "_system": {
        "description": "Operational files — scripts, temp, results",
        "lifecycle_standard_days": 7,
        "lifecycle_ia_days": None,
        "lifecycle_glacier_days": None,
        "lifecycle_expiry_days": 30,
        "subprefixes": [
            "_system/glue-temp/",
            "_system/glue-scripts/",
            "_system/glue-libs/",
            "_system/athena-results/",
            "_system/logs/"
        ]
    }
}


def create_datalake_bucket():
    """Create the data lake bucket with all configurations"""
    s3_client = boto3.client("s3", region_name=REGION)

    # ━━━ CREATE BUCKET ━━━
    try:
        if REGION == "us-east-1":
            s3_client.create_bucket(Bucket=DATALAKE_BUCKET)
        else:
            s3_client.create_bucket(
                Bucket=DATALAKE_BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        print(f"✅ Bucket created: {DATALAKE_BUCKET}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Bucket already exists: {DATALAKE_BUCKET}")

    # ━━━ BLOCK PUBLIC ACCESS ━━━
    s3_client.put_public_access_block(
        Bucket=DATALAKE_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    )
    print("✅ Public access blocked")

    # ━━━ ENABLE VERSIONING ━━━
    s3_client.put_bucket_versioning(
        Bucket=DATALAKE_BUCKET,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print("✅ Versioning enabled")

    # ━━━ ENABLE ENCRYPTION ━━━
    s3_client.put_bucket_encryption(
        Bucket=DATALAKE_BUCKET,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True
            }]
        }
    )
    print("✅ Server-side encryption (AES256) enabled")

    # ━━━ ENABLE EVENTBRIDGE ━━━
    current_notification = s3_client.get_bucket_notification_configuration(
        Bucket=DATALAKE_BUCKET
    )
    current_notification.pop("ResponseMetadata", None)
    current_notification["EventBridgeConfiguration"] = {}
    s3_client.put_bucket_notification_configuration(
        Bucket=DATALAKE_BUCKET,
        NotificationConfiguration=current_notification
    )
    print("✅ EventBridge notifications enabled")

    # ━━━ TAGS ━━━
    s3_client.put_bucket_tagging(
        Bucket=DATALAKE_BUCKET,
        Tagging={
            "TagSet": [
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Project", "Value": "QuickCart-DataLake"},
                {"Key": "Architecture", "Value": "Medallion"},
                {"Key": "CostCenter", "Value": "ENG-001"}
            ]
        }
    )
    print("✅ Bucket tagged")

    return DATALAKE_BUCKET


def create_zone_marker_objects():
    """
    Create 0-byte marker objects for each zone prefix.
    
    WHY:
    → S3 console shows "folders" for navigation
    → Documents the zone structure visually
    → Some tools need prefix to exist before writing
    """
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n📁 Creating zone structure...")

    for zone_name, zone_config in ZONES.items():
        for prefix in zone_config["subprefixes"]:
            # Create marker object (0 bytes, acts as "folder")
            s3_client.put_object(
                Bucket=DATALAKE_BUCKET,
                Key=prefix,
                Body=b"",
                Metadata={
                    "zone": zone_name,
                    "description": zone_config["description"],
                    "created-by": "datalake-setup-script"
                }
            )
        print(f"   ✅ {zone_name}/: {len(zone_config['subprefixes'])} prefixes created")


def configure_lifecycle_rules():
    """
    Configure S3 Lifecycle rules per zone.
    
    Each zone gets its own rule with:
    → Transition to Infrequent Access after N days
    → Expiration (deletion) after M days
    → Applied to all objects under the zone prefix
    """
    s3_client = boto3.client("s3", region_name=REGION)

    rules = []

    for zone_name, zone_config in ZONES.items():
        rule = {
            "ID": f"lifecycle-{zone_name}",
            "Filter": {"Prefix": f"{zone_name}/"},
            "Status": "Enabled",
            "Transitions": [],
            "NoncurrentVersionTransitions": [],
        }

        # Transition to Infrequent Access
        ia_days = zone_config.get("lifecycle_ia_days")
        if ia_days:
            rule["Transitions"].append({
                "Days": ia_days,
                "StorageClass": "STANDARD_IA"
            })

        # Transition to Glacier
        glacier_days = zone_config.get("lifecycle_glacier_days")
        if glacier_days:
            rule["Transitions"].append({
                "Days": glacier_days,
                "StorageClass": "GLACIER"
            })

        # Expiration
        expiry_days = zone_config.get("lifecycle_expiry_days")
        if expiry_days:
            rule["Expiration"] = {"Days": expiry_days}

        # Old versions (since versioning is enabled)
        rule["NoncurrentVersionExpiration"] = {
            "NoncurrentDays": 30  # Delete old versions after 30 days
        }

        # Clean up incomplete multipart uploads
        rule["AbortIncompleteMultipartUpload"] = {
            "DaysAfterInitiation": 7
        }

        rules.append(rule)

    # Special rule: clean _system/glue-temp/ aggressively
    rules.append({
        "ID": "cleanup-glue-temp",
        "Filter": {"Prefix": "_system/glue-temp/"},
        "Status": "Enabled",
        "Expiration": {"Days": 3},
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}
    })

    # Special rule: clean _system/athena-results/
    rules.append({
        "ID": "cleanup-athena-results",
        "Filter": {"Prefix": "_system/athena-results/"},
        "Status": "Enabled",
        "Expiration": {"Days": 14}
    })

    s3_client.put_bucket_lifecycle_configuration(
        Bucket=DATALAKE_BUCKET,
        LifecycleConfiguration={"Rules": rules}
    )

    print(f"\n✅ Lifecycle rules configured:")
    for rule in rules:
        transitions = [f"{t['StorageClass']} after {t['Days']}d"
                       for t in rule.get("Transitions", [])]
        expiry = rule.get("Expiration", {}).get("Days", "never")
        print(f"   {rule['ID']}: transitions={transitions}, expiry={expiry}d")


def setup_datalake():
    """Complete data lake setup"""
    print("=" * 70)
    print("🏗️  DATA LAKE SETUP — MEDALLION ARCHITECTURE")
    print("=" * 70)

    create_datalake_bucket()
    create_zone_marker_objects()
    configure_lifecycle_rules()

    print(f"\n{'='*70}")
    print(f"✅ DATA LAKE READY: s3://{DATALAKE_BUCKET}/")
    print(f"{'='*70}")
    print(f"   bronze/  → Raw data from sources")
    print(f"   silver/  → Cleansed, typed, deduplicated Parquet")
    print(f"   gold/    → Business aggregations, consumption-ready")
    print(f"   _system/ → Operational files (scripts, temp, logs)")


if __name__ == "__main__":
    setup_datalake()