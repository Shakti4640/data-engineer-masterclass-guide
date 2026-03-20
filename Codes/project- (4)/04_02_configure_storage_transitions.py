# file: 18_configure_storage_transitions.py
# Purpose: Add lifecycle rules for CURRENT version transitions
# DEPENDS ON: Project 2 (existing lifecycle for non-current versions)
# NOTE: This MERGES with existing rules — does not replace them

import boto3
import json
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def build_complete_lifecycle_config():
    """
    Complete lifecycle configuration combining:
    → Project 2 rules (non-current version management)
    → Project 4 rules (current version transitions)
    
    WHY REBUILD ENTIRE CONFIG:
    → put_bucket_lifecycle_configuration REPLACES all rules
    → Cannot add a single rule — must submit complete config
    → Always read existing, merge, and write back
    
    ALTERNATIVELY: read existing with get_bucket_lifecycle_configuration,
    append new rules, and put back — shown in merge_lifecycle_rules()
    """
    lifecycle_config = {
        "Rules": [
            # ══════════════════════════════════════════════════════════
            # FROM PROJECT 2: Non-current version management
            # (Referenced — not repeating explanation from Project 2)
            # ══════════════════════════════════════════════════════════
            {
                "ID": "manage-noncurrent-versions",
                "Status": "Enabled",
                "Filter": {
                    "Prefix": "daily_exports/"
                },
                "NoncurrentVersionTransitions": [
                    {
                        "NoncurrentDays": 30,
                        "StorageClass": "STANDARD_IA"
                    },
                    {
                        "NoncurrentDays": 60,
                        "StorageClass": "GLACIER_IR"
                    }
                ],
                "NoncurrentVersionExpiration": {
                    "NoncurrentDays": 90
                }
            },
            {
                "ID": "cleanup-delete-markers",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "Expiration": {
                    "ExpiredObjectDeleteMarker": True
                }
            },
            {
                "ID": "abort-incomplete-multipart",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 7
                }
            },

            # ══════════════════════════════════════════════════════════
            # NEW IN PROJECT 4: Current version transitions
            # These manage ACTIVE files as they age
            # ══════════════════════════════════════════════════════════
            {
                "ID": "transition-daily-exports-current",
                "Status": "Enabled",
                "Filter": {
                    "And": {
                        "Prefix": "daily_exports/",
                        # Only transition files > 128KB
                        # Smaller files cost MORE in IA/Glacier
                        "ObjectSizeGreaterThan": 131072   # 128 KB
                    }
                },
                "Transitions": [
                    {
                        # Day 30: Standard → Standard-IA
                        # 46% storage savings, instant access, $0.01/GB retrieval
                        "Days": 30,
                        "StorageClass": "STANDARD_IA"
                    },
                    {
                        # Day 90: Standard-IA → Glacier IR
                        # 83% storage savings, instant access, $0.03/GB retrieval
                        "Days": 90,
                        "StorageClass": "GLACIER_IR"
                    },
                    {
                        # Day 365: Glacier IR → Glacier Flexible
                        # 84% savings, 3-5 hour retrieval
                        "Days": 365,
                        "StorageClass": "GLACIER"
                    }
                ],
                "Expiration": {
                    # Day 2555: Delete after 7 years (compliance requirement)
                    "Days": 2555
                }
            },

            # Finance reports: same transition but longer retention
            {
                "ID": "transition-finance-reports-current",
                "Status": "Enabled",
                "Filter": {
                    "And": {
                        "Prefix": "finance_reports/",
                        "ObjectSizeGreaterThan": 131072
                    }
                },
                "Transitions": [
                    {
                        "Days": 30,
                        "StorageClass": "STANDARD_IA"
                    },
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER_IR"
                    },
                    {
                        "Days": 365,
                        "StorageClass": "DEEP_ARCHIVE"
                    }
                ],
                "Expiration": {
                    # Finance records: 10 year retention
                    "Days": 3650
                }
            },

            # Marketing data: shorter retention, no deep archive needed
            {
                "ID": "transition-marketing-data-current",
                "Status": "Enabled",
                "Filter": {
                    "And": {
                        "Prefix": "marketing_data/",
                        "ObjectSizeGreaterThan": 131072
                    }
                },
                "Transitions": [
                    {
                        "Days": 30,
                        "StorageClass": "STANDARD_IA"
                    },
                    {
                        "Days": 90,
                        "StorageClass": "GLACIER_IR"
                    }
                ],
                "Expiration": {
                    # Marketing data: 2 year retention
                    "Days": 730
                }
            }
        ]
    }

    return lifecycle_config


def apply_lifecycle_config(bucket_name, config):
    s3_client = boto3.client("s3", region_name=REGION)

    try:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=config
        )
        print("✅ Lifecycle configuration applied successfully")
        return True
    except ClientError as e:
        print(f"❌ Failed: {e}")
        return False


def display_lifecycle_summary(bucket_name):
    """Display all lifecycle rules in a readable format"""
    s3_client = boto3.client("s3", region_name=REGION)

    response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)

    print(f"\n📋 LIFECYCLE RULES SUMMARY — {bucket_name}")
    print("=" * 75)

    for rule in response["Rules"]:
        rule_id = rule["ID"]
        status = rule["Status"]

        # Get filter info
        filter_info = rule.get("Filter", {})
        prefix = ""
        min_size = 0
        if "And" in filter_info:
            prefix = filter_info["And"].get("Prefix", "")
            min_size = filter_info["And"].get("ObjectSizeGreaterThan", 0)
        elif "Prefix" in filter_info:
            prefix = filter_info["Prefix"]

        print(f"\n  📌 {rule_id} [{status}]")
        print(f"     Prefix: {prefix if prefix else '(entire bucket)'}")
        if min_size > 0:
            print(f"     Min Size: {min_size / 1024:.0f} KB")

        # Current version transitions
        if "Transitions" in rule:
            print(f"     Current Version Transitions:")
            for t in rule["Transitions"]:
                print(f"       → Day {t['Days']:>4}: move to {t['StorageClass']}")

        # Non-current version transitions
        if "NoncurrentVersionTransitions" in rule:
            print(f"     Non-Current Version Transitions:")
            for t in rule["NoncurrentVersionTransitions"]:
                print(f"       → Day {t['NoncurrentDays']:>4} after becoming non-current: "
                      f"move to {t['StorageClass']}")

        # Expiration
        if "Expiration" in rule:
            exp = rule["Expiration"]
            if "Days" in exp:
                years = exp["Days"] / 365
                print(f"     Expiration: Day {exp['Days']} (~{years:.1f} years)")
            if exp.get("ExpiredObjectDeleteMarker"):
                print(f"     Cleanup: expired delete markers")

        # Non-current expiration
        if "NoncurrentVersionExpiration" in rule:
            days = rule["NoncurrentVersionExpiration"]["NoncurrentDays"]
            print(f"     Non-Current Expiration: {days} days after becoming non-current")

        # Multipart
        if "AbortIncompleteMultipartUpload" in rule:
            days = rule["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"]
            print(f"     Abort Multipart: after {days} days")

    print("\n" + "=" * 75)


def calculate_projected_savings(bucket_name):
    """Project cost savings over time"""
    print("\n💰 PROJECTED COST SAVINGS")
    print("=" * 75)

    # Assumptions
    scenarios = [
        {"label": "Year 1", "total_gb": 22, "hot_pct": 15, "warm_pct": 25,
         "cool_pct": 35, "cold_pct": 25, "archive_pct": 0},
        {"label": "Year 2", "total_gb": 72, "hot_pct": 8, "warm_pct": 15,
         "cool_pct": 25, "cold_pct": 30, "archive_pct": 22},
        {"label": "Year 3", "total_gb": 172, "hot_pct": 5, "warm_pct": 10,
         "cool_pct": 20, "cold_pct": 30, "archive_pct": 35},
        {"label": "Year 5", "total_gb": 472, "hot_pct": 3, "warm_pct": 7,
         "cool_pct": 15, "cold_pct": 30, "archive_pct": 45},
        {"label": "Year 7", "total_gb": 872, "hot_pct": 2, "warm_pct": 5,
         "cool_pct": 10, "cold_pct": 25, "archive_pct": 58},
    ]

    rates = {
        "Standard": 0.023,
        "S3-IA": 0.0125,
        "Glacier IR": 0.004,
        "Glacier Flex": 0.0036,
        "Deep Archive": 0.00099
    }

    print(f"  {'Period':<10} {'Total GB':>10} {'All Standard':>14} "
          f"{'With Lifecycle':>16} {'Savings':>10} {'Savings %':>10}")
    print("  " + "-" * 72)

    for s in scenarios:
        all_standard = s["total_gb"] * 0.023

        hot = s["total_gb"] * s["hot_pct"] / 100 * rates["Standard"]
        warm = s["total_gb"] * s["warm_pct"] / 100 * rates["S3-IA"]
        cool = s["total_gb"] * s["cool_pct"] / 100 * rates["Glacier IR"]
        cold = s["total_gb"] * s["cold_pct"] / 100 * rates["Glacier Flex"]
        archive = s["total_gb"] * s["archive_pct"] / 100 * rates["Deep Archive"]
        with_lifecycle = hot + warm + cool + cold + archive

        savings = all_standard - with_lifecycle
        savings_pct = (savings / all_standard * 100) if all_standard > 0 else 0

        print(f"  {s['label']:<10} {s['total_gb']:>8} GB ${all_standard:>11.2f}/mo "
              f"${with_lifecycle:>13.2f}/mo ${savings:>8.2f} {savings_pct:>8.1f}%")

    print("\n  💡 At scale (10+ TB), lifecycle transitions save thousands per month")
    print("=" * 75)


if __name__ == "__main__":
    print("=" * 75)
    print("♻️  CONFIGURING STORAGE CLASS TRANSITIONS")
    print(f"   Bucket: {BUCKET_NAME}")
    print("=" * 75)

    config = build_complete_lifecycle_config()

    # Count rules
    print(f"\n📋 Total lifecycle rules: {len(config['Rules'])}")
    print("   Rules from Project 2: 3 (non-current versions, markers, multipart)")
    print("   Rules from Project 4: 3 (current version transitions per prefix)")

    apply_lifecycle_config(BUCKET_NAME, config)
    display_lifecycle_summary(BUCKET_NAME)
    calculate_projected_savings(BUCKET_NAME)