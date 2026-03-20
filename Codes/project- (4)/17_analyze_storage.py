# file: 17_analyze_storage.py
# Purpose: Analyze current storage distribution before applying transitions
# DEPENDS ON: Project 1 (bucket with data), Project 2 (versioning)

import boto3
from collections import defaultdict
from datetime import datetime, timezone

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"


def analyze_storage_distribution(bucket_name):
    """
    Analyze storage by:
    → Storage class
    → Prefix (entity type)
    → Age bucket (0-30d, 30-90d, 90-365d, 365+d)
    
    This is what you'd present to CFO before implementing transitions
    """
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    # Aggregation containers
    by_class = defaultdict(lambda: {"count": 0, "size": 0})
    by_prefix = defaultdict(lambda: {"count": 0, "size": 0})
    by_age = defaultdict(lambda: {"count": 0, "size": 0})

    now = datetime.now(timezone.utc)
    total_objects = 0
    total_size = 0

    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            storage_class = obj.get("StorageClass", "STANDARD")
            last_modified = obj["LastModified"]

            total_objects += 1
            total_size += size

            # By storage class
            by_class[storage_class]["count"] += 1
            by_class[storage_class]["size"] += size

            # By top-level prefix
            top_prefix = key.split("/")[0] if "/" in key else "(root)"
            by_prefix[top_prefix]["count"] += 1
            by_prefix[top_prefix]["size"] += size

            # By age
            age_days = (now - last_modified).days
            if age_days <= 30:
                age_bucket = "0-30 days (HOT)"
            elif age_days <= 90:
                age_bucket = "31-90 days (WARM)"
            elif age_days <= 365:
                age_bucket = "91-365 days (COOL)"
            else:
                age_bucket = "365+ days (ARCHIVE)"

            by_age[age_bucket]["count"] += 1
            by_age[age_bucket]["size"] += size

    # --- PRINT REPORT ---
    total_gb = total_size / (1024 ** 3)
    monthly_cost_standard = total_gb * 0.023

    print("=" * 70)
    print(f"📊 STORAGE ANALYSIS REPORT — {bucket_name}")
    print(f"   Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    print(f"\n📦 TOTALS:")
    print(f"   Objects: {total_objects:,}")
    print(f"   Size: {total_gb:.2f} GB")
    print(f"   Current monthly cost (all Standard): ${monthly_cost_standard:.2f}")

    # By Storage Class
    print(f"\n📋 BY STORAGE CLASS:")
    print(f"   {'Class':<25} {'Objects':>10} {'Size (MB)':>12} {'Monthly $':>10}")
    print("   " + "-" * 60)
    for cls, data in sorted(by_class.items()):
        size_mb = data["size"] / (1024 ** 2)
        # Approximate cost by class
        cost_rates = {
            "STANDARD": 0.023, "STANDARD_IA": 0.0125,
            "ONEZONE_IA": 0.010, "INTELLIGENT_TIERING": 0.023,
            "GLACIER_IR": 0.004, "GLACIER": 0.0036,
            "DEEP_ARCHIVE": 0.00099
        }
        rate = cost_rates.get(cls, 0.023)
        monthly = (data["size"] / (1024 ** 3)) * rate
        print(f"   {cls:<25} {data['count']:>10,} {size_mb:>10,.1f} MB ${monthly:>8.4f}")

    # By Prefix
    print(f"\n📋 BY PREFIX:")
    print(f"   {'Prefix':<25} {'Objects':>10} {'Size (MB)':>12}")
    print("   " + "-" * 50)
    for prefix, data in sorted(by_prefix.items()):
        size_mb = data["size"] / (1024 ** 2)
        print(f"   {prefix:<25} {data['count']:>10,} {size_mb:>10,.1f} MB")

    # By Age
    print(f"\n📋 BY AGE (this drives transition decisions):")
    print(f"   {'Age Bucket':<25} {'Objects':>10} {'Size (MB)':>12} {'% of Total':>12}")
    print("   " + "-" * 62)
    for age, data in sorted(by_age.items()):
        size_mb = data["size"] / (1024 ** 2)
        pct = (data["size"] / total_size * 100) if total_size > 0 else 0
        print(f"   {age:<25} {data['count']:>10,} {size_mb:>10,.1f} MB {pct:>10.1f}%")

    # Optimization recommendation
    print(f"\n💡 OPTIMIZATION RECOMMENDATION:")
    for age, data in by_age.items():
        size_gb = data["size"] / (1024 ** 3)
        current_cost = size_gb * 0.023
        if "HOT" in age:
            target = "Standard (no change)"
            target_cost = current_cost
        elif "WARM" in age:
            target = "Standard-IA"
            target_cost = size_gb * 0.0125
        elif "COOL" in age:
            target = "Glacier IR"
            target_cost = size_gb * 0.004
        else:
            target = "Glacier Flexible"
            target_cost = size_gb * 0.0036

        savings = current_cost - target_cost
        print(f"   {age:<25} → {target:<20} "
              f"Save: ${savings:.4f}/mo")

    print("\n" + "=" * 70)
    return by_age


if __name__ == "__main__":
    analyze_storage_distribution(BUCKET_NAME)