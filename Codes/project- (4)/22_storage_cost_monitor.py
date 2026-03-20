# file: 22_storage_cost_monitor.py
# Purpose: Monitor storage class distribution and costs over time
# DEPENDS ON: All previous steps in Project 4
# USE WHEN: Monthly cost review, CFO reporting, optimization validation

import boto3
from collections import defaultdict
from datetime import datetime, timezone

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"

# S3 pricing (us-east-2) — update as needed
PRICING = {
    "STANDARD":             {"storage": 0.023,   "retrieval": 0.000},
    "INTELLIGENT_TIERING":  {"storage": 0.023,   "retrieval": 0.000},
    "STANDARD_IA":          {"storage": 0.0125,  "retrieval": 0.010},
    "ONEZONE_IA":           {"storage": 0.010,   "retrieval": 0.010},
    "GLACIER_IR":           {"storage": 0.004,   "retrieval": 0.030},
    "GLACIER":              {"storage": 0.0036,  "retrieval": 0.010},
    "DEEP_ARCHIVE":         {"storage": 0.00099, "retrieval": 0.020},
}


def generate_cost_report(bucket_name):
    """
    Generate detailed storage cost report
    Shows: actual cost vs what it WOULD cost if everything was Standard
    """
    s3_client = boto3.client("s3", region_name=REGION)
    paginator = s3_client.get_paginator("list_objects_v2")

    # Aggregation
    by_class = defaultdict(lambda: {"count": 0, "size_bytes": 0})
    by_prefix_class = defaultdict(lambda: defaultdict(lambda: {"count": 0, "size_bytes": 0}))
    total_objects = 0
    total_bytes = 0

    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            storage_class = obj.get("StorageClass", "STANDARD")
            size = obj["Size"]
            key = obj["Key"]

            total_objects += 1
            total_bytes += size

            by_class[storage_class]["count"] += 1
            by_class[storage_class]["size_bytes"] += size

            # Top-level prefix
            top_prefix = key.split("/")[0] if "/" in key else "(root)"
            by_prefix_class[top_prefix][storage_class]["count"] += 1
            by_prefix_class[top_prefix][storage_class]["size_bytes"] += size

    # --- REPORT ---
    now = datetime.now(timezone.utc)
    total_gb = total_bytes / (1024 ** 3)

    print("=" * 80)
    print(f"💰 S3 STORAGE COST REPORT — {bucket_name}")
    print(f"   Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 80)

    # Overall summary
    actual_cost = 0
    standard_cost = total_gb * PRICING["STANDARD"]["storage"]

    print(f"\n📦 OVERALL SUMMARY:")
    print(f"   Total Objects: {total_objects:,}")
    print(f"   Total Size: {total_gb:.4f} GB ({total_bytes:,} bytes)")

    # By storage class
    print(f"\n📋 COST BY STORAGE CLASS:")
    print(f"   {'Class':<25} {'Objects':>8} {'Size (MB)':>12} {'$/GB/mo':>10} {'Cost/mo':>12}")
    print("   " + "-" * 70)

    for cls in sorted(by_class.keys()):
        data = by_class[cls]
        size_mb = data["size_bytes"] / (1024 ** 2)
        size_gb = data["size_bytes"] / (1024 ** 3)
        rate = PRICING.get(cls, PRICING["STANDARD"])["storage"]
        cost = size_gb * rate
        actual_cost += cost

        print(f"   {cls:<25} {data['count']:>8,} {size_mb:>10,.2f} MB "
              f"${rate:>8.5f} ${cost:>10.6f}")

    print("   " + "-" * 70)
    print(f"   {'ACTUAL TOTAL':<25} {total_objects:>8,} {total_gb * 1024:>10,.2f} MB "
          f"{'':>10} ${actual_cost:>10.6f}")
    print(f"   {'IF ALL STANDARD':<25} {'':>8} {'':>12} "
          f"{'':>10} ${standard_cost:>10.6f}")

    savings = standard_cost - actual_cost
    savings_pct = (savings / standard_cost * 100) if standard_cost > 0 else 0
    print(f"\n   💰 Monthly Savings: ${savings:.6f} ({savings_pct:.1f}%)")

    # By prefix
    print(f"\n📋 COST BY PREFIX:")
    print(f"   {'Prefix':<20} {'Class':<22} {'Objects':>8} {'Size (MB)':>10} {'Cost/mo':>10}")
    print("   " + "-" * 72)

    for prefix in sorted(by_prefix_class.keys()):
        prefix_total_cost = 0
        for cls, data in sorted(by_prefix_class[prefix].items()):
            size_mb = data["size_bytes"] / (1024 ** 2)
            size_gb = data["size_bytes"] / (1024 ** 3)
            rate = PRICING.get(cls, PRICING["STANDARD"])["storage"]
            cost = size_gb * rate
            prefix_total_cost += cost

            print(f"   {prefix:<20} {cls:<22} {data['count']:>8,} "
                  f"{size_mb:>8,.2f} MB ${cost:>8.6f}")

        print(f"   {'':>20} {'SUBTOTAL':<22} {'':>8} {'':>10} ${prefix_total_cost:>8.6f}")
        print("   " + "-" * 72)

    # Annual projection
    print(f"\n📈 ANNUAL PROJECTION:")
    print(f"   Current monthly (actual): ${actual_cost:.4f}")
    print(f"   Current monthly (if all Standard): ${standard_cost:.4f}")
    print(f"   Annual savings: ${savings * 12:.4f}")

    # Scaling projection
    print(f"\n📈 AT SCALE (1 TB total, same distribution):")
    scale_factor = 1024 / total_gb if total_gb > 0 else 1
    print(f"   With lifecycle: ${actual_cost * scale_factor:.2f}/month")
    print(f"   Without lifecycle: ${standard_cost * scale_factor:.2f}/month")
    print(f"   Annual savings at 1 TB: ${savings * scale_factor * 12:.2f}")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    generate_cost_report(BUCKET_NAME)