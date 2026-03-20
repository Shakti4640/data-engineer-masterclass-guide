# file: 44_06_datalake_health_check.py
# Purpose: Verify data lake structure, freshness, and zone health

import boto3
from datetime import datetime, timedelta
from collections import defaultdict

REGION = "us-east-2"
DATALAKE_BUCKET = "quickcart-datalake-prod"

ZONES = ["bronze", "silver", "gold", "_system"]

EXPECTED_ENTITIES = {
    "bronze": [
        "bronze/mariadb/cdc/orders/",
        "bronze/mariadb/cdc/customers/",
        "bronze/mariadb/cdc/products/"
    ],
    "silver": [
        "silver/orders/",
        "silver/customers/",
        "silver/products/"
    ],
    "gold": [
        "gold/customer_360/",
        "gold/revenue_daily/",
        "gold/product_performance/"
    ]
}


def check_zone_health():
    """Comprehensive data lake health check"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print(f"🏥 DATA LAKE HEALTH CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Bucket: s3://{DATALAKE_BUCKET}/")
    print("=" * 70)

    zone_stats = {}

    for zone in ZONES:
        print(f"\n{'─'*60}")
        print(f"📋 ZONE: {zone}/")
        print(f"{'─'*60}")

        # Count objects and total size per zone
        paginator = s3_client.get_paginator("list_objects_v2")
        total_objects = 0
        total_size = 0
        latest_modified = None
        file_types = defaultdict(int)

        for page in paginator.paginate(Bucket=DATALAKE_BUCKET, Prefix=f"{zone}/"):
            for obj in page.get("Contents", []):
                total_objects += 1
                total_size += obj["Size"]

                # Track latest modification
                if latest_modified is None or obj["LastModified"] > latest_modified:
                    latest_modified = obj["LastModified"]

                # Track file types
                key = obj["Key"]
                if key.endswith(".parquet"):
                    file_types["parquet"] += 1
                elif key.endswith(".csv"):
                    file_types["csv"] += 1
                elif key.endswith(".json"):
                    file_types["json"] += 1
                elif key.endswith(".py"):
                    file_types["python"] += 1
                elif obj["Size"] == 0:
                    file_types["marker"] += 1
                else:
                    file_types["other"] += 1

        total_mb = round(total_size / (1024 * 1024), 2)
        total_gb = round(total_size / (1024 * 1024 * 1024), 3)

        print(f"   Objects: {total_objects:,}")
        print(f"   Size: {total_mb:,.2f} MB ({total_gb:.3f} GB)")
        print(f"   File types: {dict(file_types)}")

        if latest_modified:
            age_hours = (datetime.now(latest_modified.tzinfo) - latest_modified).total_seconds() / 3600
            freshness = "✅ FRESH" if age_hours < 26 else "❌ STALE"
            print(f"   Latest update: {latest_modified.strftime('%Y-%m-%d %H:%M')} ({age_hours:.1f}h ago) {freshness}")
        else:
            print(f"   Latest update: NO DATA")

        zone_stats[zone] = {
            "objects": total_objects,
            "size_mb": total_mb,
            "latest": latest_modified,
            "file_types": dict(file_types)
        }

        # Format compliance check (Silver/Gold must be Parquet)
        if zone in ("silver", "gold"):
            non_parquet = sum(v for k, v in file_types.items() if k not in ("parquet", "marker"))
            if non_parquet > 0:
                print(f"   ⚠️  FORMAT VIOLATION: {non_parquet} non-Parquet files in {zone}/")
                print(f"      {zone}/ zone should contain ONLY Parquet files")
            else:
                print(f"   ✅ Format compliance: all Parquet")

    # ━━━ ENTITY FRESHNESS CHECK ━━━
    print(f"\n{'─'*60}")
    print(f"📋 ENTITY FRESHNESS")
    print(f"{'─'*60}")

    today = datetime.now().strftime("%Y-%m-%d")
    year, month, day = today.split("-")

    for zone, prefixes in EXPECTED_ENTITIES.items():
        for prefix in prefixes:
            entity_path = f"{prefix}year={year}/month={month}/day={day}/"
            response = s3_client.list_objects_v2(
                Bucket=DATALAKE_BUCKET,
                Prefix=entity_path,
                MaxKeys=1
            )
            has_today = response.get("KeyCount", 0) > 0
            status = "✅" if has_today else "❌"
            entity_name = prefix.rstrip("/").split("/")[-1]
            print(f"   {status} {zone}/{entity_name}: {'data for today' if has_today else 'NO data for today'}")

    # ━━━ COST ESTIMATE ━━━
    print(f"\n{'─'*60}")
    print(f"💰 STORAGE COST ESTIMATE (current month)")
    print(f"{'─'*60}")

    total_cost = 0
    for zone, stats in zone_stats.items():
        gb = stats["size_mb"] / 1024
        cost = gb * 0.023  # S3 Standard rate
        total_cost += cost
        print(f"   {zone}: {gb:.3f} GB → ${cost:.4f}/month")
    print(f"   {'─'*40}")
    print(f"   TOTAL: ${total_cost:.4f}/month")

    # ━━━ OVERALL HEALTH ━━━
    print(f"\n{'='*70}")
    issues = []
    for zone in ["bronze", "silver", "gold"]:
        stats = zone_stats.get(zone, {})
        if stats.get("objects", 0) == 0:
            issues.append(f"{zone}/ is EMPTY")
        latest = stats.get("latest")
        if latest:
            age = (datetime.now(latest.tzinfo) - latest).total_seconds() / 3600
            if age > 26:
                issues.append(f"{zone}/ is STALE ({age:.0f}h old)")

    if not issues:
        print(f"✅ DATA LAKE HEALTH: GOOD")
    else:
        print(f"⚠️  DATA LAKE HEALTH: ISSUES FOUND")
        for issue in issues:
            print(f"   ❌ {issue}")
    print(f"{'='*70}")


if __name__ == "__main__":
    check_zone_health()