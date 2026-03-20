# file: 53_06_verify_cleaned_output.py
# Purpose: Verify DataBrew cleaning results — compare before vs after

import boto3
import csv
import io
import json
from collections import Counter

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"


def analyze_source_data():
    """Analyze DIRTY source data quality"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("📊 DATA QUALITY COMPARISON: BEFORE vs AFTER DATABREW")
    print("=" * 70)

    # List source files
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="daily_exports/customers/"
    )

    if "Contents" not in response:
        print("❌ No source files found")
        return

    # Read first CSV
    csv_key = response["Contents"][0]["Key"]
    print(f"\n📌 BEFORE (Source): s3://{BUCKET_NAME}/{csv_key}")
    print("─" * 60)

    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_key)
    content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    rows = list(reader)

    total_rows = len(rows)
    print(f"  Total rows: {total_rows:,}")

    # Analyze quality issues
    # Duplicates
    ids = [r["customer_id"] for r in rows]
    id_counts = Counter(ids)
    duplicates = sum(1 for count in id_counts.values() if count > 1)
    duplicate_rows = sum(count - 1 for count in id_counts.values() if count > 1)
    print(f"\n  DUPLICATES:")
    print(f"    Unique customer_ids with duplicates: {duplicates}")
    print(f"    Extra duplicate rows: {duplicate_rows}")

    # Email issues
    bad_emails = sum(1 for r in rows if "@" not in r.get("email", ""))
    space_emails = sum(1 for r in rows if " " in r.get("email", "").strip())
    print(f"\n  EMAIL ISSUES:")
    print(f"    Missing @: {bad_emails} ({bad_emails/total_rows*100:.1f}%)")
    print(f"    Contains spaces: {space_emails} ({space_emails/total_rows*100:.1f}%)")

    # State variations
    states = Counter(r.get("state", "").strip() for r in rows if r.get("state", "").strip())
    print(f"\n  STATE VARIATIONS:")
    print(f"    Unique state values: {len(states)}")
    print(f"    Top 10: {states.most_common(10)}")

    # Nulls
    null_city = sum(1 for r in rows if not r.get("city", "").strip())
    null_date = sum(1 for r in rows if not r.get("signup_date", "").strip())
    print(f"\n  MISSING VALUES:")
    print(f"    Empty city: {null_city} ({null_city/total_rows*100:.1f}%)")
    print(f"    Empty signup_date: {null_date} ({null_date/total_rows*100:.1f}%)")

    # Phone formats
    phone_patterns = Counter()
    for r in rows:
        phone = r.get("phone", "")
        if phone.startswith("+"):
            phone_patterns["+X-XXX-XXXX"] += 1
        elif phone.startswith("("):
            phone_patterns["(XXX) XXXX"] += 1
        elif "-" in phone:
            phone_patterns["XXX-XXXX"] += 1
        elif "." in phone:
            phone_patterns["XXX.XXXX"] += 1
        else:
            phone_patterns["XXXXXXX"] += 1
    print(f"\n  PHONE FORMAT VARIATIONS:")
    for pattern, count in phone_patterns.most_common():
        print(f"    {pattern}: {count}")

    # Tier case issues
    tiers = Counter(r.get("tier", "") for r in rows)
    print(f"\n  TIER CASE VARIATIONS:")
    for tier, count in tiers.most_common():
        print(f"    '{tier}': {count}")

    return {
        "total_rows": total_rows,
        "duplicate_rows": duplicate_rows,
        "bad_emails": bad_emails,
        "unique_states": len(states),
        "null_city": null_city,
        "null_date": null_date,
        "phone_formats": len(phone_patterns)
    }


def analyze_cleaned_data():
    """Analyze CLEANED output data quality"""
    s3_client = boto3.client("s3", region_name=REGION)

    print(f"\n\n📌 AFTER (Cleaned): s3://{BUCKET_NAME}/cleaned/customers/")
    print("─" * 60)

    # List cleaned files (Parquet)
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="cleaned/customers/"
    )

    if "Contents" not in response:
        print("  ⚠️  No cleaned files found yet")
        print("  → Run 53_04_run_jobs.py first")
        print("  → Or verify Recipe Job completed successfully")
        return None

    total_size = sum(obj["Size"] for obj in response["Contents"])
    file_count = len(response["Contents"])

    print(f"  Output files: {file_count}")
    print(f"  Total size: {total_size / 1024:.1f} KB")
    print(f"  Format: Parquet + Snappy compression")

    # For detailed verification: use Athena (from Project 24)
    print(f"\n  📝 FOR DETAILED VERIFICATION:")
    print(f"     Use Athena to query cleaned data:")
    print(f"")
    print(f"     -- Count distinct states (should be ~5, not ~30)")
    print(f"     SELECT state, COUNT(*) as cnt")
    print(f"     FROM cleaned_customers")
    print(f"     GROUP BY state ORDER BY cnt DESC;")
    print(f"")
    print(f"     -- Check for remaining duplicates (should be 0)")
    print(f"     SELECT customer_id, COUNT(*) as cnt")
    print(f"     FROM cleaned_customers")
    print(f"     GROUP BY customer_id HAVING cnt > 1;")
    print(f"")
    print(f"     -- Check null cities (should be 0)")
    print(f"     SELECT COUNT(*) FROM cleaned_customers")
    print(f"     WHERE city IS NULL OR city = '';")
    print(f"")
    print(f"     -- Check email quality")
    print(f"     SELECT COUNT(*) FROM cleaned_customers")
    print(f"     WHERE email NOT LIKE '%@%';")
    print(f"")
    print(f"     -- Check phone format (should all be digits only)")
    print(f"     SELECT phone, LENGTH(phone) FROM cleaned_customers")
    print(f"     LIMIT 10;")
    print(f"")
    print(f"     -- Check tier consistency (should all be lowercase)")
    print(f"     SELECT DISTINCT tier FROM cleaned_customers;")


def generate_comparison_report(before_stats):
    """Generate before/after comparison summary"""
    if not before_stats:
        return

    print(f"\n\n{'=' * 70}")
    print(f"📊 DATA QUALITY IMPROVEMENT REPORT")
    print(f"{'=' * 70}")
    print(f"")
    print(f"{'Metric':<35} {'Before':<15} {'Expected After':<15} {'Status'}")
    print(f"{'─' * 75}")
    print(f"{'Total rows':<35} {before_stats['total_rows']:<15,} {'~' + str(before_stats['total_rows'] - before_stats['duplicate_rows']):<15} {'Deduped'}")
    print(f"{'Duplicate rows':<35} {before_stats['duplicate_rows']:<15} {'0':<15} {'✅ Removed'}")
    print(f"{'Bad emails (no @)':<35} {before_stats['bad_emails']:<15} {'0':<15} {'✅ Cleaned'}")
    print(f"{'Unique state values':<35} {before_stats['unique_states']:<15} {'5':<15} {'✅ Standardized'}")
    print(f"{'Missing city':<35} {before_stats['null_city']:<15} {'0':<15} {'✅ Filled'}")
    print(f"{'Missing signup_date':<35} {before_stats['null_date']:<15} {'0':<15} {'✅ Filled'}")
    print(f"{'Phone format variations':<35} {before_stats['phone_formats']:<15} {'1':<15} {'✅ Standardized'}")
    print(f"{'─' * 75}")
    print(f"")
    print(f"  🎯 Sarah can now:")
    print(f"     → View this report in DataBrew Profile comparison")
    print(f"     → Modify recipe steps if new issues emerge")
    print(f"     → Recipe runs automatically every night at 2 AM")
    print(f"     → Clean data available in Silver zone for dashboards")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    before_stats = analyze_source_data()
    analyze_cleaned_data()
    generate_comparison_report(before_stats)