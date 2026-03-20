# file: 04_monitor_exports.py
# Checks if nightly export ran successfully
# Alerts if export is missing or stale

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
S3_PREFIX = "mariadb_exports"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"


def check_export_status(target_date=None):
    """Check if export for target_date exists and is complete"""
    if target_date is None:
        target_date = datetime.now() - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")
    manifest_key = f"{S3_PREFIX}/_manifests/export_{date_str}.json"

    s3_client = boto3.client("s3", region_name=REGION)

    print(f"🔍 Checking export for: {date_str}")
    print(f"   Manifest: s3://{BUCKET_NAME}/{manifest_key}")

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=manifest_key)
        manifest = json.loads(response["Body"].read().decode("utf-8"))

        summary = manifest["summary"]
        print(f"\n   📊 Export Status:")
        print(f"      Succeeded: {summary['succeeded']}/{summary['total_tables']}")
        print(f"      Failed:    {summary['failed']}")
        print(f"      Rows:      {summary['total_rows']:,}")
        print(f"      Size:      {round(summary['total_size_bytes']/(1024*1024), 2)} MB")
        print(f"      Duration:  {manifest['total_duration_seconds']}s")
        print(f"      Timestamp: {manifest['export_timestamp']}")

        # Check for failures
        if summary["failed"] > 0:
            print(f"\n   ⚠️  PARTIAL FAILURE:")
            for table in manifest["tables"]:
                if table["status"] == "FAILED":
                    print(f"      ❌ {table['table']}: {table.get('error', 'unknown')}")
            return "PARTIAL"

        print(f"\n   ✅ Export complete and healthy")
        return "SUCCESS"

    except s3_client.exceptions.NoSuchKey:
        print(f"\n   ❌ MANIFEST NOT FOUND — export may not have run!")
        return "MISSING"
    except Exception as e:
        print(f"\n   ❌ Error checking export: {e}")
        return "ERROR"


def check_last_7_days():
    """Check export status for the last 7 days"""
    print("=" * 60)
    print("📊 EXPORT STATUS — LAST 7 DAYS")
    print("=" * 60)

    alerts = []

    for days_ago in range(1, 8):
        target_date = datetime.now() - timedelta(days=days_ago)
        date_str = target_date.strftime("%Y-%m-%d")
        day_name = target_date.strftime("%A")

        status = check_export_status(target_date)

        if status == "MISSING":
            alerts.append(f"MISSING: {date_str} ({day_name})")
        elif status == "PARTIAL":
            alerts.append(f"PARTIAL: {date_str} ({day_name})")
        elif status == "ERROR":
            alerts.append(f"ERROR: {date_str} ({day_name})")

        print()

    # Send alerts
    if alerts:
        print(f"{'=' * 60}")
        print(f"🚨 ALERTS ({len(alerts)}):")
        for alert in alerts:
            print(f"   ⚠️  {alert}")

        try:
            sns_client = boto3.client("sns", region_name=REGION)
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"⚠️ MariaDB Export Alert — {len(alerts)} issue(s)",
                Message="\n".join([
                    "MARIADB EXPORT MONITORING ALERT",
                    "",
                    "Issues found:",
                    *[f"• {a}" for a in alerts],
                    "",
                    f"Checked: last 7 days",
                    f"Time: {datetime.now().isoformat()}"
                ])
            )
            print(f"\n   📧 Alert sent via SNS")
        except Exception as e:
            print(f"\n   ⚠️  Could not send alert: {e}")
    else:
        print(f"{'=' * 60}")
        print(f"✅ All exports healthy for the last 7 days")


if __name__ == "__main__":
    check_last_7_days()