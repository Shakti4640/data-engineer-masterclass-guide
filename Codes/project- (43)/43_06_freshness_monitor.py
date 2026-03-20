# file: 43_06_freshness_monitor.py
# Purpose: Continuous monitoring of reverse ETL data freshness
# Run as a cron job or CloudWatch scheduled Lambda

import boto3
import pymysql
from datetime import datetime, timedelta

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"

MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart"
}

# Maximum acceptable age in hours before alerting
FRESHNESS_THRESHOLDS = {
    "customer_metrics": 26,              # Alert if older than 26 hours
    "customer_recommendations": 26       # (pipeline runs nightly, allow buffer)
}


def check_freshness():
    """
    Check computed_at timestamp for each reverse ETL table.
    Alert if data is stale beyond threshold.
    
    WHY 26 hours (not 24):
    → Pipeline runs at ~01:00
    → Data is "from" yesterday's aggregation
    → If today's pipeline hasn't run yet, data is 25 hours old
    → 26 hours = 1 hour buffer before alerting
    → 48+ hours = pipeline definitely broken
    """
    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    print(f"🕐 FRESHNESS CHECK — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)

    stale_tables = []

    for table, threshold_hours in FRESHNESS_THRESHOLDS.items():
        try:
            cursor.execute(f"SELECT MAX(computed_at) FROM {table}")
            result = cursor.fetchone()

            if result[0] is None:
                print(f"   ❌ {table}: NO DATA (table empty)")
                stale_tables.append((table, "NO DATA", 999))
                continue

            latest = result[0]
            age_hours = (datetime.now() - latest).total_seconds() / 3600

            if age_hours > threshold_hours:
                status = "❌ STALE"
                stale_tables.append((table, str(latest), age_hours))
            elif age_hours > threshold_hours * 0.8:
                status = "⚠️  AGING"
            else:
                status = "✅ FRESH"

            print(f"   {status} {table}")
            print(f"      Latest: {latest}")
            print(f"      Age: {age_hours:.1f} hours (threshold: {threshold_hours}h)")

        except Exception as e:
            print(f"   ❌ {table}: ERROR — {e}")
            stale_tables.append((table, "ERROR", 999))

    cursor.close()
    conn.close()

    # Send alert if any table is stale
    if stale_tables:
        send_staleness_alert(stale_tables)

    return len(stale_tables) == 0


def send_staleness_alert(stale_tables):
    """Send SNS alert for stale reverse ETL data"""
    sns_client = boto3.client("sns", region_name=REGION)

    message_lines = [
        "🚨 REVERSE ETL DATA FRESHNESS ALERT",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "The following tables have stale data:",
        "-" * 40
    ]

    for table, latest, age in stale_tables:
        message_lines.append(f"  ❌ {table}")
        message_lines.append(f"     Latest computed_at: {latest}")
        message_lines.append(f"     Age: {age:.1f} hours")
        message_lines.append("")

    message_lines.extend([
        "Possible causes:",
        "  1. Reverse ETL pipeline failed (check Step Functions)",
        "  2. Redshift UNLOAD failed (check Redshift query history)",
        "  3. Glue job failed (check Glue job runs)",
        "  4. MariaDB connection issue",
        "",
        "Action: Check Step Functions execution history for recent failures"
    ])

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="🚨 Reverse ETL Data Stale — QuickCart",
            Message="\n".join(message_lines)
        )
        print(f"\n📧 Staleness alert sent!")
    except Exception as e:
        print(f"\n⚠️  Failed to send alert: {e}")


if __name__ == "__main__":
    is_fresh = check_freshness()
    exit(0 if is_fresh else 1)