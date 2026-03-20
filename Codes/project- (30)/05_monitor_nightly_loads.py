# file: 05_monitor_nightly_loads.py
# Monitors that all expected tables were loaded into Redshift
# Runs at 4 AM to verify 2 AM pipeline completed successfully

import boto3
import psycopg2
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"
QUEUE_NAME = "quickcart-redshift-load-queue"
DLQ_NAME = "quickcart-redshift-load-dlq"

REDSHIFT_HOST = "quickcart-warehouse.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "quickcart"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "YourSecurePassword123!"

EXPECTED_TABLES = {
    "public.orders":    {"min_rows": 1000},
    "public.customers": {"min_rows": 100},
    "public.products":  {"min_rows": 10}
}


def check_redshift_freshness():
    """Check that Redshift tables have recent data"""
    print("=" * 60)
    print(f"🔍 NIGHTLY LOAD MONITOR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    alerts = []

    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST, port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB, user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        cursor = conn.cursor()

        print(f"\n📊 REDSHIFT TABLE STATUS:")
        print(f"   {'Table':<25} {'Rows':>12} {'Min Expected':>14} {'Status':>10}")
        print(f"   {'─'*25} {'─'*12} {'─'*14} {'─'*10}")

        for table, config in EXPECTED_TABLES.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                row_count = cursor.fetchone()[0]
                min_rows = config["min_rows"]

                if row_count >= min_rows:
                    status = "✅ OK"
                elif row_count == 0:
                    status = "❌ EMPTY"
                    alerts.append(f"TABLE EMPTY: {table} has 0 rows")
                else:
                    status = "⚠️ LOW"
                    alerts.append(f"LOW ROWS: {table} has {row_count} (min: {min_rows})")

                print(f"   {table:<25} {row_count:>12,} {min_rows:>14,} {status:>10}")

            except psycopg2.Error as e:
                print(f"   {table:<25} {'ERROR':>12} {'':>14} ❌ {e}")
                alerts.append(f"QUERY ERROR: {table} — {e}")

        # Check recent COPY activity
        print(f"\n📋 RECENT COPY ACTIVITY (last 24h):")
        cursor.execute("""
            SELECT 
                TRIM(querytxt) as sql_text,
                starttime,
                DATEDIFF(seconds, starttime, endtime) as duration_sec
            FROM stl_query
            WHERE TRIM(querytxt) LIKE 'COPY%'
              AND starttime > GETDATE() - INTERVAL '24 hours'
            ORDER BY starttime DESC
            LIMIT 10;
        """)

        copies = cursor.fetchall()
        if copies:
            for copy in copies:
                sql_preview = copy[0][:60]
                time_str = copy[1].strftime("%H:%M:%S")
                duration = copy[2]
                print(f"   {time_str} ({duration}s): {sql_preview}...")
        else:
            print(f"   ⚠️  No COPY activity in last 24 hours!")
            alerts.append("NO COPY ACTIVITY: No COPY commands executed in 24h")

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"\n   ❌ Cannot connect to Redshift: {e}")
        alerts.append(f"REDSHIFT UNREACHABLE: {e}")

    # Check SQS DLQ
    print(f"\n📬 QUEUE STATUS:")
    try:
        sqs_client = boto3.client("sqs", region_name=REGION)

        # Main queue
        queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        main_attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]
        )["Attributes"]
        main_visible = int(main_attrs.get("ApproximateNumberOfMessages", 0))
        main_inflight = int(main_attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        print(f"   Main queue:  {main_visible} waiting, {main_inflight} in-flight")

        if main_visible > 10:
            alerts.append(f"QUEUE BACKLOG: {main_visible} messages waiting")

        # DLQ
        dlq_url = sqs_client.get_queue_url(QueueName=DLQ_NAME)["QueueUrl"]
        dlq_attrs = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]
        dlq_visible = int(dlq_attrs.get("ApproximateNumberOfMessages", 0))
        print(f"   DLQ:         {dlq_visible} failed messages")

        if dlq_visible > 0:
            alerts.append(f"DLQ HAS {dlq_visible} FAILED MESSAGE(S) — investigate!")

    except Exception as e:
        print(f"   ⚠️  SQS check failed: {e}")

    # Check S3 export manifest (from Project 29)
    print(f"\n📁 S3 EXPORT STATUS:")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    manifest_key = f"mariadb_exports/_manifests/export_{yesterday}.json"

    try:
        s3_client = boto3.client("s3", region_name=REGION)
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=manifest_key)
        manifest = json.loads(response["Body"].read().decode("utf-8"))
        summary = manifest["summary"]
        print(f"   Export manifest: ✅ found for {yesterday}")
        print(f"   Tables exported: {summary['succeeded']}/{summary['total_tables']}")
        print(f"   Total rows:      {summary['total_rows']:,}")
    except Exception:
        print(f"   Export manifest: ❌ NOT FOUND for {yesterday}")
        alerts.append(f"EXPORT MISSING: No manifest for {yesterday}")

    # Send alerts
    print(f"\n{'=' * 60}")
    if alerts:
        print(f"🚨 ALERTS ({len(alerts)}):")
        for alert in alerts:
            print(f"   ⚠️  {alert}")

        try:
            sns_client = boto3.client("sns", region_name=REGION)
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"⚠️ Redshift Nightly Load Alert — {len(alerts)} issue(s)",
                Message="\n".join([
                    "REDSHIFT NIGHTLY LOAD MONITOR",
                    f"Time: {datetime.now().isoformat()}",
                    "",
                    "Issues found:",
                    *[f"• {a}" for a in alerts],
                    "",
                    "Action required: check worker logs and DLQ"
                ])
            )
            print(f"\n   📧 Alert sent via SNS")
        except Exception as e:
            print(f"\n   ⚠️  Could not send alert: {e}")
    else:
        print(f"✅ ALL CHECKS PASSED — Nightly load is healthy")
    print("=" * 60)

    return len(alerts) == 0


if __name__ == "__main__":
    healthy = check_redshift_freshness()
    exit(0 if healthy else 1)