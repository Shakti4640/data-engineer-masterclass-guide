# file: 62_05_small_file_monitor.py
# Purpose: Lambda function that detects small file problems
#          and publishes CloudWatch metrics + SNS alerts
# Trigger: CloudWatch Events rule — daily at 6 AM UTC (after ETL completes)

import boto3
import json
from datetime import datetime

S3_CLIENT = boto3.client("s3")
CW_CLIENT = boto3.client("cloudwatch")
SNS_CLIENT = boto3.client("sns")
GLUE_CLIENT = boto3.client("glue")

# --- CONFIGURATION ---
MONITORED_PATHS = [
    {
        "bucket": "quickcart-analytics-datalake",
        "prefix": "silver/orders/",
        "table_name": "silver_orders",
        "alert_threshold_files": 500,
        "alert_threshold_avg_mb": 10
    },
    {
        "bucket": "quickcart-analytics-datalake",
        "prefix": "silver/customers/",
        "table_name": "silver_customers",
        "alert_threshold_files": 200,
        "alert_threshold_avg_mb": 10
    },
    {
        "bucket": "quickcart-analytics-datalake",
        "prefix": "silver/products/",
        "table_name": "silver_products",
        "alert_threshold_files": 200,
        "alert_threshold_avg_mb": 10
    }
]

SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:222222222222:data-platform-alerts"
COMPACTION_JOB_NAME = "s3-small-file-compaction"


def analyze_s3_prefix(bucket, prefix):
    """
    Count files and calculate average size for an S3 prefix
    """
    paginator = S3_CLIENT.get_paginator("list_objects_v2")

    total_size = 0
    file_count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Only count data files, not _SUCCESS markers or metadata
            if key.endswith(".parquet") or key.endswith(".snappy.parquet"):
                total_size += obj["Size"]
                file_count += 1

    total_mb = total_size / (1024 * 1024) if total_size > 0 else 0
    avg_mb = total_mb / file_count if file_count > 0 else 0

    return {
        "file_count": file_count,
        "total_mb": round(total_mb, 2),
        "avg_file_mb": round(avg_mb, 4)
    }


def publish_metrics(table_name, metrics):
    """
    Publish custom CloudWatch metrics for dashboarding and alarming
    
    METRICS:
    → DataLake/FileCount — number of files per table
    → DataLake/AvgFileSizeMB — average file size per table
    → DataLake/TotalSizeMB — total data size per table
    
    DASHBOARD: create CloudWatch dashboard showing these over time
    ALARM: trigger when AvgFileSizeMB < 10 for 2 consecutive datapoints
    """
    timestamp = datetime.utcnow()

    CW_CLIENT.put_metric_data(
        Namespace="QuickCart/DataLake",
        MetricData=[
            {
                "MetricName": "FileCount",
                "Dimensions": [
                    {"Name": "TableName", "Value": table_name}
                ],
                "Timestamp": timestamp,
                "Value": metrics["file_count"],
                "Unit": "Count"
            },
            {
                "MetricName": "AvgFileSizeMB",
                "Dimensions": [
                    {"Name": "TableName", "Value": table_name}
                ],
                "Timestamp": timestamp,
                "Value": metrics["avg_file_mb"],
                "Unit": "Megabytes"
            },
            {
                "MetricName": "TotalSizeMB",
                "Dimensions": [
                    {"Name": "TableName", "Value": table_name}
                ],
                "Timestamp": timestamp,
                "Value": metrics["total_mb"],
                "Unit": "Megabytes"
            }
        ]
    )


def trigger_auto_compaction(path_config, metrics):
    """
    Auto-trigger compaction if thresholds exceeded
    
    DECISION LOGIC:
    → file_count > threshold AND avg_size < threshold → compact
    → Both conditions must be true (high count of small files)
    → Prevents compacting 500 files that are 200 MB each (not a problem)
    """
    source_path = f"s3://{path_config['bucket']}/{path_config['prefix']}"
    target_path = source_path.rstrip("/") + "_compacting/"

    try:
        response = GLUE_CLIENT.start_job_run(
            JobName=COMPACTION_JOB_NAME,
            Arguments={
                "--source_path": source_path,
                "--target_path": target_path,
                "--target_file_size_mb": "128",
                "--file_format": "parquet",
                "--partition_keys": "none",
                "--min_files_threshold": str(path_config["alert_threshold_files"])
            }
        )
        return response["JobRunId"]
    except Exception as e:
        print(f"❌ Failed to trigger compaction: {str(e)}")
        return None


def send_alert(alerts):
    """
    Send SNS alert with details of all tables with small file problems
    """
    if not alerts:
        return

    subject = f"⚠️ Small File Alert — {len(alerts)} table(s) need compaction"

    message_lines = [
        "SMALL FILE PROBLEM DETECTED",
        f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "Affected tables:",
        "-" * 50
    ]

    for alert in alerts:
        message_lines.extend([
            f"Table: {alert['table_name']}",
            f"  Files: {alert['metrics']['file_count']:,}",
            f"  Avg size: {alert['metrics']['avg_file_mb']:.4f} MB",
            f"  Total: {alert['metrics']['total_mb']:.2f} MB",
            f"  Auto-compaction: {'Triggered (' + alert['compaction_run_id'] + ')' if alert.get('compaction_run_id') else 'Not triggered'}",
            ""
        ])

    message_lines.extend([
        "-" * 50,
        "ACTION REQUIRED:",
        "1. Check Glue compaction job runs in console",
        "2. Verify ETL jobs have coalesce() before write",
        "3. Review spark.sql.shuffle.partitions setting"
    ])

    message = "\n".join(message_lines)

    SNS_CLIENT.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )
    print(f"📧 Alert sent to {SNS_TOPIC_ARN}")


def lambda_handler(event, context):
    """
    Lambda entry point — called by CloudWatch Events rule
    
    FLOW:
    1. Scan each monitored S3 path
    2. Calculate file count + average size
    3. Publish CloudWatch metrics (always)
    4. If thresholds exceeded: trigger compaction + send alert
    """
    print("=" * 60)
    print("🔍 SMALL FILE MONITORING RUN")
    print(f"   Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)

    alerts = []

    for path_config in MONITORED_PATHS:
        table_name = path_config["table_name"]
        bucket = path_config["bucket"]
        prefix = path_config["prefix"]

        print(f"\n📋 Analyzing: {table_name}")
        print(f"   Path: s3://{bucket}/{prefix}")

        # Analyze
        metrics = analyze_s3_prefix(bucket, prefix)
        print(f"   Files: {metrics['file_count']:,}")
        print(f"   Avg size: {metrics['avg_file_mb']:.4f} MB")
        print(f"   Total: {metrics['total_mb']:.2f} MB")

        # Publish metrics (always — for dashboard)
        publish_metrics(table_name, metrics)
        print(f"   ✅ CloudWatch metrics published")

        # Check thresholds
        needs_compaction = (
            metrics["file_count"] > path_config["alert_threshold_files"] and
            metrics["avg_file_mb"] < path_config["alert_threshold_avg_mb"]
        )

        if needs_compaction:
            print(f"   ⚠️ THRESHOLD EXCEEDED — triggering compaction")

            compaction_run_id = trigger_auto_compaction(path_config, metrics)

            alerts.append({
                "table_name": table_name,
                "metrics": metrics,
                "compaction_run_id": compaction_run_id
            })
        else:
            print(f"   ✅ Within acceptable thresholds")

    # Send consolidated alert if any tables need attention
    if alerts:
        send_alert(alerts)

    # Summary
    print(f"\n{'='*60}")
    print(f"📊 MONITORING SUMMARY")
    print(f"   Tables scanned: {len(MONITORED_PATHS)}")
    print(f"   Alerts raised: {len(alerts)}")
    print(f"   Compactions triggered: {sum(1 for a in alerts if a.get('compaction_run_id'))}")
    print(f"{'='*60}")

    return {
        "statusCode": 200,
        "tables_scanned": len(MONITORED_PATHS),
        "alerts": len(alerts),
        "details": alerts
    }