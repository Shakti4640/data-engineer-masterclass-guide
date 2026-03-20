# file: 66_06_watchdog_lambda.py
# Purpose: Lambda that runs at 5 AM to check if all sources completed
# Triggered by: EventBridge scheduled rule

import boto3
import json
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
state_table = dynamodb.Table("pipeline_batch_state")

REQUIRED_SOURCES = [
    "partner_feed",
    "payment_data",
    "cdc_mariadb",
    "inventory_api"
]

SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:222222222222:data-platform-alerts"


def lambda_handler(event, context):
    """
    Check if all required sources completed for today's batch
    If any missing → alert the team
    """
    batch_date = datetime.utcnow().strftime("%Y-%m-%d")

    print(f"🔍 Watchdog check for batch: {batch_date}")

    # Query state table
    response = state_table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key("batch_date").eq(batch_date)
    )

    items = {item["source_name"]: item for item in response.get("Items", [])}

    # Analyze status
    missing = []
    failed = []
    completed = []
    processing = []

    for source in REQUIRED_SOURCES:
        if source not in items:
            missing.append(source)
        elif items[source]["status"] == "completed":
            completed.append(source)
        elif items[source]["status"] == "failed":
            failed.append({
                "source": source,
                "error": items[source].get("error_message", "Unknown")
            })
        elif items[source]["status"] == "processing":
            started = items[source].get("started_at", "unknown")
            processing.append({"source": source, "started_at": started})

    print(f"   Completed: {completed}")
    print(f"   Missing: {missing}")
    print(f"   Failed: {[f['source'] for f in failed]}")
    print(f"   Still processing: {[p['source'] for p in processing]}")

    # Alert if any issues
    issues = missing + [f["source"] for f in failed] + [p["source"] for p in processing]

    if issues:
        subject = f"⚠️ Pipeline Watchdog: {len(issues)} source(s) incomplete at 5 AM"

        message_lines = [
            f"PIPELINE WATCHDOG ALERT — Batch: {batch_date}",
            f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "",
            f"Completed ({len(completed)}/{len(REQUIRED_SOURCES)}):",
            *[f"  ✅ {s}" for s in completed],
            ""
        ]

        if missing:
            message_lines.extend([
                f"MISSING — never arrived ({len(missing)}):",
                *[f"  ❌ {s} — file never landed in S3" for s in missing],
                ""
            ])

        if failed:
            message_lines.extend([
                f"FAILED — ETL error ({len(failed)}):",
                *[f"  ❌ {f['source']}: {f['error'][:100]}" for f in failed],
                ""
            ])

        if processing:
            message_lines.extend([
                f"STILL RUNNING ({len(processing)}):",
                *[f"  ⏳ {p['source']} — started at {p['started_at']}" for p in processing],
                ""
            ])

        message_lines.extend([
            "ACTION REQUIRED:",
            "1. Missing sources: check source system delivery",
            "2. Failed sources: check Glue job logs in CloudWatch",
            "3. Running sources: check if Glue job is stuck",
            "4. Manual trigger: re-run via Step Functions console"
        ])

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message="\n".join(message_lines)
        )
        print(f"📧 Alert sent — {len(issues)} issue(s)")

    else:
        print(f"✅ All {len(REQUIRED_SOURCES)} required sources completed — no action needed")

    return {
        "batch_date": batch_date,
        "completed": len(completed),
        "missing": len(missing),
        "failed": len(failed),
        "processing": len(processing),
        "alert_sent": len(issues) > 0
    }
