# file: 50_04_dlq_monitoring.py
# Purpose: Comprehensive DLQ monitoring — run daily or as CloudWatch scheduled event

import boto3
import json
from datetime import datetime, timezone, timedelta
from collections import defaultdict

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"

DLQ_NAMES = [
    "quickcart-file-process-q-dlq",
    "quickcart-notification-q-dlq",
    "quickcart-etl-trigger-q-dlq"
]


def generate_dlq_report():
    """Generate comprehensive DLQ health report"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    print("╔" + "═" * 68 + "╗")
    print("║" + "  📊 DLQ MONITORING REPORT".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    report_data = {}
    total_failed = 0
    issues = []

    for dlq_name in DLQ_NAMES:
        try:
            dlq_url = sqs_client.get_queue_url(QueueName=dlq_name)["QueueUrl"]
        except Exception:
            print(f"\n   ⚠️  Queue not found: {dlq_name}")
            continue

        # Get current queue stats
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "MessageRetentionPeriod",
                "CreatedTimestamp"
            ]
        )["Attributes"]

        visible = int(attrs.get("ApproximateNumberOfMessages", 0))
        in_flight = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
        retention_days = int(attrs.get("MessageRetentionPeriod", 0)) // 86400
        total_in_queue = visible + in_flight
        total_failed += total_in_queue

        # Get CloudWatch metrics: messages received in last 24 hours
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=24)

        new_messages_24h = get_metric_sum(
            cw_client, dlq_name, "NumberOfMessagesSent",
            start_time, end_time
        )

        # Get oldest message age
        oldest_age_seconds = get_metric_max(
            cw_client, dlq_name, "ApproximateAgeOfOldestMessage",
            start_time, end_time
        )
        oldest_age_hours = oldest_age_seconds / 3600 if oldest_age_seconds else 0

        # Get 7-day trend
        trend_start = end_time - timedelta(days=7)
        new_messages_7d = get_metric_sum(
            cw_client, dlq_name, "NumberOfMessagesSent",
            trend_start, end_time
        )

        # Status determination
        if total_in_queue == 0:
            status = "✅ HEALTHY"
        elif oldest_age_hours > 168:  # > 7 days
            status = "🚨 CRITICAL"
            issues.append(f"{dlq_name}: {total_in_queue} messages, oldest {oldest_age_hours:.0f}h")
        elif oldest_age_hours > 24:
            status = "❌ STALE"
            issues.append(f"{dlq_name}: {total_in_queue} messages, oldest {oldest_age_hours:.0f}h")
        else:
            status = "⚠️  ACTIVE"
            issues.append(f"{dlq_name}: {total_in_queue} new failed messages")

        # Display
        print(f"\n{'─'*60}")
        print(f"{status} {dlq_name}")
        print(f"{'─'*60}")
        print(f"   Current messages: {total_in_queue} (visible: {visible}, in-flight: {in_flight})")
        print(f"   New failures (24h): {int(new_messages_24h)}")
        print(f"   New failures (7d):  {int(new_messages_7d)}")
        print(f"   Oldest message:     {oldest_age_hours:.1f} hours")
        print(f"   Retention:          {retention_days} days")

        # Daily trend (last 7 days)
        if new_messages_7d > 0:
            print(f"\n   📈 Daily failure trend:")
            for day_offset in range(6, -1, -1):
                day_start = end_time - timedelta(days=day_offset + 1)
                day_end = end_time - timedelta(days=day_offset)
                day_count = get_metric_sum(
                    cw_client, dlq_name, "NumberOfMessagesSent",
                    day_start, day_end
                )
                day_str = day_start.strftime("%m-%d")
                bar = "█" * int(min(day_count, 50))
                if day_count > 0:
                    print(f"   {day_str} | {int(day_count):>4} | {bar}")
                else:
                    print(f"   {day_str} |    0 | (none)")

        report_data[dlq_name] = {
            "current_messages": total_in_queue,
            "new_24h": int(new_messages_24h),
            "new_7d": int(new_messages_7d),
            "oldest_hours": oldest_age_hours,
            "status": status
        }

    # ━━━ OVERALL SUMMARY ━━━
    print(f"\n{'='*60}")
    print(f"📊 OVERALL DLQ SUMMARY")
    print(f"{'='*60}")
    print(f"   Total failed messages across all DLQs: {total_failed}")
    print(f"   DLQs with issues: {len(issues)}")

    if issues:
        print(f"\n   ⚠️  ISSUES REQUIRING ATTENTION:")
        for issue in issues:
            print(f"      → {issue}")

        # Send alert if there are issues
        send_monitoring_alert(report_data, issues)
    else:
        print(f"\n   ✅ ALL DLQs HEALTHY — no failed messages")

    print(f"{'='*60}")

    return report_data


def get_metric_sum(cw_client, queue_name, metric_name, start_time, end_time):
    """Get sum of a CloudWatch metric for a queue"""
    try:
        response = cw_client.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName=metric_name,
            Dimensions=[{"Name": "QueueName", "Value": queue_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=86400,
            Statistics=["Sum"]
        )
        datapoints = response.get("Datapoints", [])
        return sum(dp.get("Sum", 0) for dp in datapoints)
    except Exception:
        return 0


def get_metric_max(cw_client, queue_name, metric_name, start_time, end_time):
    """Get max of a CloudWatch metric for a queue"""
    try:
        response = cw_client.get_metric_statistics(
            Namespace="AWS/SQS",
            MetricName=metric_name,
            Dimensions=[{"Name": "QueueName", "Value": queue_name}],
            StartTime=start_time,
            EndTime=end_time,
            Period=3600,
            Statistics=["Maximum"]
        )
        datapoints = response.get("Datapoints", [])
        if datapoints:
            return max(dp.get("Maximum", 0) for dp in datapoints)
        return 0
    except Exception:
        return 0


def send_monitoring_alert(report_data, issues):
    """Send DLQ monitoring alert via SNS"""
    sns_client = boto3.client("sns", region_name=REGION)

    total_messages = sum(d["current_messages"] for d in report_data.values())

    subject = f"⚠️ DLQ Alert: {total_messages} failed messages across {len(issues)} queues"

    message_lines = [
        "DLQ Monitoring Alert — QuickCart",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        f"Total failed messages: {total_messages}",
        "",
        "Details:",
        "─" * 50
    ]

    for dlq_name, data in report_data.items():
        if data["current_messages"] > 0:
            message_lines.append(f"  {dlq_name}:")
            message_lines.append(f"    Messages: {data['current_messages']}")
            message_lines.append(f"    New (24h): {data['new_24h']}")
            message_lines.append(f"    Oldest: {data['oldest_hours']:.1f} hours")
            message_lines.append(f"    Status: {data['status']}")
            message_lines.append("")

    message_lines.extend([
        "Recommended actions:",
        "  1. Inspect DLQ messages: python 50_02_dlq_inspector.py",
        "  2. Identify root cause (check consumer logs)",
        "  3. Fix root cause",
        "  4. Redrive messages: python 50_03_dlq_redrive.py redrive <dlq-name>",
        "",
        "If messages are unrecoverable:",
        "  5. Purge DLQ: python 50_03_dlq_redrive.py purge <dlq-name>"
    ])

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message="\n".join(message_lines)
        )
        print(f"\n   📧 Monitoring alert sent")
    except Exception as e:
        print(f"\n   ⚠️  Alert failed: {e}")


def check_source_queue_health():
    """
    Also check source queues — compare processing rate vs failure rate.
    High failure rate = systemic problem, not just occasional errors.
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    source_queues = [
        "quickcart-file-process-q",
        "quickcart-notification-q",
        "quickcart-etl-trigger-q"
    ]

    print(f"\n{'='*60}")
    print(f"📊 SOURCE QUEUE HEALTH")
    print(f"{'='*60}")

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    for queue_name in source_queues:
        dlq_name = f"{queue_name}-dlq"

        # Messages processed (deleted = successfully processed)
        processed = get_metric_sum(
            cw_client, queue_name, "NumberOfMessagesDeleted",
            start_time, end_time
        )

        # Messages received (total attempts)
        received = get_metric_sum(
            cw_client, queue_name, "NumberOfMessagesReceived",
            start_time, end_time
        )

        # Messages sent to DLQ (failures)
        failed = get_metric_sum(
            cw_client, dlq_name, "NumberOfMessagesSent",
            start_time, end_time
        )

        # Failure rate
        total_attempted = processed + failed
        failure_rate = (failed / max(total_attempted, 1)) * 100

        status = "✅" if failure_rate < 1 else "⚠️" if failure_rate < 5 else "❌"

        print(f"\n   {status} {queue_name}")
        print(f"      Processed (24h): {int(processed):,}")
        print(f"      Failed (24h):    {int(failed):,}")
        print(f"      Failure rate:    {failure_rate:.2f}%")

        if failure_rate > 5:
            print(f"      ❌ HIGH FAILURE RATE — investigate systemic issue")
        elif failure_rate > 1:
            print(f"      ⚠️  Elevated failure rate — monitor closely")


if __name__ == "__main__":
    report = generate_dlq_report()
    check_source_queue_health()