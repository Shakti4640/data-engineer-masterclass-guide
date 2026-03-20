# file: 04_cost_monitoring.py
# Track Athena costs per workgroup and alert on overspend
# Integrates with CloudWatch metrics and SNS (Project 12)

import boto3
from datetime import datetime, timedelta

REGION = "us-east-2"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:123456789012:data-pipeline-alerts"

WORKGROUPS = ["finance_team", "marketing_team", "product_team", "data_engineering"]
COST_PER_TB = 5.00  # Athena pricing


def get_workgroup_usage(workgroup, hours=24):
    """
    Calculate data scanned and cost for a workgroup
    
    Uses Athena API to list recent queries and sum up bytes scanned
    """
    athena_client = boto3.client("athena", region_name=REGION)
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)

    # List recent query executions
    query_ids = []
    next_token = None

    while True:
        kwargs = {"WorkGroup": workgroup, "MaxResults": 50}
        if next_token:
            kwargs["NextToken"] = next_token

        response = athena_client.list_query_executions(**kwargs)
        query_ids.extend(response.get("QueryExecutionIds", []))
        next_token = response.get("NextToken")

        if not next_token or len(query_ids) >= 200:
            break

    if not query_ids:
        return {"queries": 0, "bytes_scanned": 0, "cost_usd": 0}

    # Batch get details (max 50 per call)
    total_bytes = 0
    total_queries = 0
    succeeded = 0
    failed = 0

    for i in range(0, len(query_ids), 50):
        batch = query_ids[i:i + 50]
        batch_response = athena_client.batch_get_query_execution(
            QueryExecutionIds=batch
        )

        for execution in batch_response["QueryExecutions"]:
            submit_time = execution["Status"].get("SubmissionDateTime")
            if submit_time and submit_time.replace(tzinfo=None) < cutoff_time:
                continue

            state = execution["Status"]["State"]
            total_queries += 1

            if state == "SUCCEEDED":
                succeeded += 1
                bytes_scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
                total_bytes += bytes_scanned
            elif state == "FAILED":
                failed += 1

    cost = (total_bytes / (1024 ** 4)) * COST_PER_TB

    return {
        "queries": total_queries,
        "succeeded": succeeded,
        "failed": failed,
        "bytes_scanned": total_bytes,
        "gb_scanned": round(total_bytes / (1024 ** 3), 3),
        "cost_usd": round(cost, 4)
    }


def generate_cost_report(hours=24):
    """Generate cost report across all workgroups"""
    print("=" * 70)
    print(f"💰 ATHENA COST REPORT — Last {hours} hours")
    print(f"   Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    grand_total_bytes = 0
    grand_total_cost = 0
    grand_total_queries = 0

    print(f"\n   {'Workgroup':<25} {'Queries':>8} {'GB Scanned':>12} {'Cost':>10}")
    print(f"   {'─' * 25} {'─' * 8} {'─' * 12} {'─' * 10}")

    alerts = []

    for wg in WORKGROUPS:
        usage = get_workgroup_usage(wg, hours)

        print(f"   {wg:<25} {usage['queries']:>8} {usage['gb_scanned']:>11.3f} ${usage['cost_usd']:>9.4f}")

        grand_total_bytes += usage["bytes_scanned"]
        grand_total_cost += usage["cost_usd"]
        grand_total_queries += usage["queries"]

        # Alert thresholds
        if usage["gb_scanned"] > 50:
            alerts.append(f"HIGH USAGE: {wg} scanned {usage['gb_scanned']} GB in {hours}h")
        if usage["failed"] > 10:
            alerts.append(f"HIGH FAILURES: {wg} had {usage['failed']} failed queries in {hours}h")

    grand_total_gb = round(grand_total_bytes / (1024 ** 3), 3)
    print(f"   {'─' * 25} {'─' * 8} {'─' * 12} {'─' * 10}")
    print(f"   {'TOTAL':<25} {grand_total_queries:>8} {grand_total_gb:>11.3f} ${grand_total_cost:>9.4f}")

    # Monthly projection
    daily_cost = grand_total_cost * (24 / hours) if hours < 24 else grand_total_cost
    monthly_projection = round(daily_cost * 30, 2)
    print(f"\n   📈 Monthly projection: ${monthly_projection:.2f}")

    # Comparison with Redshift
    redshift_monthly = 180.0
    savings = round(redshift_monthly - monthly_projection, 2)
    savings_pct = round((savings / redshift_monthly) * 100, 1)
    print(f"   📊 vs Redshift ($180/mo): saving ${savings:.2f}/mo ({savings_pct}%)")

    # --- SEND ALERTS IF ANY ---
    if alerts:
        print(f"\n🚨 ALERTS:")
        for alert in alerts:
            print(f"   ⚠️  {alert}")

        try:
            sns_client = boto3.client("sns", region_name=REGION)
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"⚠️ Athena Cost Alert — {len(alerts)} issue(s)",
                Message="\n".join([
                    f"ATHENA COST ALERT",
                    f"Period: last {hours} hours",
                    f"Total scanned: {grand_total_gb} GB",
                    f"Total cost: ${grand_total_cost:.4f}",
                    f"Monthly projection: ${monthly_projection:.2f}",
                    "",
                    "ALERTS:",
                    *[f"• {a}" for a in alerts],
                    "",
                    f"Timestamp: {datetime.now().isoformat()}"
                ])
            )
            print(f"   📧 Alert sent to SNS topic")
        except Exception as e:
            print(f"   ⚠️  Failed to send alert: {e}")

    print("=" * 70)
    return grand_total_cost, monthly_projection


def setup_cloudwatch_alarm():
    """
    Create CloudWatch alarm for Athena data scanned
    
    Triggers when daily data scanned exceeds threshold
    Sends SNS notification to data engineering team
    """
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    for wg in WORKGROUPS:
        alarm_name = f"athena-high-scan-{wg}"

        # Set threshold per workgroup
        thresholds = {
            "finance_team": 5 * (1024 ** 3),       # 5 GB
            "marketing_team": 20 * (1024 ** 3),     # 20 GB
            "product_team": 10 * (1024 ** 3),       # 10 GB
            "data_engineering": 100 * (1024 ** 3)   # 100 GB
        }

        threshold = thresholds.get(wg, 10 * (1024 ** 3))
        threshold_gb = round(threshold / (1024 ** 3))

        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Athena data scanned exceeds {threshold_gb} GB for {wg}",
            Namespace="AWS/Athena",
            MetricName="ProcessedBytes",
            Dimensions=[
                {"Name": "WorkGroup", "Value": wg}
            ],
            Statistic="Sum",
            Period=86400,          # 24 hours
            EvaluationPeriods=1,
            Threshold=threshold,
            ComparisonOperator="GreaterThanThreshold",
            AlarmActions=[SNS_TOPIC_ARN],
            TreatMissingData="notBreaching",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Service", "Value": "Athena"}
            ]
        )
        print(f"✅ Alarm created: {alarm_name} (threshold: {threshold_gb} GB)")

    print(f"\n✅ CloudWatch alarms set for all workgroups")
    print(f"   Alerts sent to: {SNS_TOPIC_ARN}")


if __name__ == "__main__":
    generate_cost_report(hours=24)
    print()
    setup_cloudwatch_alarm()