# file: 64_05_federation_monitoring.py
# Run from: Account B[REDACTED:BANK_ACCOUNT_NUMBER]22)
# Purpose: Monitor federated query performance, cost, and connector health

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"
ACCOUNT_B_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"
WORKGROUP = "federated-queries"

cw = boto3.client("cloudwatch", region_name=REGION)
athena = boto3.client("athena", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"


def get_federation_query_stats(hours=24):
    """
    Analyze federated query performance over last N hours
    
    USES: Athena query history API
    → List all queries in federated workgroup
    → Calculate: avg duration, data scanned, success rate
    """
    print(f"📊 Federated Query Statistics (last {hours} hours)")
    print("-" * 60)

    since = datetime.utcnow() - timedelta(hours=hours)

    # List query executions in federated workgroup
    paginator = athena.get_paginator("list_query_executions")

    query_ids = []
    for page in paginator.paginate(WorkGroup=WORKGROUP):
        query_ids.extend(page.get("QueryExecutionIds", []))

    if not query_ids:
        print("   No queries found in this period")
        return

    # Get details in batches of 50
    all_queries = []
    for i in range(0, len(query_ids), 50):
        batch = query_ids[i:i + 50]
        response = athena.batch_get_query_execution(QueryExecutionIds=batch)
        all_queries.extend(response.get("QueryExecutions", []))

    # Filter by time window
    recent = [
        q for q in all_queries
        if q.get("Status", {}).get("SubmissionDateTime", datetime.min).replace(tzinfo=None) > since
    ]

    if not recent:
        print(f"   No queries in last {hours} hours")
        return

    # Analyze
    succeeded = [q for q in recent if q["Status"]["State"] == "SUCCEEDED"]
    failed = [q for q in recent if q["Status"]["State"] == "FAILED"]

    total_scan_bytes = sum(
        q.get("Statistics", {}).get("DataScannedInBytes", 0)
        for q in succeeded
    )
    total_scan_mb = total_scan_bytes / (1024 * 1024)

    durations = [
        q.get("Statistics", {}).get("TotalExecutionTimeInMillis", 0) / 1000
        for q in succeeded
    ]
    avg_duration = sum(durations) / len(durations) if durations else 0
    max_duration = max(durations) if durations else 0

    # Estimated cost
    athena_cost = (total_scan_bytes / (1024**4)) * 5.0  # $5/TB scanned

    print(f"   Total queries: {len(recent)}")
    print(f"   Succeeded: {len(succeeded)}")
    print(f"   Failed: {len(failed)}")
    print(f"   Success rate: {len(succeeded)/len(recent)*100:.1f}%")
    print(f"   Avg duration: {avg_duration:.2f}s")
    print(f"   Max duration: {max_duration:.2f}s")
    print(f"   Total data scanned: {total_scan_mb:.2f} MB")
    print(f"   Estimated Athena cost: ${athena_cost:.4f}")

    # Show failed queries
    if failed:
        print(f"\n   ❌ Failed queries:")
        for q in failed[:5]:
            error = q["Status"].get("StateChangeReason", "Unknown")
            sql_preview = q.get("Query", "")[:80]
            print(f"      → {sql_preview}...")
            print(f"        Error: {error[:100]}")

    return {
        "total": len(recent),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "avg_duration_s": round(avg_duration, 2),
        "total_scan_mb": round(total_scan_mb, 2),
        "estimated_cost": round(athena_cost, 4)
    }


def check_connector_lambda_health():
    """
    Check health of connector Lambda functions
    → Invocation count, error rate, duration, throttles
    """
    print(f"\n📡 Connector Lambda Health")
    print("-" * 60)

    connectors = [
        "athena-mysql-connector",
        "athena-dynamodb-connector"
    ]

    for connector_name in connectors:
        print(f"\n   🔌 {connector_name}:")

        # Get CloudWatch metrics
        for metric_name, label in [
            ("Invocations", "Invocations"),
            ("Errors", "Errors"),
            ("Duration", "Avg Duration (ms)"),
            ("Throttles", "Throttles"),
            ("ConcurrentExecutions", "Max Concurrent")
        ]:
            try:
                response = cw.get_metric_statistics(
                    Namespace="AWS/Lambda",
                    MetricName=metric_name,
                    Dimensions=[
                        {"Name": "FunctionName", "Value": connector_name}
                    ],
                    StartTime=datetime.utcnow() - timedelta(hours=24),
                    EndTime=datetime.utcnow(),
                    Period=86400,  # 24 hours aggregate
                    Statistics=["Sum", "Average", "Maximum"]
                )

                datapoints = response.get("Datapoints", [])
                if datapoints:
                    dp = datapoints[0]
                    if metric_name == "Duration":
                        value = f"{dp.get('Average', 0):.0f}ms (max: {dp.get('Maximum', 0):.0f}ms)"
                    elif metric_name == "ConcurrentExecutions":
                        value = f"{dp.get('Maximum', 0):.0f}"
                    else:
                        value = f"{dp.get('Sum', 0):.0f}"
                    print(f"      {label}: {value}")
                else:
                    print(f"      {label}: no data")

            except Exception as e:
                print(f"      {label}: error — {str(e)[:50]}")


def check_cross_account_iam_health():
    """
    Verify cross-account AssumeRole still works
    → Trust policies can be modified by Account C admin
    → Periodic verification prevents surprise failures
    """
    print(f"\n🔐 Cross-Account IAM Verification")
    print("-" * 60)

    sts = boto3.client("sts", region_name=REGION)

    roles_to_check = [
        {
            "name": "Account C DynamoDB Read",
            "arn": "arn:aws:iam::333333333333:role/athena-federation-cross-account-read",
            "external_id": "athena-federation-2025"
        }
    ]

    for role in roles_to_check:
        print(f"\n   Checking: {role['name']}")
        print(f"   Role ARN: {role['arn']}")

        try:
            response = sts.assume_role(
                RoleArn=role["arn"],
                RoleSessionName="federation-health-check",
                ExternalId=role["external_id"],
                DurationSeconds=900  # Minimum: 15 minutes
            )

            expiry = response["Credentials"]["Expiration"]
            print(f"   ✅ AssumeRole SUCCEEDED")
            print(f"   Credentials expire: {expiry}")

        except sts.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            print(f"   ❌ AssumeRole FAILED: {error_code}")
            print(f"   Error: {e.response['Error']['Message']}")

            # Alert on failure
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"❌ Cross-Account IAM Failure: {role['name']}",
                Message=(
                    f"AssumeRole failed for: {role['arn']}\n"
                    f"Error: {error_code}\n"
                    f"Impact: Athena federated queries to Account C will fail\n"
                    f"Action: Contact Account C admin to verify trust policy"
                )
            )


def publish_federation_metrics(stats):
    """
    Publish custom CloudWatch metrics for federation monitoring dashboard
    """
    if not stats:
        return

    timestamp = datetime.utcnow()

    cw.put_metric_data(
        Namespace="QuickCart/AthenaFederation",
        MetricData=[
            {
                "MetricName": "QueryCount",
                "Value": stats["total"],
                "Timestamp": timestamp,
                "Unit": "Count"
            },
            {
                "MetricName": "SuccessRate",
                "Value": stats["succeeded"] / max(stats["total"], 1) * 100,
                "Timestamp": timestamp,
                "Unit": "Percent"
            },
            {
                "MetricName": "AvgQueryDuration",
                "Value": stats["avg_duration_s"],
                "Timestamp": timestamp,
                "Unit": "Seconds"
            },
            {
                "MetricName": "DataScannedMB",
                "Value": stats["total_scan_mb"],
                "Timestamp": timestamp,
                "Unit": "Megabytes"
            },
            {
                "MetricName": "EstimatedCostUSD",
                "Value": stats["estimated_cost"],
                "Timestamp": timestamp,
                "Unit": "None"
            }
        ]
    )
    print(f"\n✅ CloudWatch metrics published to QuickCart/AthenaFederation")


def run_full_health_check():
    """
    Complete federation health check
    → Run as Lambda on daily schedule or before critical queries
    """
    print("=" * 70)
    print("🏥 ATHENA FEDERATION HEALTH CHECK")
    print(f"   Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    # 1. Query statistics
    stats = get_federation_query_stats(hours=24)

    # 2. Lambda connector health
    check_connector_lambda_health()

    # 3. Cross-account IAM
    check_cross_account_iam_health()

    # 4. Publish metrics
    if stats:
        publish_federation_metrics(stats)

    print(f"\n{'='*70}")
    print("✅ HEALTH CHECK COMPLETE")
    print(f"{'='*70}")


if __name__ == "__main__":
    run_full_health_check()