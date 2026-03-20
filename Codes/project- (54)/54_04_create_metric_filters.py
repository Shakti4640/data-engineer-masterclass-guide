# file: 54_04_create_metric_filters.py
# Purpose: Create metric filters that extract numeric metrics from log text

import boto3
import json

REGION = "us-east-2"

# Log groups that pipeline components write to
LOG_GROUPS = {
    "glue_etl": "/aws-glue/jobs/error",
    "glue_etl_output": "/aws-glue/jobs/output",
    "databrew": "/aws-glue-databrew/jobs",
    "sqs_worker": "/quickcart/sqs-workers"
}


def create_log_group_if_not_exists(logs_client, log_group_name, retention_days=30):
    """Create log group with retention policy"""
    try:
        logs_client.create_log_group(
            logGroupName=log_group_name,
            tags={
                "Team": "Data-Engineering",
                "Pipeline": "nightly-etl"
            }
        )
        print(f"  ✅ Log group created: {log_group_name}")
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"  ℹ️  Log group exists: {log_group_name}")

    # Set retention (default is NEVER expire = growing cost)
    try:
        logs_client.put_retention_policy(
            logGroupName=log_group_name,
            retentionInDays=retention_days
        )
        print(f"     Retention: {retention_days} days")
    except Exception as e:
        print(f"     ⚠️  Retention policy: {e}")


def create_metric_filters():
    """
    Metric Filters: convert LOG TEXT into NUMERIC METRICS
    
    HOW IT WORKS:
    1. Log event arrives in CloudWatch Logs
    2. Metric filter pattern matches against log text
    3. If match → increment a CloudWatch metric by specified value
    4. Metric can then be used in alarms (just like any other metric)
    
    THIS IS THE BRIDGE between unstructured logs and structured monitoring
    """
    logs_client = boto3.client("logs", region_name=REGION)

    print("=" * 70)
    print("📝 CREATING CLOUDWATCH METRIC FILTERS")
    print("=" * 70)

    # Ensure log groups exist
    print("\n📌 Step 1: Ensuring log groups exist...")
    for name, log_group in LOG_GROUPS.items():
        create_log_group_if_not_exists(logs_client, log_group)

    # ═══════════════════════════════════════════════════
    # FILTER 1: Count ERROR lines in Glue ETL logs
    # ═══════════════════════════════════════════════════
    print(f"\n📌 Creating metric filters...")

    logs_client.put_metric_filter(
        logGroupName=LOG_GROUPS["glue_etl"],
        filterName="GlueETL-ErrorCount",
        filterPattern='"ERROR" -"No ERROR"',
        # Pattern explanation:
        # → Matches lines containing "ERROR"
        # → Excludes lines containing "No ERROR" (false positive)
        metricTransformations=[
            {
                "metricNamespace": "QuickCart/Pipeline",
                "metricName": "GlueETLErrorCount",
                "metricValue": "1",
                "defaultValue": 0,
                "unit": "Count"
            }
        ]
    )
    print(f"  ✅ GlueETL-ErrorCount")
    print(f"     Log: {LOG_GROUPS['glue_etl']}")
    print(f"     Pattern: '\"ERROR\" -\"No ERROR\"'")
    print(f"     Metric: QuickCart/Pipeline → GlueETLErrorCount")

    # ═══════════════════════════════════════════════════
    # FILTER 2: Count OutOfMemory errors in Glue logs
    # ═══════════════════════════════════════════════════
    logs_client.put_metric_filter(
        logGroupName=LOG_GROUPS["glue_etl"],
        filterName="GlueETL-OOMErrors",
        filterPattern='"OutOfMemoryError" OR "Java heap space" OR "GC overhead"',
        metricTransformations=[
            {
                "metricNamespace": "QuickCart/Pipeline",
                "metricName": "GlueETLOOMCount",
                "metricValue": "1",
                "defaultValue": 0,
                "unit": "Count"
            }
        ]
    )
    print(f"  ✅ GlueETL-OOMErrors")
    print(f"     Pattern: OOM-related keywords")
    print(f"     Metric: GlueETLOOMCount")

    # ═══════════════════════════════════════════════════
    # FILTER 3: Extract Glue job duration from output logs
    # (JSON-based log parsing)
    # ═══════════════════════════════════════════════════
    logs_client.put_metric_filter(
        logGroupName=LOG_GROUPS["glue_etl_output"],
        filterName="GlueETL-JobDuration",
        filterPattern='{ $.execution_time_seconds = * }',
        # JSON pattern: matches logs with execution_time_seconds field
        metricTransformations=[
            {
                "metricNamespace": "QuickCart/Pipeline",
                "metricName": "GlueETLDurationSeconds",
                "metricValue": "$.execution_time_seconds",
                # Value extracted FROM the log event field
                "defaultValue": 0,
                "unit": "Seconds"
            }
        ]
    )
    print(f"  ✅ GlueETL-JobDuration")
    print(f"     Pattern: JSON field extraction")
    print(f"     Metric: GlueETLDurationSeconds (value from log)")

    # ═══════════════════════════════════════════════════
    # FILTER 4: Count SQS worker processing errors
    # ═══════════════════════════════════════════════════
    logs_client.put_metric_filter(
        logGroupName=LOG_GROUPS["sqs_worker"],
        filterName="SQSWorker-ProcessingErrors",
        filterPattern='"Error processing" OR "Processing returned False"',
        metricTransformations=[
            {
                "metricNamespace": "QuickCart/Pipeline",
                "metricName": "SQSWorkerProcessingErrors",
                "metricValue": "1",
                "defaultValue": 0,
                "unit": "Count"
            }
        ]
    )
    print(f"  ✅ SQSWorker-ProcessingErrors")
    print(f"     Pattern: worker error messages (from Project 52)")
    print(f"     Metric: SQSWorkerProcessingErrors")

    # ═══════════════════════════════════════════════════
    # FILTER 5: Count DataBrew recipe step failures
    # ═══════════════════════════════════════════════════
    logs_client.put_metric_filter(
        logGroupName=LOG_GROUPS["databrew"],
        filterName="DataBrew-StepFailures",
        filterPattern='"FAILED" OR "RecipeStepError" OR "TransformException"',
        metricTransformations=[
            {
                "metricNamespace": "QuickCart/Pipeline",
                "metricName": "DataBrewStepFailures",
                "metricValue": "1",
                "defaultValue": 0,
                "unit": "Count"
            }
        ]
    )
    print(f"  ✅ DataBrew-StepFailures")
    print(f"     Pattern: DataBrew error keywords")
    print(f"     Metric: DataBrewStepFailures")

    # --- SUMMARY ---
    print(f"\n{'=' * 70}")
    print(f"📊 METRIC FILTER SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Filters created: 5")
    print(f"  Custom namespace: QuickCart/Pipeline")
    print(f"  Metrics available for alarms:")
    print(f"    → GlueETLErrorCount")
    print(f"    → GlueETLOOMCount")
    print(f"    → GlueETLDurationSeconds")
    print(f"    → SQSWorkerProcessingErrors")
    print(f"    → DataBrewStepFailures")
    print(f"")
    print(f"  These metrics can now be used in CloudWatch alarms")
    print(f"  just like any AWS-published metric")
    print(f"{'=' * 70}")


def test_metric_filter(log_group, test_message):
    """Publish a test log event to verify metric filter works"""
    logs_client = boto3.client("logs", region_name=REGION)
    import time

    # Create log stream
    stream_name = f"test-stream-{int(time.time())}"
    try:
        logs_client.create_log_stream(
            logGroupName=log_group,
            logStreamName=stream_name
        )
    except Exception:
        pass

    # Put log event
    logs_client.put_log_events(
        logGroupName=log_group,
        logStreamName=stream_name,
        logEvents=[
            {
                "timestamp": int(time.time() * 1000),
                "message": test_message
            }
        ]
    )
    print(f"\n🧪 Test log event published:")
    print(f"   Log group: {log_group}")
    print(f"   Message: {test_message}")
    print(f"   → Check metric in ~2 minutes (filter evaluation delay)")


if __name__ == "__main__":
    create_metric_filters()

    # Test: publish a fake error log to verify filter works
    test_metric_filter(
        LOG_GROUPS["glue_etl"],
        "2025-01-15 02:03:45 ERROR: Glue job quickcart-orders-etl failed with OutOfMemoryError: Java heap space"
    )