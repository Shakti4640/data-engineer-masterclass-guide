# file: 54_07_test_alarms.py
# Purpose: Test each alarm by publishing fake failure metrics

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/monitoring_config.json", "r") as f:
        return json.load(f)


def test_alarm_by_publishing_metric(metric_namespace, metric_name,
                                     dimensions, value, description):
    """Publish a metric value that should trigger an alarm"""
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    cw_client.put_metric_data(
        Namespace=metric_namespace,
        MetricData=[
            {
                "MetricName": metric_name,
                "Timestamp": datetime.utcnow(),
                "Value": value,
                "Unit": "Count",
                "Dimensions": [
                    {"Name": k, "Value": v} for k, v in dimensions.items()
                ]
            }
        ]
    )
    print(f"  📤 Published: {metric_namespace}/{metric_name} = {value}")
    print(f"     {description}")


def verify_alarm_state(alarm_name, expected_state, max_wait=180):
    """Wait for alarm to reach expected state"""
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    print(f"\n  ⏳ Waiting for alarm '{alarm_name}' to reach '{expected_state}'...")

    start = time.time()
    while (time.time() - start) < max_wait:
        response = cw_client.describe_alarms(AlarmNames=[alarm_name])

        if response["MetricAlarms"]:
            current_state = response["MetricAlarms"][0]["StateValue"]
            if current_state == expected_state:
                print(f"  ✅ Alarm '{alarm_name}' is now in '{expected_state}' state")
                return True
            else:
                elapsed = int(time.time() - start)
                print(f"     [{elapsed}s] Current: {current_state} (waiting for {expected_state})")

        elif response.get("CompositeAlarms"):
            current_state = response["CompositeAlarms"][0]["StateValue"]
            if current_state == expected_state:
                print(f"  ✅ Composite alarm '{alarm_name}' is now '{expected_state}'")
                return True

        time.sleep(15)

    print(f"  ⚠️  Timeout: alarm didn't reach '{expected_state}' in {max_wait}s")
    return False


def run_alarm_tests():
    """
    Test each alarm by triggering it with fake data
    
    TEST PLAN:
    1. Publish failure metric → verify alarm fires
    2. Verify SNS notification received (check email)
    3. Verify composite alarm state changes
    4. Publish success metric → verify alarm clears
    """
    print("=" * 70)
    print("🧪 ALARM TESTING SUITE")
    print("=" * 70)
    print("⚠️  This will trigger REAL notifications!")
    print("   → Check email for alarm notifications")
    print("   → Check SMS if configured")
    print("")

    # ═══════════════════════════════════════════════════
    # TEST 1: Trigger DLQ alarm (easiest to test)
    # ═══════════════════════════════════════════════════
    print("📌 TEST 1: SQS DLQ Alarm")
    print("   Sending test message to DLQ to trigger alarm...")

    sqs_client = boto3.client("sqs", region_name=REGION)
    try:
        # Create a test DLQ if it doesn't exist
        dlq_url = sqs_client.create_queue(
            QueueName="quickcart-etl-queue-dlq",
            Attributes={"MessageRetentionPeriod": "86400"}
        )["QueueUrl"]

        # Send a message to DLQ (simulates failed processing)
        sqs_client.send_message(
            QueueUrl=dlq_url,
            MessageBody=json.dumps({
                "test": True,
                "purpose": "alarm testing",
                "timestamp": datetime.utcnow().isoformat()
            })
        )
        print(f"  ✅ Test message sent to DLQ")
        print(f"  → DLQ metric will update within 5 minutes")
        print(f"  → Alarm should fire within 6-7 minutes")
    except Exception as e:
        print(f"  ⚠️  Could not send to DLQ: {e}")

    # ═══════════════════════════════════════════════════
    # TEST 2: Trigger upload missing alarm
    # ═══════════════════════════════════════════════════
    print(f"\n📌 TEST 2: S3 Upload Missing Alarm")
    print("   This alarm uses TreatMissingData=breaching")
    print("   If DailyUploadSuccess metric is NOT published → alarm fires")
    print("   → Simply NOT publishing the metric = test passes")
    print("   → Check: alarm should be in ALARM or INSUFFICIENT_DATA")

    # ═══════════════════════════════════════════════════
    # TEST 3: Publish custom error metric
    # ═══════════════════════════════════════════════════
    print(f"\n📌 TEST 3: Custom Error Metrics")
    test_alarm_by_publishing_metric(
        metric_namespace="QuickCart/Pipeline",
        metric_name="GlueETLErrorCount",
        dimensions={},
        value=5,
        description="Simulating 5 Glue ETL errors"
    )

    # ═══════════════════════════════════════════════════
    # CHECK ALL ALARM STATES
    # ═══════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print(f"📊 CURRENT ALARM STATES")
    print(f"{'=' * 70}")

    cw_client = boto3.client("cloudwatch", region_name=REGION)

    # Get all pipeline alarms
    response = cw_client.describe_alarms(
        AlarmNamePrefix="Pipeline-"
    )

    for alarm in response.get("MetricAlarms", []):
        state = alarm["StateValue"]
        icon = {"OK": "🟢", "ALARM": "🔴", "INSUFFICIENT_DATA": "⚪"}.get(state, "❓")
        print(f"  {icon} {alarm['AlarmName']}: {state}")

    for alarm in response.get("CompositeAlarms", []):
        state = alarm["StateValue"]
        icon = {"OK": "🟢", "ALARM": "🔴", "INSUFFICIENT_DATA": "⚪"}.get(state, "❓")
        print(f"  {icon} {alarm['AlarmName']}: {state} (COMPOSITE)")

    print(f"\n{'=' * 70}")
    print(f"📋 VERIFICATION CHECKLIST:")
    print(f"  □ Email received for CRITICAL alarm?")
    print(f"  □ SMS received (if configured)?")
    print(f"  □ Composite alarm changed state?")
    print(f"  □ Dashboard shows correct alarm colors?")
    print(f"  □ Alarm description contains runbook link?")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    run_alarm_tests()