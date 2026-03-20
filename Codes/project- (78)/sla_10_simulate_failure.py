# file: sla/10_simulate_failure.py
# Purpose: Simulate a real failure scenario to validate the entire SLA system
# Tests: Detection → Auto-Recovery → Escalation → Incident Tracking → SLA Metrics

import boto3
import json
import time
import uuid
from datetime import datetime, timezone

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def simulate_glue_failure_event():
    """
    Simulate a Glue job failure event via EventBridge
    
    WHY SIMULATE:
    → Cannot intentionally break production Glue jobs
    → EventBridge accepts custom events that match patterns
    → Our rule catches the event and triggers Step Functions
    → Full recovery pipeline executes end-to-end
    
    In production: this event comes from Glue automatically
    In testing: we inject it manually
    """
    events = boto3.client("events", region_name=REGION)

    simulated_event = {
        "Source": "aws.glue",
        "DetailType": "Glue Job State Change",
        "Detail": json.dumps({
            "jobName": "job_clean_orders",
            "state": "FAILED",
            "message": "OutOfMemoryError: GC overhead limit exceeded — processing 50M rows with 10 DPU",
            "jobRunId": f"jr_sim_{uuid.uuid4().hex[:8]}",
            "severity": "ERROR"
        }),
        "EventBusName": "default"
    }

    print("=" * 70)
    print("🧪 SIMULATING GLUE JOB FAILURE")
    print("=" * 70)
    print(f"   Job: job_clean_orders")
    print(f"   Error: OutOfMemoryError")
    print(f"   Time: {datetime.now(timezone.utc).isoformat()}")
    print()

    response = events.put_events(Entries=[simulated_event])

    failed_count = response.get("FailedEntryCount", 0)
    if failed_count == 0:
        print("✅ Event injected successfully into EventBridge")
    else:
        print(f"❌ Event injection failed: {response}")
        return False

    return True


def verify_step_function_started():
    """
    Check that Step Functions execution was triggered by the event
    """
    sfn = boto3.client("stepfunctions", region_name=REGION)
    sfn_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:PipelineAutoRecovery"

    print("\n⏳ Waiting 10 seconds for Step Functions to start...")
    time.sleep(10)

    response = sfn.list_executions(
        stateMachineArn=sfn_arn,
        statusFilter="RUNNING",
        maxResults=5
    )

    executions = response.get("executions", [])

    if executions:
        latest = executions[0]
        print(f"✅ Step Functions execution detected:")
        print(f"   Name: {latest['name']}")
        print(f"   Status: {latest['status']}")
        print(f"   Started: {latest['startDate'].isoformat()}")
        print(f"   ARN: {latest['executionArn']}")
        return latest["executionArn"]
    else:
        print("⚠️  No running executions found yet")
        print("   → Check EventBridge rule is enabled")
        print("   → Check IAM role allows EventBridge → Step Functions")
        return None


def monitor_execution(execution_arn, timeout_seconds=600):
    """
    Monitor Step Functions execution progress in real-time
    
    Prints state transitions as they happen:
    → LogIncidentStart ✅
    → NotifyInfo ✅
    → DetermineFailureType → TRANSIENT
    → RetryGlueJob ⏳ (waiting...)
    → RetryGlueJob ❌ (failed, retrying 2/3)
    → ...
    """
    if not execution_arn:
        print("⚠️  No execution to monitor")
        return None

    sfn = boto3.client("stepfunctions", region_name=REGION)

    print(f"\n📊 MONITORING EXECUTION")
    print("-" * 70)

    seen_events = set()
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        # Get execution status
        exec_response = sfn.describe_execution(
            executionArn=execution_arn
        )
        status = exec_response["status"]

        # Get execution history
        history_response = sfn.get_execution_history(
            executionArn=execution_arn,
            maxResults=100,
            reverseOrder=False
        )

        for event in history_response["events"]:
            event_id = event["id"]
            if event_id in seen_events:
                continue
            seen_events.add(event_id)

            event_type = event["type"]
            timestamp = event["timestamp"].strftime("%H:%M:%S")

            # Format based on event type
            if "stateEnteredEventDetails" in event:
                state_name = event["stateEnteredEventDetails"]["name"]
                print(f"   [{timestamp}] ▶️  Entered: {state_name}")

            elif "stateExitedEventDetails" in event:
                state_name = event["stateExitedEventDetails"]["name"]
                print(f"   [{timestamp}] ✅ Exited:  {state_name}")

            elif "taskFailedEventDetails" in event:
                error = event["taskFailedEventDetails"].get("error", "")
                cause = event["taskFailedEventDetails"].get("cause", "")[:100]
                print(f"   [{timestamp}] ❌ Failed:  {error} — {cause}")

            elif "executionFailedEventDetails" in event:
                print(f"   [{timestamp}] 🔴 Execution FAILED")

            elif "executionSucceededEventDetails" in event:
                print(f"   [{timestamp}] 🟢 Execution SUCCEEDED")

        # Check terminal states
        if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
            print(f"\n   Final Status: {status}")

            if status == "SUCCEEDED":
                output = json.loads(exec_response.get("output", "{}"))
                print(f"   Output: {json.dumps(output, indent=4)[:500]}")
            elif status == "FAILED":
                error = exec_response.get("error", "Unknown")
                cause = exec_response.get("cause", "Unknown")
                print(f"   Error: {error}")
                print(f"   Cause: {cause[:300]}")

            return status

        time.sleep(5)

    print(f"\n⚠️  Monitoring timed out after {timeout_seconds}s")
    return "TIMEOUT"


def verify_incident_created():
    """Check DynamoDB for the incident record"""
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table("SLAIncidentTracker")

    # Scan for recent incidents
    today = datetime.now().strftime("%Y-%m")
    response = table.query(
        IndexName="GSI-MonthPipeline",
        KeyConditionExpression=(
            boto3.dynamodb.conditions.Key("month_key").eq(today)
        ),
        ScanIndexForward=False,  # Most recent first
        Limit=5
    )

    incidents = response.get("Items", [])

    if incidents:
        print(f"\n📋 RECENT INCIDENTS:")
        print("-" * 70)
        for inc in incidents:
            status_icon = {
                "OPEN": "🔴",
                "RESOLVED": "✅",
                "ESCALATED": "⚠️"
            }.get(inc.get("status", ""), "⚪")

            print(f"   {status_icon} {inc['incident_id']}")
            print(f"      Pipeline: {inc.get('pipeline', 'N/A')}")
            print(f"      Status: {inc.get('status', 'N/A')}")
            print(f"      Started: {inc.get('started_at', 'N/A')}")
            print(f"      MTTR: {inc.get('mttr_minutes', 'N/A')} minutes")
            print(f"      Resolution: {inc.get('resolution', 'N/A')}")
            print()
    else:
        print("\n⚠️  No incidents found in DynamoDB")

    return incidents


def verify_cloudwatch_metrics():
    """Check that SLA metrics were published to CloudWatch"""
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print(f"\n📊 CLOUDWATCH SLA METRICS:")
    print("-" * 70)

    metrics_to_check = [
        ("QuickCart/SLA", "IncidentCount", "finance_summary"),
        ("QuickCart/SLA", "MTTRMinutes", "finance_summary"),
        ("QuickCart/SLA", "AutoRecoverySuccess", "finance_summary"),
        ("QuickCart/DataPlatform", "DataStalenessMinutes", "finance_summary"),
    ]

    for namespace, metric_name, pipeline in metrics_to_check:
        response = cloudwatch.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[
                {"Name": "Pipeline", "Value": pipeline}
            ],
            StartTime=datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0
            ),
            EndTime=datetime.now(timezone.utc),
            Period=86400,
            Statistics=["Sum", "Average", "Maximum"]
        )

        datapoints = response.get("Datapoints", [])
        if datapoints:
            dp = datapoints[-1]
            print(
                f"   ✅ {namespace}/{metric_name}: "
                f"Sum={dp.get('Sum', 'N/A')}, "
                f"Avg={dp.get('Average', 'N/A')}, "
                f"Max={dp.get('Maximum', 'N/A')}"
            )
        else:
            print(f"   ⚠️  {namespace}/{metric_name}: No data points yet")


def run_full_simulation():
    """
    Execute complete end-to-end simulation
    
    FLOW:
    1. Inject failure event → EventBridge
    2. Verify Step Functions started
    3. Monitor execution progress
    4. Verify incident was logged
    5. Verify CloudWatch metrics published
    6. Generate SLA report
    """
    print("=" * 70)
    print("🧪 FULL SLA SYSTEM END-TO-END TEST")
    print(f"   Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 70)

    # Step 1: Inject failure
    success = simulate_glue_failure_event()
    if not success:
        print("❌ Simulation aborted — event injection failed")
        return

    # Step 2: Verify Step Functions triggered
    execution_arn = verify_step_function_started()

    # Step 3: Monitor execution
    final_status = monitor_execution(execution_arn, timeout_seconds=300)

    # Step 4: Verify incident logged
    incidents = verify_incident_created()

    # Step 5: Verify metrics
    verify_cloudwatch_metrics()

    # Step 6: Generate SLA report
    print("\n" + "=" * 70)
    print("📊 GENERATING SLA REPORT")
    print("=" * 70)

    from incident_tracker import SLACalculator, print_sla_report
    calculator = SLACalculator()
    now = datetime.now()
    monthly = calculator.calculate_monthly_sla(now.year, now.month, "finance_summary")
    print_sla_report(monthly)

    # Final summary
    print("\n" + "=" * 70)
    print("🏁 SIMULATION COMPLETE")
    print("=" * 70)
    print(f"""
    Results:
    ├── Event Injected:      ✅
    ├── Step Functions:       {'✅' if execution_arn else '❌'} {final_status or 'N/A'}
    ├── Incident Logged:      {'✅' if incidents else '❌'} ({len(incidents or [])} found)
    ├── Metrics Published:    (check CloudWatch dashboard)
    └── SLA Report:           Generated above
    
    WHAT TO VERIFY MANUALLY:
    1. Check Slack channels for tiered notifications
    2. Check PagerDuty for critical/emergency alerts
    3. Open CloudWatch Dashboard: DataPlatform-SLA-Dashboard
    4. Query DynamoDB: SLAIncidentTracker
    """)


if __name__ == "__main__":
    run_full_simulation()