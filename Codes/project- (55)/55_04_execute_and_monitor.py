# file: 55_04_execute_and_monitor.py
# Purpose: Start a pipeline execution and monitor its progress

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/step_functions_config.json", "r") as f:
        return json.load(f)


def start_execution(execution_input=None):
    """
    Start a new pipeline execution
    
    In production: EventBridge rule triggers this at 1 AM daily
    For testing: we call it manually
    """
    config = load_config()
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    today = datetime.utcnow().strftime("%Y-%m-%d")
    execution_name = f"nightly-{today}-{int(time.time())}"

    if execution_input is None:
        execution_input = {
            "execution_date": today,
            "pipeline": "nightly-etl",
            "triggered_by": "manual",
            "source_bucket": "quickcart-raw-data-prod",
            "source_prefix": f"daily_exports/"
        }

    print("=" * 70)
    print("🚀 STARTING PIPELINE EXECUTION")
    print("=" * 70)
    print(f"  State Machine: {config['state_machine_name']}")
    print(f"  Execution Name: {execution_name}")
    print(f"  Input: {json.dumps(execution_input, indent=2)}")

    response = sfn_client.start_execution(
        stateMachineArn=config["state_machine_arn"],
        name=execution_name,
        input=json.dumps(execution_input)
    )

    execution_arn = response["executionArn"]
    print(f"\n  ✅ Execution started!")
    print(f"  Execution ARN: {execution_arn}")

    # Console URL for this execution
    console_url = (
        f"https://{REGION}.console.aws.amazon.com/states/home?"
        f"region={REGION}#/v2/executions/details/{execution_arn}"
    )
    print(f"  🔗 Console URL: {console_url}")
    print(f"     → Open to see LIVE visual execution progress")

    return execution_arn


def monitor_execution(execution_arn, poll_interval=10, max_wait=1800):
    """
    Monitor execution progress — show state transitions in real-time
    
    THIS IS THE KEY VALUE OF STEP FUNCTIONS:
    → See exactly which state is running
    → See exactly where it failed
    → See exact input/output of each state
    → All in one place (not scattered across 6 services)
    """
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    print(f"\n{'=' * 70}")
    print(f"📊 MONITORING EXECUTION PROGRESS")
    print(f"{'=' * 70}")
    print(f"  Polling every {poll_interval}s (max wait: {max_wait}s)")
    print(f"{'─' * 70}")

    start_time = time.time()
    last_event_count = 0

    while (time.time() - start_time) < max_wait:
        # Get execution status
        response = sfn_client.describe_execution(
            executionArn=execution_arn
        )
        status = response["status"]
        elapsed = int(time.time() - start_time)

        # Get execution history (shows state transitions)
        history = sfn_client.get_execution_history(
            executionArn=execution_arn,
            maxResults=100,
            reverseOrder=False
        )

        events = history.get("events", [])

        # Show new events since last check
        for event in events[last_event_count:]:
            event_type = event["type"]
            event_id = event["id"]
            timestamp = event["timestamp"].strftime("%H:%M:%S")

            # Extract state name from event details
            state_name = ""
            details = {}

            if "stateEnteredEventDetails" in event:
                state_name = event["stateEnteredEventDetails"]["name"]
                icon = "▶️"
                print(f"  [{timestamp}] {icon} ENTERED: {state_name}")

            elif "stateExitedEventDetails" in event:
                state_name = event["stateExitedEventDetails"]["name"]
                icon = "✅"
                print(f"  [{timestamp}] {icon} EXITED:  {state_name}")

            elif "taskStartedEventDetails" in event:
                icon = "⚙️"
                print(f"  [{timestamp}] {icon} Task started (event #{event_id})")

            elif "taskSucceededEventDetails" in event:
                icon = "✅"
                output = event["taskSucceededEventDetails"].get("output", "{}")
                # Truncate long output
                if len(output) > 100:
                    output = output[:100] + "..."
                print(f"  [{timestamp}] {icon} Task succeeded: {output}")

            elif "taskFailedEventDetails" in event:
                icon = "❌"
                error = event["taskFailedEventDetails"].get("error", "Unknown")
                cause = event["taskFailedEventDetails"].get("cause", "Unknown")
                if len(cause) > 150:
                    cause = cause[:150] + "..."
                print(f"  [{timestamp}] {icon} Task FAILED: {error}")
                print(f"             Cause: {cause}")

            elif "executionSucceededEventDetails" in event:
                print(f"  [{timestamp}] 🎉 EXECUTION SUCCEEDED")

            elif "executionFailedEventDetails" in event:
                error = event["executionFailedEventDetails"].get("error", "Unknown")
                cause = event["executionFailedEventDetails"].get("cause", "Unknown")
                print(f"  [{timestamp}] 💥 EXECUTION FAILED: {error}")
                print(f"             Cause: {cause}")

        last_event_count = len(events)

        # Check terminal states
        if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
            print(f"{'─' * 70}")
            status_icon = {
                "SUCCEEDED": "✅",
                "FAILED": "❌",
                "TIMED_OUT": "⏰",
                "ABORTED": "🛑"
            }.get(status, "❓")

            print(f"\n  {status_icon} EXECUTION {status}")
            print(f"  Duration: {elapsed} seconds")

            if status == "SUCCEEDED":
                output = json.loads(response.get("output", "{}"))
                print(f"  Output: {json.dumps(output, indent=4)[:500]}")

            elif status == "FAILED":
                error = response.get("error", "Unknown")
                cause = response.get("cause", "Unknown")
                print(f"  Error: {error}")
                print(f"  Cause: {cause[:300]}")

            return status

        time.sleep(poll_interval)

    print(f"\n  ⏰ Monitoring timeout ({max_wait}s) — execution still running")
    print(f"  → Check console for continued progress")
    return "MONITORING_TIMEOUT"


def list_recent_executions(max_results=10):
    """List recent pipeline executions"""
    config = load_config()
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    response = sfn_client.list_executions(
        stateMachineArn=config["state_machine_arn"],
        maxResults=max_results
    )

    print(f"\n{'=' * 70}")
    print(f"📋 RECENT PIPELINE EXECUTIONS (last {max_results})")
    print(f"{'=' * 70}")
    print(f"  {'Name':<35} {'Status':<12} {'Started':<20} {'Duration'}")
    print(f"  {'─' * 75}")

    for execution in response.get("executions", []):
        name = execution["name"]
        status = execution["status"]
        started = execution["startDate"].strftime("%Y-%m-%d %H:%M:%S")

        if "stopDate" in execution:
            duration = (execution["stopDate"] - execution["startDate"]).total_seconds()
            duration_str = f"{duration:.0f}s"
        else:
            duration_str = "running..."

        icon = {
            "SUCCEEDED": "✅",
            "FAILED": "❌",
            "RUNNING": "🔄",
            "TIMED_OUT": "⏰",
            "ABORTED": "🛑"
        }.get(status, "❓")

        print(f"  {icon} {name:<33} {status:<12} {started:<20} {duration_str}")

    print(f"{'=' * 70}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "list":
        list_recent_executions()
    else:
        execution_arn = start_execution()
        final_status = monitor_execution(execution_arn, poll_interval=5)