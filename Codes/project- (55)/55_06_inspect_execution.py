# file: 55_06_inspect_execution.py
# Purpose: Deep inspection of execution — debug failures

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/step_functions_config.json", "r") as f:
        return json.load(f)


def get_latest_execution():
    """Get the most recent execution"""
    config = load_config()
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    response = sfn_client.list_executions(
        stateMachineArn=config["state_machine_arn"],
        maxResults=1
    )

    if not response["executions"]:
        print("❌ No executions found")
        return None

    return response["executions"][0]["executionArn"]


def inspect_execution_detail(execution_arn=None):
    """
    Deep inspection of a specific execution
    Shows every state transition with full input/output
    
    THIS IS HOW YOU DEBUG PIPELINE FAILURES:
    → Find the failed state
    → See exactly what input it received
    → See the exact error message
    → Determine: was it bad input? Permission? Timeout? Service error?
    """
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    if execution_arn is None:
        execution_arn = get_latest_execution()
        if not execution_arn:
            return

    # Get execution details
    execution = sfn_client.describe_execution(executionArn=execution_arn)

    print("=" * 70)
    print("🔍 EXECUTION DEEP INSPECTION")
    print("=" * 70)
    print(f"  Name:    {execution['name']}")
    print(f"  Status:  {execution['status']}")
    print(f"  Started: {execution['startDate']}")
    if "stopDate" in execution:
        duration = (execution["stopDate"] - execution["startDate"]).total_seconds()
        print(f"  Stopped: {execution['stopDate']}")
        print(f"  Duration: {duration:.0f} seconds ({duration/60:.1f} minutes)")

    print(f"\n  Input:")
    input_json = json.loads(execution.get("input", "{}"))
    print(f"  {json.dumps(input_json, indent=4)[:500]}")

    if execution["status"] == "SUCCEEDED":
        print(f"\n  Output:")
        output_json = json.loads(execution.get("output", "{}"))
        print(f"  {json.dumps(output_json, indent=4)[:500]}")

    elif execution["status"] == "FAILED":
        print(f"\n  ❌ Error: {execution.get('error', 'N/A')}")
        print(f"  ❌ Cause: {execution.get('cause', 'N/A')[:500]}")

    # Get full history
    print(f"\n{'─' * 70}")
    print(f"📜 STATE TRANSITION HISTORY")
    print(f"{'─' * 70}")

    history = sfn_client.get_execution_history(
        executionArn=execution_arn,
        maxResults=200,
        reverseOrder=False
    )

    state_timings = {}
    current_state = None
    state_enter_time = None

    for event in history["events"]:
        event_type = event["type"]
        timestamp = event["timestamp"]
        ts_str = timestamp.strftime("%H:%M:%S.%f")[:-3]

        if "stateEnteredEventDetails" in event:
            state_name = event["stateEnteredEventDetails"]["name"]
            current_state = state_name
            state_enter_time = timestamp
            input_data = event["stateEnteredEventDetails"].get("input", "{}")

            print(f"\n  ▶️  [{ts_str}] ENTERED: {state_name}")
            # Show truncated input
            if len(input_data) > 200:
                input_data = input_data[:200] + "..."
            print(f"      Input: {input_data}")

        elif "stateExitedEventDetails" in event:
            state_name = event["stateExitedEventDetails"]["name"]
            output_data = event["stateExitedEventDetails"].get("output", "{}")

            if state_enter_time:
                duration = (timestamp - state_enter_time).total_seconds()
                state_timings[state_name] = duration
                print(f"  ✅  [{ts_str}] EXITED:  {state_name} ({duration:.1f}s)")
            else:
                print(f"  ✅  [{ts_str}] EXITED:  {state_name}")

            if len(output_data) > 200:
                output_data = output_data[:200] + "..."
            print(f"      Output: {output_data}")

        elif "taskFailedEventDetails" in event:
            details = event["taskFailedEventDetails"]
            print(f"  ❌  [{ts_str}] TASK FAILED")
            print(f"      Error: {details.get('error', 'N/A')}")
            cause = details.get("cause", "N/A")
            if len(cause) > 300:
                cause = cause[:300] + "..."
            print(f"      Cause: {cause}")

    # State timing summary
    if state_timings:
        print(f"\n{'─' * 70}")
        print(f"⏱️  STATE TIMING SUMMARY")
        print(f"{'─' * 70}")
        total = sum(state_timings.values())
        for state, duration in sorted(state_timings.items(), key=lambda x: x[1], reverse=True):
            pct = (duration / total * 100) if total > 0 else 0
            bar = "█" * int(pct / 2)
            print(f"  {state:<30} {duration:>8.1f}s ({pct:>5.1f}%) {bar}")
        print(f"  {'TOTAL':<30} {total:>8.1f}s")

    print(f"{'=' * 70}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        inspect_execution_detail(sys.argv[1])
    else:
        inspect_execution_detail()