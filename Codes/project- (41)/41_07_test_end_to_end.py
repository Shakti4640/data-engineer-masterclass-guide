# file: 41_07_test_end_to_end.py
# Purpose: Upload a test file and watch the entire pipeline trigger automatically

import boto3
import json
import time
import csv
import os
import random
from datetime import datetime

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET_NAME = "quickcart-raw-data-prod"
STATE_MACHINE_ARN = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-etl-pipeline"


def generate_and_upload_test_file():
    """
    Generate a small CSV and upload it to S3.
    This should AUTOMATICALLY trigger the entire pipeline.
    """
    s3_client = boto3.client("s3", region_name=REGION)

    today = datetime.now().strftime("%Y%m%d")
    entity = "orders"
    filename = f"{entity}_{today}.csv"
    s3_key = f"daily_exports/{entity}/{filename}"

    # Generate CSV content in memory (no temp file needed)
    rows = []
    rows.append("order_id,customer_id,product_id,quantity,unit_price,total_amount,status,order_date")
    
    statuses = ["completed", "pending", "shipped", "cancelled"]
    for i in range(1, 101):  # 100 test rows
        qty = random.randint(1, 5)
        price = round(random.uniform(9.99, 199.99), 2)
        rows.append(
            f"ORD-{today}-{i:06d},"
            f"CUST-{random.randint(1,50000):06d},"
            f"PROD-{random.randint(1,5000):05d},"
            f"{qty},{price},{round(qty * price, 2)},"
            f"{random.choice(statuses)},{datetime.now().strftime('%Y-%m-%d')}"
        )

    csv_content = "\n".join(rows)

    print(f"📤 Uploading test file: s3://{BUCKET_NAME}/{s3_key}")
    print(f"   Rows: 100")
    print(f"   Size: {len(csv_content)} bytes")

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "source-system": "test-script",
            "entity-type": entity,
            "upload-date": datetime.now().strftime("%Y-%m-%d")
        }
    )

    print(f"   ✅ Upload complete")
    print(f"\n⏳ EventBridge should detect this within ~1-3 seconds...")
    print(f"   Step Functions should start a new execution automatically")

    return s3_key


def monitor_pipeline_execution():
    """
    Watch for new Step Functions executions triggered by the file upload.
    
    This is what you'd do in production:
    → Step Functions Console shows the visual execution graph
    → Each state turns green (success) or red (failure) in real-time
    → Click any state to see input/output JSON
    
    Here we poll the API for the same information.
    """
    sfn_client = boto3.client("stepfunctions", region_name=REGION)

    print(f"\n🔍 Monitoring Step Functions executions...")
    print(f"   State Machine: {STATE_MACHINE_ARN}")
    print("-" * 70)

    # Wait for execution to appear
    max_wait = 30  # seconds
    execution_arn = None

    for attempt in range(max_wait):
        time.sleep(1)

        response = sfn_client.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            statusFilter="RUNNING",
            maxResults=1
        )

        if response["executions"]:
            execution_arn = response["executions"][0]["executionArn"]
            exec_name = response["executions"][0]["name"]
            start_time = response["executions"][0]["startDate"]
            print(f"\n🚀 EXECUTION DETECTED!")
            print(f"   Name: {exec_name}")
            print(f"   ARN: {execution_arn}")
            print(f"   Started: {start_time}")
            break
        else:
            print(f"   Waiting... ({attempt + 1}s)", end="\r")

    if not execution_arn:
        # Check if execution already completed
        response = sfn_client.list_executions(
            stateMachineArn=STATE_MACHINE_ARN,
            maxResults=1
        )
        if response["executions"]:
            execution_arn = response["executions"][0]["executionArn"]
            print(f"\n📋 Found latest execution: {execution_arn}")
            print(f"   Status: {response['executions'][0]['status']}")
        else:
            print(f"\n❌ No execution found after {max_wait}s")
            print(f"   Possible causes:")
            print(f"   → EventBridge not enabled on bucket")
            print(f"   → Rule filter not matching")
            print(f"   → IAM role missing permissions")
            return

    # Monitor execution progress
    print(f"\n📊 Tracking execution state transitions:")
    print("-" * 70)

    seen_events = set()
    terminal_states = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}

    while True:
        # Get execution history
        history = sfn_client.get_execution_history(
            executionArn=execution_arn,
            maxResults=100,
            reverseOrder=False
        )

        for event in history["events"]:
            event_id = event["id"]
            if event_id not in seen_events:
                seen_events.add(event_id)
                event_type = event["type"]
                timestamp = event["timestamp"].strftime("%H:%M:%S")

                # Format output based on event type
                if "stateEnteredEventDetails" in event:
                    state_name = event["stateEnteredEventDetails"]["name"]
                    print(f"  [{timestamp}] ▶️  ENTERED: {state_name}")

                elif "stateExitedEventDetails" in event:
                    state_name = event["stateExitedEventDetails"]["name"]
                    print(f"  [{timestamp}] ✅ EXITED:  {state_name}")

                elif "taskStartedEventDetails" in event:
                    print(f"  [{timestamp}] ⚙️  TASK STARTED (API call in progress)")

                elif "taskSucceededEventDetails" in event:
                    print(f"  [{timestamp}] ✅ TASK SUCCEEDED")

                elif "taskFailedEventDetails" in event:
                    error = event["taskFailedEventDetails"].get("error", "Unknown")
                    cause = event["taskFailedEventDetails"].get("cause", "Unknown")
                    print(f"  [{timestamp}] ❌ TASK FAILED: {error}")
                    print(f"              Cause: {cause[:200]}")

                elif "executionSucceededEventDetails" in event:
                    print(f"  [{timestamp}] 🎉 EXECUTION SUCCEEDED")

                elif "executionFailedEventDetails" in event:
                    error = event["executionFailedEventDetails"].get("error", "Unknown")
                    cause = event["executionFailedEventDetails"].get("cause", "Unknown")
                    print(f"  [{timestamp}] 💥 EXECUTION FAILED: {error}")
                    print(f"              Cause: {cause[:300]}")

        # Check if execution is done
        describe = sfn_client.describe_execution(executionArn=execution_arn)
        status = describe["status"]

        if status in terminal_states:
            print("-" * 70)
            print(f"\n📋 FINAL STATUS: {status}")

            if status == "SUCCEEDED":
                output = json.loads(describe.get("output", "{}"))
                print(f"   Output: {json.dumps(output, indent=2)[:500]}")
            elif status == "FAILED":
                print(f"   Error: {describe.get('error', 'N/A')}")
                print(f"   Cause: {describe.get('cause', 'N/A')[:500]}")

            # Execution timing
            start = describe["startDate"]
            stop = describe.get("stopDate", datetime.now(start.tzinfo))
            duration = (stop - start).total_seconds()
            print(f"   Duration: {duration:.1f} seconds")
            break

        time.sleep(5)


def check_pipeline_output():
    """Verify that Parquet files were created in the curated zone"""
    s3_client = boto3.client("s3", region_name=REGION)

    curated_prefix = "curated/orders/"
    print(f"\n📦 Checking curated output: s3://{BUCKET_NAME}/{curated_prefix}")

    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=curated_prefix
    )

    if "Contents" in response:
        total_size = 0
        for obj in response["Contents"]:
            size_kb = round(obj["Size"] / 1024, 1)
            total_size += obj["Size"]
            print(f"   📄 {obj['Key']} ({size_kb} KB)")
        print(f"   Total: {len(response['Contents'])} files, "
              f"{round(total_size / 1024, 1)} KB")
    else:
        print(f"   (no files yet — Glue job may still be writing)")


if __name__ == "__main__":
    print("=" * 70)
    print("🧪 END-TO-END PIPELINE TEST")
    print("=" * 70)

    # Step 1: Upload file (triggers pipeline automatically)
    s3_key = generate_and_upload_test_file()

    # Step 2: Monitor execution
    monitor_pipeline_execution()

    # Step 3: Verify output
    check_pipeline_output()

    print("\n" + "=" * 70)
    print("🧪 TEST COMPLETE")
    print("=" * 70)