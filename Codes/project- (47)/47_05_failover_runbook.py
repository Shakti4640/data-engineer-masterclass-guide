# file: 47_05_failover_runbook.py
# Purpose: Automated failover procedure — executed when primary region fails
# This is the RUNBOOK — step-by-step with verification at each stage

import boto3
import time
from datetime import datetime

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
PRIMARY_BUCKET = "quickcart-datalake-prod"
DR_BUCKET = "quickcart-datalake-dr"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
SNS_TOPIC_ARN = f"arn:aws:sns:{PRIMARY_REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"


def step_0_confirm_outage():
    """
    STEP 0: Verify that primary region is actually down.
    Prevent false-positive failovers.
    """
    print(f"\n{'━'*60}")
    print(f"📋 STEP 0: CONFIRM PRIMARY OUTAGE")
    print(f"{'━'*60}")

    s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)

    # Try 3 times with 10-second intervals
    failures = 0
    for attempt in range(3):
        try:
            s3_primary.head_bucket(Bucket=PRIMARY_BUCKET)
            print(f"   Attempt {attempt+1}: Primary S3 REACHABLE ✅")
            time.sleep(10)
        except Exception as e:
            print(f"   Attempt {attempt+1}: Primary S3 UNREACHABLE ❌ ({str(e)[:80]})")
            failures += 1
            time.sleep(10)

    if failures < 2:
        print(f"\n   ⚠️  Primary region appears HEALTHY ({3-failures}/3 checks passed)")
        print(f"   ❌ FAILOVER NOT RECOMMENDED — may be transient")
        confirm = input(f"   Force failover anyway? (yes/no): ")
        if confirm.lower() != "yes":
            print(f"   🛑 FAILOVER ABORTED")
            return False
    else:
        print(f"\n   ❌ Primary region CONFIRMED DOWN ({failures}/3 checks failed)")

    return True


def step_1_announce_failover():
    """STEP 1: Announce failover to team"""
    print(f"\n{'━'*60}")
    print(f"📋 STEP 1: ANNOUNCE FAILOVER")
    print(f"{'━'*60}")

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    announcement = f"""
🚨 DATA PLATFORM FAILOVER INITIATED

Time: {timestamp}
Reason: Primary region (us-east-2) unavailable
Action: Switching to DR region (us-west-2)

FOR ALL ANALYSTS:
→ Switch to Athena console in us-west-2 (Oregon)
→ Use database: quickcart_gold_dr (instead of quickcart_gold)
→ Use workgroup: dr-dashboards or dr-adhoc
→ Data may be up to 15 minutes behind (replication lag)

FOR DASHBOARD OWNERS:
→ Update dashboard data source to us-west-2 Athena endpoint
→ Database names have _dr suffix

We will announce when primary is restored and failback begins.
"""

    print(announcement)

    # Try to send SNS (may fail if primary region is down)
    try:
        # Try DR region SNS
        sns_dr = boto3.client("sns", region_name=DR_REGION)

        # Create DR SNS topic if needed
        topic_response = sns_dr.create_topic(Name="quickcart-pipeline-alerts-dr")
        dr_topic_arn = topic_response["TopicArn"]

        sns_dr.publish(
            TopicArn=dr_topic_arn,
            Subject="🚨 DATA PLATFORM FAILOVER — Switching to DR",
            Message=announcement
        )
        print(f"   ✅ Announcement sent via DR region SNS")
    except Exception as e:
        print(f"   ⚠️  SNS notification failed: {e}")
        print(f"   → Announce manually via Slack/email")

    return True


def step_2_verify_dr_data():
    """STEP 2: Verify DR bucket has recent data"""
    print(f"\n{'━'*60}")
    print(f"📋 STEP 2: VERIFY DR DATA FRESHNESS")
    print(f"{'━'*60}")

    s3_dr = boto3.client("s3", region_name=DR_REGION)

    zones_ok = True

    for zone in ["silver", "gold"]:
        # Find latest object in zone
        paginator = s3_dr.get_paginator("list_objects_v2")
        latest_time = None
        latest_key = None

        for page in paginator.paginate(Bucket=DR_BUCKET, Prefix=f"{zone}/", MaxKeys=100):
            for obj in page.get("Contents", []):
                if obj["Size"] > 0:  # Skip marker objects
                    if latest_time is None or obj["LastModified"] > latest_time:
                        latest_time = obj["LastModified"]
                        latest_key = obj["Key"]

        if latest_time:
            age_hours = (datetime.now(latest_time.tzinfo) - latest_time).total_seconds() / 3600
            status = "✅" if age_hours < 1 else "⚠️" if age_hours < 24 else "❌"

            print(f"   {status} {zone}/:")
            print(f"      Latest object: {latest_key}")
            print(f"      Last modified: {latest_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"      Age: {age_hours:.1f} hours")

            if age_hours > 24:
                zones_ok = False
                print(f"      ❌ WARNING: Data is >24 hours old — replication may have been failing")
        else:
            print(f"   ❌ {zone}/: NO DATA in DR bucket!")
            zones_ok = False

    if not zones_ok:
        print(f"\n   ⚠️  DR data freshness issues detected")
        print(f"   → Analysts should be warned about potential data gaps")
    else:
        print(f"\n   ✅ DR data appears fresh")

    return zones_ok


def step_3_test_dr_queries():
    """STEP 3: Run smoke test queries in DR Athena"""
    print(f"\n{'━'*60}")
    print(f"📋 STEP 3: SMOKE TEST DR QUERIES")
    print(f"{'━'*60}")

    athena_client = boto3.client("athena", region_name=DR_REGION)

    smoke_tests = [
        ("Gold customer count",
         "SELECT COUNT(*) FROM quickcart_gold_dr.customer_360 WHERE year = '2025'"),
        ("Gold revenue check",
         "SELECT COUNT(*) FROM quickcart_gold_dr.revenue_daily WHERE year = '2025'"),
        ("Silver orders check",
         "SELECT COUNT(*) FROM quickcart_silver_dr.orders WHERE year = '2025'")
    ]

    all_passed = True

    for test_name, sql in smoke_tests:
        try:
            response = athena_client.start_query_execution(
                QueryString=sql,
                WorkGroup="dr-adhoc",
                ResultConfiguration={
                    "OutputLocation": f"s3://{DR_BUCKET}/_system/athena-results/failover-test/"
                }
            )
            execution_id = response["QueryExecutionId"]

            for _ in range(30):
                status = athena_client.get_query_execution(QueryExecutionId=execution_id)
                state = status["QueryExecution"]["Status"]["State"]
                if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
                time.sleep(2)

            if state == "SUCCEEDED":
                results = athena_client.get_query_results(
                    QueryExecutionId=execution_id, MaxResults=2
                )
                value = results["ResultSet"]["Rows"][1]["Data"][0].get("VarCharValue", "0")
                print(f"   ✅ {test_name}: {value} rows")
            else:
                error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
                print(f"   ❌ {test_name}: FAILED — {error[:100]}")
                all_passed = False

        except Exception as e:
            print(f"   ❌ {test_name}: ERROR — {e}")
            all_passed = False

    return all_passed


def step_4_activate_dr():
    """STEP 4: Mark DR as active (update status tracking)"""
    print(f"\n{'━'*60}")
    print(f"📋 STEP 4: ACTIVATE DR REGION")
    print(f"{'━'*60}")

    # Store failover status in DynamoDB (DR region)
    dynamodb = boto3.resource("dynamodb", region_name=DR_REGION)

    try:
        table = dynamodb.create_table(
            TableName="quickcart_dr_status",
            KeySchema=[{"AttributeName": "status_key", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "status_key", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST"
        )
        table.wait_until_exists()
    except Exception:
        table = dynamodb.Table("quickcart_dr_status")

    table.put_item(Item={
        "status_key": "current_state",
        "active_region": DR_REGION,
        "failover_time": datetime.utcnow().isoformat(),
        "primary_region": PRIMARY_REGION,
        "status": "DR_ACTIVE",
        "initiated_by": "failover_runbook"
    })

    print(f"   ✅ DR status: ACTIVE")
    print(f"   Active region: {DR_REGION}")
    print(f"   Failover time: {datetime.utcnow().isoformat()}")

    return True


def step_5_provide_instructions():
    """STEP 5: Print analyst instructions"""
    print(f"\n{'━'*60}")
    print(f"📋 STEP 5: ANALYST INSTRUCTIONS")
    print(f"{'━'*60}")

    instructions = f"""
╔════════════════════════════════════════════════════════════════════╗
║                    DR FAILOVER ACTIVE                              ║
╠════════════════════════════════════════════════════════════════════╣
║                                                                    ║
║  ATHENA CONSOLE:                                                   ║
║  → Switch to region: us-west-2 (Oregon)                           ║
║  → URL: https://us-west-2.console.aws.amazon.com/athena/          ║
║                                                                    ║
║  DATABASES:                                                        ║
║  → Gold zone:   quickcart_gold_dr   (instead of quickcart_gold)   ║
║  → Silver zone: quickcart_silver_dr (instead of quickcart_silver) ║
║                                                                    ║
║  WORKGROUPS:                                                       ║
║  → Dashboards: dr-dashboards                                      ║
║  → Ad-hoc:     dr-adhoc                                           ║
║                                                                    ║
║  EXAMPLE QUERY:                                                    ║
║  SELECT segment, COUNT(*), SUM(lifetime_value)                    ║
║  FROM quickcart_gold_dr.customer_360                              ║
║  WHERE year = '2025' AND month = '01'                             ║
║  GROUP BY segment                                                  ║
║                                                                    ║
║  DATA FRESHNESS:                                                   ║
║  → Data may be up to 15 minutes behind primary                    ║
║  → Check computed_at column for exact freshness                    ║
║                                                                    ║
║  NOT AVAILABLE IN DR:                                              ║
║  → Federated queries (MariaDB in us-east-2)                       ║
║  → Bronze zone data (not replicated)                               ║
║  → Redshift (us-east-2 only)                                      ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
"""
    print(instructions)


def run_failover():
    """Execute the complete failover runbook"""
    start_time = time.time()

    print("╔" + "═" * 68 + "╗")
    print("║" + "  🚨 DATA PLATFORM FAILOVER RUNBOOK".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    # Step 0: Confirm outage
    if not step_0_confirm_outage():
        return

    # Step 1: Announce
    step_1_announce_failover()

    # Step 2: Verify DR data
    data_ok = step_2_verify_dr_data()

    # Step 3: Smoke test
    queries_ok = step_3_test_dr_queries()

    # Step 4: Activate
    step_4_activate_dr()

    # Step 5: Instructions
    step_5_provide_instructions()

    # Summary
    duration = time.time() - start_time

    print(f"╔{'═'*68}╗")
    print(f"║{'  FAILOVER COMPLETE'.center(68)}║")
    print(f"║{f'  RTO: {duration:.0f} seconds ({duration/60:.1f} minutes)'.center(68)}║")
    print(f"║{f'  Target RTO: <900 seconds (15 minutes)'.center(68)}║")
    status = "✅ WITHIN TARGET" if duration < 900 else "⚠️ EXCEEDED TARGET"
    print(f"║{f'  Status: {status}'.center(68)}║")
    print(f"║{f'  Data freshness: {\"OK\" if data_ok else \"ISSUES\"}'.center(68)}║")
    print(f"║{f'  Query tests: {\"PASSED\" if queries_ok else \"SOME FAILED\"}'.center(68)}║")
    print(f"╚{'═'*68}╝")


if __name__ == "__main__":
    run_failover()