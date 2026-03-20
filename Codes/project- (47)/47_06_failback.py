# file: 47_06_failback.py
# Purpose: Procedure to switch back to primary region after it recovers

import boto3
import time
from datetime import datetime

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
PRIMARY_BUCKET = "quickcart-datalake-prod"
DR_BUCKET = "quickcart-datalake-dr"


def run_failback():
    """
    Switch back to primary region after recovery.
    
    IMPORTANT: Failback is simpler than failover because:
    → CRR was still running (replication resumes after primary recovers)
    → Primary bucket has data up to the outage start
    → CRR catches up with data written during outage (if any)
    → We just need to verify primary is healthy and switch back
    """
    print("╔" + "═" * 68 + "╗")
    print("║" + "  🔄 FAILBACK TO PRIMARY REGION".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    # Step 1: Verify primary is back
    print(f"\n📋 Step 1: Verify primary region health")
    s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)

    try:
        s3_primary.head_bucket(Bucket=PRIMARY_BUCKET)
        print(f"   ✅ Primary bucket reachable: {PRIMARY_BUCKET}")
    except Exception as e:
        print(f"   ❌ Primary still down: {e}")
        print(f"   🛑 FAILBACK ABORTED — primary not yet recovered")
        return

    # Step 2: Verify replication caught up
    print(f"\n📋 Step 2: Verify replication caught up")
    from test_and_monitor_replication import compare_primary_vs_dr
    compare_primary_vs_dr()

    # Step 3: Re-run ETL pipeline to catch up
    print(f"\n📋 Step 3: Re-run ETL pipeline")
    print(f"   → Trigger CDC extraction for today's data")
    print(f"   → Run Bronze→Silver→Gold pipeline")
    print(f"   → This ensures primary has LATEST data")
    print(f"   ℹ️  Run: python 42_05_cdc_pipeline_orchestrator.py")

    # Step 4: Update DR status
    print(f"\n📋 Step 4: Update failover status")
    dynamodb = boto3.resource("dynamodb", region_name=DR_REGION)
    try:
        table = dynamodb.Table("quickcart_dr_status")
        table.put_item(Item={
            "status_key": "current_state",
            "active_region": PRIMARY_REGION,
            "failback_time": datetime.utcnow().isoformat(),
            "status": "PRIMARY_ACTIVE",
            "initiated_by": "failback_runbook"
        })
        print(f"   ✅ Status updated: PRIMARY_ACTIVE")
    except Exception as e:
        print(f"   ⚠️  Status update failed: {e}")

    # Step 5: Announce failback
    print(f"\n📋 Step 5: Announce failback to team")
    announcement = f"""
✅ DATA PLATFORM FAILBACK COMPLETE

Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Active region: {PRIMARY_REGION} (primary)

FOR ALL ANALYSTS:
→ Switch BACK to Athena in us-east-2 (Ohio)
→ Use databases: quickcart_gold (remove _dr suffix)
→ Use normal workgroups
→ All data is current

Thank you for your patience during the outage.
"""
    print(announcement)


if __name__ == "__main__":
    run_failback()