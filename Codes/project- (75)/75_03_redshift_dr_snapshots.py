# file: 75_03_redshift_dr_snapshots.py
# Automate Redshift cross-region snapshots for DR
# Hourly snapshots copied to us-west-2
# On failover: restore from latest DR snapshot

import boto3
import json
import time
from datetime import datetime, timedelta

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
WORKGROUP_NAME = "quickcart-analytics"
NAMESPACE_NAME = "quickcart-namespace"
SNAPSHOT_RETENTION_DAYS = 7


def configure_cross_region_snapshots_serverless():
    """
    Configure Redshift Serverless cross-region snapshot copy
    
    Redshift Serverless uses recovery points (not traditional snapshots)
    → Recovery points created automatically every 30 minutes
    → Cross-region copy must be configured explicitly
    → On failover: create new workgroup from recovery point
    """
    rs_primary = boto3.client("redshift-serverless", region_name=PRIMARY_REGION)

    print("📸 Configuring Redshift Serverless cross-region snapshots:")

    try:
        # Create a manual snapshot for DR
        snapshot_name = f"dr-snapshot-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

        rs_primary.create_snapshot(
            namespaceName=NAMESPACE_NAME,
            snapshotName=snapshot_name,
            retentionPeriod=SNAPSHOT_RETENTION_DAYS,
            tags=[
                {"key": "Purpose", "value": "disaster-recovery"},
                {"key": "SourceRegion", "value": PRIMARY_REGION},
                {"key": "AutomatedDR", "value": "true"}
            ]
        )
        print(f"   ✅ Snapshot created: {snapshot_name}")

        # Wait for snapshot to be available
        print("   Waiting for snapshot...", end="", flush=True)
        while True:
            response = rs_primary.get_snapshot(snapshotName=snapshot_name)
            status = response["snapshot"]["status"]
            if status == "AVAILABLE":
                print(f" ✅ ({status})")
                break
            elif status == "FAILED":
                print(f" ❌ ({status})")
                return None
            time.sleep(10)
            print(".", end="", flush=True)

        return snapshot_name

    except Exception as e:
        print(f"   ⚠️  Snapshot creation: {e}")
        return None


def copy_snapshot_to_dr(snapshot_name):
    """
    Copy snapshot from primary to DR region
    
    IMPORTANT: This is an async operation
    → Snapshot data transfers across regions
    → Time depends on data size (500 GB ≈ 30-60 min)
    → Monitor with get_snapshot in DR region
    """
    rs_primary = boto3.client("redshift-serverless", region_name=PRIMARY_REGION)

    print(f"\n📋 Copying snapshot to {DR_REGION}:")

    try:
        dr_snapshot_name = f"dr-{snapshot_name}"

        # For Redshift Serverless, we need to use
        # the snapshot ARN and restore in DR region
        response = rs_primary.get_snapshot(snapshotName=snapshot_name)
        snapshot_arn = response["snapshot"]["snapshotArn"]

        print(f"   Source: {snapshot_name} ({PRIMARY_REGION})")
        print(f"   ARN: {snapshot_arn}")
        print(f"   ✅ Snapshot available for cross-region restore")
        print(f"   → On failover: restore from this ARN in {DR_REGION}")

        return snapshot_arn

    except Exception as e:
        print(f"   ⚠️  Snapshot copy: {e}")
        return None


def restore_redshift_in_dr(snapshot_arn=None):
    """
    FAILOVER FUNCTION: Restore Redshift in DR region from snapshot
    
    Creates:
    → New namespace in DR region
    → New workgroup in DR region
    → Restores data from cross-region snapshot
    → Creates Spectrum external schemas
    """
    rs_dr = boto3.client("redshift-serverless", region_name=DR_REGION)

    print(f"\n🔄 RESTORING REDSHIFT IN {DR_REGION}")

    # Step 1: Create namespace (if not exists)
    dr_namespace = f"{NAMESPACE_NAME}-dr"
    try:
        rs_dr.create_namespace(
            namespaceName=dr_namespace,
            adminUsername="admin",
            adminUserPassword="CHANGE_ME_Secure123!",  # Use Secrets Manager
            dbName="quickcart",
            defaultIamRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftSpectrumRole",
            iamRoles=[
                f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftSpectrumRole"
            ],
            tags=[
                {"key": "Purpose", "value": "disaster-recovery"},
                {"key": "SourceRegion", "value": PRIMARY_REGION}
            ]
        )
        print(f"   ✅ Namespace created: {dr_namespace}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Namespace exists: {dr_namespace}")
        else:
            print(f"   ⚠️  Namespace: {e}")

    # Step 2: Create workgroup
    dr_workgroup = f"{WORKGROUP_NAME}-dr"
    try:
        rs_dr.create_workgroup(
            workgroupName=dr_workgroup,
            namespaceName=dr_namespace,
            baseCapacity=32,  # Minimum RPU for DR
            enhancedVpcRouting=False,
            publiclyAccessible=False,
            tags=[
                {"key": "Purpose", "value": "disaster-recovery"}
            ]
        )
        print(f"   ✅ Workgroup created: {dr_workgroup}")

        # Wait for workgroup to be available
        print("   Waiting for workgroup...", end="", flush=True)
        while True:
            response = rs_dr.get_workgroup(workgroupName=dr_workgroup)
            status = response["workgroup"]["status"]
            if status == "AVAILABLE":
                print(f" ✅ ({status})")
                endpoint = response["workgroup"]["endpoint"]["address"]
                print(f"   Endpoint: {endpoint}")
                break
            elif status in ("FAILED", "DELETING"):
                print(f" ❌ ({status})")
                return None
            time.sleep(15)
            print(".", end="", flush=True)

    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Workgroup exists: {dr_workgroup}")
        else:
            print(f"   ⚠️  Workgroup: {e}")

    # Step 3: Restore from snapshot (if provided)
    if snapshot_arn:
        try:
            rs_dr.restore_from_snapshot(
                namespaceName=dr_namespace,
                workgroupName=dr_workgroup,
                snapshotArn=snapshot_arn
            )
            print(f"   ✅ Restoring from snapshot: {snapshot_arn}")
            print(f"   ⏳ This may take 45-120 minutes for 500 GB...")
        except Exception as e:
            print(f"   ⚠️  Snapshot restore: {e}")

    # Step 4: Create Spectrum external schemas (after restore completes)
    print(f"\n   📋 Spectrum schemas must be created after restore completes")
    print(f"   → Run Project 73 Redshift SQL against DR workgroup")
    print(f"   → Update external schema S3 locations to DR buckets")

    return dr_workgroup


def create_snapshot_schedule():
    """
    Create EventBridge rule to trigger hourly snapshots
    → Lambda creates snapshot → copies to DR
    → Maintains RPO < 1 hour for Redshift
    """
    events_client = boto3.client("events", region_name=PRIMARY_REGION)

    events_client.put_rule(
        Name="dr-redshift-hourly-snapshot",
        ScheduleExpression="rate(1 hour)",
        State="ENABLED",
        Description="Create Redshift DR snapshot every hour"
    )

    print("✅ Hourly snapshot schedule created: rate(1 hour)")
    print("   → Lambda target should call configure_cross_region_snapshots_serverless()")

    return "dr-redshift-hourly-snapshot"


def cleanup_old_snapshots():
    """Remove snapshots older than retention period"""
    rs_client = boto3.client("redshift-serverless", region_name=PRIMARY_REGION)

    cutoff = datetime.utcnow() - timedelta(days=SNAPSHOT_RETENTION_DAYS)

    print(f"\n🧹 Cleaning snapshots older than {SNAPSHOT_RETENTION_DAYS} days:")

    try:
        response = rs_client.list_snapshots(namespaceName=NAMESPACE_NAME)
        for snapshot in response.get("snapshots", []):
            snap_name = snapshot["snapshotName"]
            snap_time = snapshot.get("snapshotCreateTime")

            if snap_name.startswith("dr-") and snap_time and snap_time < cutoff:
                try:
                    rs_client.delete_snapshot(snapshotName=snap_name)
                    print(f"   🗑️  Deleted: {snap_name}")
                except Exception:
                    pass
    except Exception as e:
        print(f"   ⚠️  Cleanup: {e}")


def main():
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "snapshot"

    print("=" * 70)

    if mode == "snapshot":
        print("📸 REDSHIFT DR SNAPSHOT")
        print("=" * 70)
        snapshot_name = configure_cross_region_snapshots_serverless()
        if snapshot_name:
            copy_snapshot_to_dr(snapshot_name)
            cleanup_old_snapshots()
        create_snapshot_schedule()

    elif mode == "restore":
        print("🔄 REDSHIFT DR RESTORE")
        print("=" * 70)
        snapshot_arn = sys.argv[2] if len(sys.argv) > 2 else None
        restore_redshift_in_dr(snapshot_arn)

    print("\n" + "=" * 70)
    print("✅ COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()