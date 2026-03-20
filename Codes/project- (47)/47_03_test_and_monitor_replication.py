# file: 47_03_test_and_monitor_replication.py
# Purpose: Test that replication works, monitor replication health

import boto3
import time
import json
from datetime import datetime

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
PRIMARY_BUCKET = "quickcart-datalake-prod"
DR_BUCKET = "quickcart-datalake-dr"


def test_replication():
    """
    Write a test object to primary and verify it appears in DR.
    Measures actual replication latency.
    """
    s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)
    s3_dr = boto3.client("s3", region_name=DR_REGION)

    test_key = f"gold/_dr_test/replication_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    test_content = f"DR replication test — {datetime.now().isoformat()}"

    print(f"🧪 REPLICATION TEST")
    print(f"{'─'*60}")

    # Write to primary
    print(f"   📤 Writing test object to primary...")
    write_time = time.time()

    s3_primary.put_object(
        Bucket=PRIMARY_BUCKET,
        Key=test_key,
        Body=test_content.encode("utf-8"),
        Metadata={"test-type": "dr-replication-test"}
    )
    print(f"   ✅ Written: s3://{PRIMARY_BUCKET}/{test_key}")

    # Check replication status on source
    time.sleep(2)
    head = s3_primary.head_object(Bucket=PRIMARY_BUCKET, Key=test_key)
    repl_status = head.get("ReplicationStatus", "UNKNOWN")
    print(f"   📋 Source replication status: {repl_status}")

    # Poll DR bucket for the object
    print(f"   ⏳ Waiting for replication to DR...")
    max_wait = 300  # 5 minutes
    check_interval = 5  # seconds

    for elapsed in range(0, max_wait, check_interval):
        try:
            dr_head = s3_dr.head_object(Bucket=DR_BUCKET, Key=test_key)
            replication_time = time.time() - write_time

            print(f"\n   ✅ REPLICATED!")
            print(f"   ⏱  Replication latency: {replication_time:.1f} seconds")
            print(f"   📋 DR object size: {dr_head['ContentLength']} bytes")
            print(f"   📋 DR replication status: {dr_head.get('ReplicationStatus', 'N/A')}")

            # Verify content matches
            dr_obj = s3_dr.get_object(Bucket=DR_BUCKET, Key=test_key)
            dr_content = dr_obj["Body"].read().decode("utf-8")

            if dr_content == test_content:
                print(f"   ✅ Content verification: MATCH")
            else:
                print(f"   ❌ Content verification: MISMATCH!")
                print(f"      Source: {test_content}")
                print(f"      DR: {dr_content}")

            # Check source status updated
            head_after = s3_primary.head_object(Bucket=PRIMARY_BUCKET, Key=test_key)
            print(f"   📋 Source replication status (after): {head_after.get('ReplicationStatus', 'N/A')}")

            return replication_time

        except s3_dr.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"   ⏳ Not yet replicated... ({elapsed}s)", end="\r")
                time.sleep(check_interval)
            else:
                raise

    print(f"\n   ❌ Replication NOT completed within {max_wait}s")
    print(f"   Check: IAM role permissions, bucket versioning, replication rules")
    return None


def monitor_replication_health():
    """
    Check CloudWatch metrics for replication health.
    
    Metrics available with RTC enabled:
    → ReplicationLatency
    → BytesPendingReplication
    → OperationsPendingReplication
    → OperationsFailedReplication
    """
    cw_client = boto3.client("cloudwatch", region_name=PRIMARY_REGION)

    print(f"\n{'='*60}")
    print(f"📊 REPLICATION HEALTH METRICS")
    print(f"{'='*60}")

    metrics = [
        {
            "name": "ReplicationLatency",
            "stat": "Maximum",
            "unit": "Seconds",
            "threshold": 900,
            "description": "Max seconds for replication (target: <900)"
        },
        {
            "name": "BytesPendingReplication",
            "stat": "Maximum",
            "unit": "Bytes",
            "threshold": 1073741824,  # 1 GB
            "description": "Bytes waiting to replicate (target: <1GB)"
        },
        {
            "name": "OperationsPendingReplication",
            "stat": "Maximum",
            "unit": "Count",
            "threshold": 1000,
            "description": "Objects waiting to replicate (target: <1000)"
        },
        {
            "name": "OperationsFailedReplication",
            "stat": "Sum",
            "unit": "Count",
            "threshold": 0,
            "description": "Failed replications (target: 0)"
        }
    ]

    end_time = datetime.utcnow()
    start_time = datetime.utcnow().replace(hour=0, minute=0, second=0)

    for metric in metrics:
        try:
            response = cw_client.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName=metric["name"],
                Dimensions=[
                    {"Name": "SourceBucket", "Value": PRIMARY_BUCKET},
                    {"Name": "DestinationBucket", "Value": DR_BUCKET},
                    {"Name": "RuleId", "Value": "replicate-gold-with-rtc"}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=[metric["stat"]]
            )

            datapoints = response.get("Datapoints", [])
            if datapoints:
                latest = sorted(datapoints, key=lambda x: x["Timestamp"])[-1]
                value = latest[metric["stat"]]
                status = "✅" if value <= metric["threshold"] else "❌"

                if metric["unit"] == "Bytes":
                    display_value = f"{value / (1024*1024):.2f} MB"
                elif metric["unit"] == "Seconds":
                    display_value = f"{value:.0f}s"
                else:
                    display_value = f"{value:.0f}"

                print(f"\n   {status} {metric['name']}: {display_value}")
                print(f"      {metric['description']}")
            else:
                print(f"\n   ℹ️  {metric['name']}: No data points")
                print(f"      {metric['description']}")
                print(f"      (Metrics may take ~15 minutes to appear after enabling RTC)")

        except Exception as e:
            print(f"\n   ⚠️  {metric['name']}: Error — {e}")


def compare_primary_vs_dr():
    """Compare object counts and sizes between primary and DR"""
    s3_primary = boto3.client("s3", region_name=PRIMARY_REGION)
    s3_dr = boto3.client("s3", region_name=DR_REGION)

    print(f"\n{'='*60}")
    print(f"📊 PRIMARY vs DR COMPARISON")
    print(f"{'='*60}")

    for zone in ["silver", "gold"]:
        # Count in primary
        primary_count, primary_size = count_objects(s3_primary, PRIMARY_BUCKET, f"{zone}/")
        dr_count, dr_size = count_objects(s3_dr, DR_BUCKET, f"{zone}/")

        match = "✅" if primary_count == dr_count else "⚠️"
        primary_mb = primary_size / (1024 * 1024)
        dr_mb = dr_size / (1024 * 1024)

        print(f"\n   {match} {zone}/:")
        print(f"      Primary: {primary_count:,} objects, {primary_mb:.2f} MB")
        print(f"      DR:      {dr_count:,} objects, {dr_mb:.2f} MB")

        if primary_count != dr_count:
            diff = primary_count - dr_count
            print(f"      ⚠️  Difference: {diff} objects")
            if diff > 0:
                print(f"         → {diff} objects pending replication")
            else:
                print(f"         → DR has {abs(diff)} extra objects (old versions?)")


def count_objects(s3_client, bucket, prefix):
    """Count objects and total size under a prefix"""
    paginator = s3_client.get_paginator("list_objects_v2")
    total_count = 0
    total_size = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_count += 1
            total_size += obj["Size"]

    return total_count, total_size


if __name__ == "__main__":
    repl_time = test_replication()
    monitor_replication_health()
    compare_primary_vs_dr()

    print(f"\n{'='*60}")
    if repl_time and repl_time < 900:
        print(f"✅ REPLICATION HEALTH: GOOD")
        print(f"   Replication latency: {repl_time:.1f}s (target: <900s)")
    else:
        print(f"⚠️  REPLICATION HEALTH: NEEDS ATTENTION")
    print(f"{'='*60}")