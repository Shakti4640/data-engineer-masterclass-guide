# file: 42_05_cdc_pipeline_orchestrator.py
# Purpose: Run the complete CDC pipeline: Extract → Upload → Merge → Verify
# This is what Step Functions (Project 41) would orchestrate

import boto3
import time
from datetime import datetime

# Import our CDC modules
from cdc_extract_mariadb_to_s3 import run_cdc_extraction, CDC_ENTITIES
from redshift_cdc_merge import run_full_cdc_merge

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{boto3.client('sts').get_caller_identity()['Account']}:quickcart-pipeline-alerts"


def verify_data_consistency(entity_name, mariadb_config, redshift_config):
    """
    Post-merge validation: compare row counts between source and target.
    
    WHY:
    → Catch silent data loss (missed rows)
    → Catch duplication bugs (extra rows)
    → Provides confidence score for the pipeline
    
    TOLERANCE:
    → Allow up to 0.1% difference (timing: new rows during extraction)
    → Flag anything above 0.1% for investigation
    """
    import pymysql
    import psycopg2

    # MariaDB count
    maria_conn = pymysql.connect(**mariadb_config)
    maria_cursor = maria_conn.cursor()
    maria_cursor.execute(f"SELECT COUNT(*) FROM {entity_name}")
    maria_count = maria_cursor.fetchone()[0]
    maria_cursor.close()
    maria_conn.close()

    # Redshift count
    rs_conn = psycopg2.connect(**redshift_config)
    rs_cursor = rs_conn.cursor()
    rs_cursor.execute(f"SELECT COUNT(*) FROM public.{entity_name}")
    rs_count = rs_cursor.fetchone()[0]
    rs_cursor.close()
    rs_conn.close()

    # Compare
    difference = abs(maria_count - rs_count)
    pct_diff = (difference / max(maria_count, 1)) * 100

    print(f"\n   🔍 Data Consistency Check: {entity_name}")
    print(f"      MariaDB:  {maria_count:,} rows")
    print(f"      Redshift: {rs_count:,} rows")
    print(f"      Diff:     {difference:,} rows ({pct_diff:.3f}%)")

    if pct_diff <= 0.1:
        print(f"      ✅ CONSISTENT (within 0.1% tolerance)")
        return True
    else:
        print(f"      ⚠️  INCONSISTENT — investigate!")
        return False


def send_pipeline_notification(sns_client, status, details):
    """Send success/failure notification via SNS (Project 12, 41)"""
    subject = f"{'✅' if status == 'SUCCESS' else '❌'} QuickCart CDC Pipeline — {status}"

    message_lines = [
        f"CDC Pipeline Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Status: {status}",
        "",
        "Details:",
        "-" * 40
    ]

    for entity, result in details.items():
        if result.get("status") == "SUCCESS":
            message_lines.append(
                f"  {entity}: {result.get('new_rows', 0)} new + "
                f"{result.get('updated_rows', 0)} updated "
                f"({result.get('merge_duration_seconds', 0):.1f}s)"
            )
        elif result.get("status") == "SKIPPED":
            message_lines.append(f"  {entity}: SKIPPED — {result.get('reason', 'N/A')}")
        else:
            message_lines.append(f"  {entity}: FAILED — {result.get('error', 'N/A')}")

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],  # SNS subject limit
            Message="\n".join(message_lines)
        )
        print(f"\n📧 Notification sent: {subject}")
    except Exception as e:
        print(f"\n⚠️  Failed to send notification: {e}")


def run_cdc_pipeline():
    """
    COMPLETE CDC PIPELINE ORCHESTRATOR
    
    This is the "brain" that Step Functions (Project 41) would call.
    In production, each step would be a Step Functions state.
    Here, we run them sequentially in Python for clarity.
    
    FLOW:
    ┌─────────────────────────────────────────┐
    │ 1. EXTRACT: MariaDB → S3 (CDC rows)    │
    │ 2. MERGE: S3 → Redshift (staging+merge)│
    │ 3. VERIFY: Count comparison             │
    │ 4. NOTIFY: SNS success/failure          │
    └─────────────────────────────────────────┘
    """
    pipeline_start = time.time()
    sns_client = boto3.client("sns", region_name=REGION)

    print("╔" + "═" * 68 + "╗")
    print("║" + "  QUICKCART CDC PIPELINE — FULL RUN".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    overall_status = "SUCCESS"

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PHASE 1: CDC EXTRACTION (MariaDB → S3)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'='*70}")
    print(f"📤 PHASE 1: CDC EXTRACTION")
    print(f"{'='*70}")

    try:
        extraction_results = run_cdc_extraction()
    except Exception as e:
        print(f"❌ Extraction phase FAILED: {e}")
        send_pipeline_notification(sns_client, "FAILED", {"extraction": {"error": str(e)}})
        return 1

    # Check if any extraction failed
    for entity, result in extraction_results.items():
        if result["status"] == "FAILED":
            overall_status = "PARTIAL_FAILURE"

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PHASE 2: CDC MERGE (S3 → Redshift)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'='*70}")
    print(f"🔀 PHASE 2: REDSHIFT MERGE")
    print(f"{'='*70}")

    try:
        merge_results = run_full_cdc_merge(extraction_results)
    except Exception as e:
        print(f"❌ Merge phase FAILED: {e}")
        send_pipeline_notification(sns_client, "FAILED", {"merge": {"error": str(e)}})
        return 1

    for entity, result in merge_results.items():
        if result.get("status") == "FAILED":
            overall_status = "PARTIAL_FAILURE"

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PHASE 3: DATA CONSISTENCY VERIFICATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n{'='*70}")
    print(f"🔍 PHASE 3: CONSISTENCY VERIFICATION")
    print(f"{'='*70}")

    from cdc_extract_mariadb_to_s3 import MARIADB_CONFIG
    from redshift_cdc_merge import REDSHIFT_CONFIG

    for entity_name in CDC_ENTITIES.keys():
        if merge_results.get(entity_name, {}).get("status") == "SUCCESS":
            is_consistent = verify_data_consistency(
                entity_name, MARIADB_CONFIG, REDSHIFT_CONFIG
            )
            if not is_consistent:
                overall_status = "WARNING"

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PHASE 4: NOTIFICATION
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    pipeline_duration = time.time() - pipeline_start

    print(f"\n{'='*70}")
    print(f"📧 PHASE 4: NOTIFICATION")
    print(f"{'='*70}")

    send_pipeline_notification(sns_client, overall_status, merge_results)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # FINAL SUMMARY
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    print(f"\n╔{'═'*68}╗")
    print(f"║{'  PIPELINE COMPLETE'.center(68)}║")
    print(f"║{''.center(68)}║")
    print(f"║{f'  Status: {overall_status}'.center(68)}║")
    print(f"║{f'  Duration: {pipeline_duration:.1f} seconds'.center(68)}║")
    print(f"╚{'═'*68}╝")

    return 0 if overall_status == "SUCCESS" else 1


if __name__ == "__main__":
    exit_code = run_cdc_pipeline()
    exit(exit_code)