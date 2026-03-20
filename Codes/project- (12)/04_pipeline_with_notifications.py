# file: 04_pipeline_with_notifications.py
# Purpose: Show how to wrap ANY pipeline with success/failure notifications
# This pattern is used in EVERY subsequent project (21, 32, 55, 65, etc.)

import time
import random
import traceback
from datetime import datetime

# Import our notifier (from Step 3)
from publish_notification import PipelineNotifier


def simulate_etl_pipeline():
    """
    Simulate a pipeline that might succeed or fail
    Replace this with actual ETL logic in real projects
    """
    print("   📦 Step 1: Extract from MariaDB...")
    time.sleep(1)
    rows_extracted = random.randint(10000, 50000)
    print(f"      → {rows_extracted:,} rows extracted")
    
    print("   📤 Step 2: Upload to S3...")
    time.sleep(1)
    
    # Simulate random failure (30% chance)
    if random.random() < 0.3:
        raise ConnectionError("S3 upload failed: network timeout after 30 seconds")
    
    print("      → Uploaded to s3://quickcart-raw-data-prod/staging/")
    
    print("   📥 Step 3: COPY into Redshift staging...")
    time.sleep(1)
    
    # Simulate another failure (20% chance)
    if random.random() < 0.2:
        raise PermissionError("Redshift COPY failed: IAM role lacks s3:GetObject")
    
    print(f"      → {rows_extracted:,} rows loaded to staging")
    
    print("   🔄 Step 4: Merge to analytics tables...")
    time.sleep(1)
    rows_merged = rows_extracted - random.randint(0, 100)
    print(f"      → {rows_merged:,} rows merged")
    
    return {
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_extracted,
        "rows_merged": rows_merged,
        "tables": ["fact_orders", "dim_customers"]
    }


def run_pipeline_with_notifications():
    """
    THE PATTERN: wrap pipeline in try/except with notifications
    
    This EXACT pattern is used in:
    → Project 21 (S3 → Redshift COPY)
    → Project 32 (Glue → Redshift)
    → Project 55 (Step Functions orchestration)
    → Project 65 (Full CDC pipeline)
    → Every production pipeline
    """
    pipeline_name = "demo_etl_pipeline"
    notifier = PipelineNotifier()
    
    print("=" * 60)
    print(f"🚀 PIPELINE: {pipeline_name}")
    print(f"   Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # ══════════════════════════════════════
        # RUN THE ACTUAL PIPELINE
        # ══════════════════════════════════════
        result = simulate_etl_pipeline()
        
        # ══════════════════════════════════════
        # SUCCESS — notify data team
        # ══════════════════════════════════════
        duration = time.time() - start_time
        
        notifier.notify_success(
            pipeline_name=pipeline_name,
            duration_seconds=duration,
            rows_processed=result["rows_merged"],
            details={
                "rows_extracted": result["rows_extracted"],
                "rows_loaded": result["rows_loaded"],
                "rows_merged": result["rows_merged"],
                "tables_updated": result["tables"],
                "run_date": datetime.now().strftime("%Y-%m-%d")
            }
        )
        
        print(f"\n✅ Pipeline completed successfully in {duration:.1f}s")
        return 0
        
    except Exception as e:
        # ══════════════════════════════════════
        # FAILURE — alert on-call immediately
        # ══════════════════════════════════════
        duration = time.time() - start_time
        error_type = type(e).__name__
        error_message = str(e)
        
        # Get the failing line for debugging context
        tb = traceback.format_exc()
        # Only include last 3 lines of traceback (not full stack)
        short_tb = "\n".join(tb.strip().split("\n")[-3:])
        
        notifier.notify_failure(
            pipeline_name=pipeline_name,
            error_message=error_message,
            error_type=error_type,
            details={
                "duration_before_failure": f"{duration:.1f}s",
                "traceback_summary": short_tb,
                "run_date": datetime.now().strftime("%Y-%m-%d"),
                "action_required": "Check CloudWatch logs and fix the issue",
                "runbook": "https://wiki.quickcart.com/runbooks/etl-failure"
            }
        )
        
        print(f"\n❌ Pipeline FAILED after {duration:.1f}s: {error_message}")
        return 1


if __name__ == "__main__":
    # Run multiple times to see both success and failure notifications
    print("Running pipeline (may succeed or fail randomly)...\n")
    exit_code = run_pipeline_with_notifications()
    exit(exit_code)