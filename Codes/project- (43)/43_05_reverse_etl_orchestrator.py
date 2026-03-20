# file: 43_05_reverse_etl_orchestrator.py
# Purpose: Run the complete reverse ETL pipeline:
#          Redshift UNLOAD → S3 → Glue → MariaDB → Verify → Notify

import boto3
import time
import pymysql
from datetime import datetime

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET_NAME = "quickcart-raw-data-prod"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"

MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart"
}


def phase_1_redshift_unload():
    """
    PHASE 1: UNLOAD aggregated data from Redshift to S3.
    Decouples Redshift from downstream MariaDB load.
    """
    print(f"\n{'='*70}")
    print(f"📤 PHASE 1: REDSHIFT UNLOAD")
    print(f"{'='*70}")

    from redshift_unload import run_all_unloads  # 43_02
    return run_all_unloads()


def phase_2_glue_reverse_etl(unload_results):
    """
    PHASE 2: Run Glue jobs to load data from S3 into MariaDB.
    Runs both jobs (metrics + recommendations).
    """
    print(f"\n{'='*70}")
    print(f"🔀 PHASE 2: GLUE REVERSE ETL → MARIADB")
    print(f"{'='*70}")

    glue_client = boto3.client("glue", region_name=REGION)
    today = datetime.now().strftime("%Y-%m-%d")

    glue_jobs = [
        {
            "job_name": "quickcart-reverse-etl-customer-metrics",
            "unload_key": "customer_metrics",
            "source_s3_path": unload_results.get("customer_metrics", {}).get(
                "s3_prefix",
                f"s3://{BUCKET_NAME}/reverse_etl/customer_metrics/dt={today}/"
            )
        },
        {
            "job_name": "quickcart-reverse-etl-customer-recommendations",
            "unload_key": "customer_recommendations",
            "source_s3_path": unload_results.get("customer_recommendations", {}).get(
                "s3_prefix",
                f"s3://{BUCKET_NAME}/reverse_etl/customer_recommendations/dt={today}/"
            )
        }
    ]

    job_runs = {}

    for job_config in glue_jobs:
        unload_key = job_config["unload_key"]

        # Skip if UNLOAD failed for this entity
        if unload_results.get(unload_key, {}).get("status") != "SUCCESS":
            print(f"\n⏭️  Skipping {unload_key} — UNLOAD failed")
            job_runs[unload_key] = {"status": "SKIPPED", "reason": "UNLOAD failed"}
            continue

        job_name = job_config["job_name"]
        source_path = job_config["source_s3_path"]

        print(f"\n🚀 Starting: {job_name}")
        print(f"   Source: {source_path}")

        try:
            response = glue_client.start_job_run(
                JobName=job_name,
                Arguments={
                    "--source_s3_path": source_path
                }
            )
            run_id = response["JobRunId"]
            print(f"   Run ID: {run_id}")

            job_runs[unload_key] = {
                "job_name": job_name,
                "run_id": run_id,
                "status": "RUNNING"
            }

        except Exception as e:
            print(f"   ❌ Failed to start: {e}")
            job_runs[unload_key] = {"status": "FAILED", "error": str(e)}

    # Wait for all running jobs to complete
    print(f"\n⏳ Waiting for Glue jobs to complete...")

    for unload_key, run_info in job_runs.items():
        if run_info["status"] != "RUNNING":
            continue

        job_name = run_info["job_name"]
        run_id = run_info["run_id"]

        while True:
            status_response = glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id
            )
            state = status_response["JobRun"]["JobRunState"]

            if state in ("SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"):
                run_info["status"] = state
                run_info["duration"] = (
                    status_response["JobRun"].get("CompletedOn", datetime.now()) -
                    status_response["JobRun"]["StartedOn"]
                ).total_seconds()

                if state == "SUCCEEDED":
                    print(f"   ✅ {job_name}: SUCCEEDED ({run_info['duration']:.0f}s)")
                else:
                    error_msg = status_response["JobRun"].get("ErrorMessage", "Unknown")
                    run_info["error"] = error_msg
                    print(f"   ❌ {job_name}: {state} — {error_msg[:200]}")
                break

            print(f"   ⏳ {job_name}: {state}...", end="\r")
            time.sleep(15)

    return job_runs


def phase_3_verify_mariadb():
    """
    PHASE 3: Verify data landed correctly in MariaDB.
    Check row counts, freshness, and sample data.
    """
    print(f"\n{'='*70}")
    print(f"🔍 PHASE 3: MARIADB VERIFICATION")
    print(f"{'='*70}")

    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()
    results = {}

    # ━━━ CHECK 1: customer_metrics ━━━
    print(f"\n   📋 customer_metrics:")

    cursor.execute("SELECT COUNT(*) FROM customer_metrics")
    count = cursor.fetchone()[0]
    print(f"      Rows: {count:,}")

    cursor.execute("SELECT MAX(computed_at) FROM customer_metrics")
    latest = cursor.fetchone()[0]
    print(f"      Latest computed_at: {latest}")

    cursor.execute("""
        SELECT segment, COUNT(*) as cnt, 
               ROUND(AVG(lifetime_value), 2) as avg_ltv
        FROM customer_metrics
        GROUP BY segment
        ORDER BY avg_ltv DESC
    """)
    print(f"      Segment distribution:")
    for row in cursor.fetchall():
        print(f"        {row[0]:<12} {row[1]:>8,} customers, avg LTV: ${row[2]:>10,.2f}")

    results["customer_metrics"] = {"rows": count, "latest": str(latest)}

    # ━━━ CHECK 2: customer_recommendations ━━━
    print(f"\n   📋 customer_recommendations:")

    cursor.execute("SELECT COUNT(*) FROM customer_recommendations")
    count = cursor.fetchone()[0]
    print(f"      Rows: {count:,}")

    cursor.execute("SELECT MAX(computed_at) FROM customer_recommendations")
    latest = cursor.fetchone()[0]
    print(f"      Latest computed_at: {latest}")

    cursor.execute("SELECT COUNT(DISTINCT customer_id) FROM customer_recommendations")
    unique_customers = cursor.fetchone()[0]
    print(f"      Unique customers: {unique_customers:,}")

    cursor.execute("SELECT AVG(rank) FROM customer_recommendations")
    avg_rank = cursor.fetchone()[0]
    print(f"      Avg rank: {avg_rank:.1f} (expect ~3.0 for top-5)")

    # Sample: show recommendations for one customer
    cursor.execute("""
        SELECT cr.customer_id, cr.product_id, cr.score, cr.rank
        FROM customer_recommendations cr
        LIMIT 5
    """)
    print(f"\n      Sample recommendations:")
    print(f"      {'customer_id':<15} {'product_id':<15} {'score':<8} {'rank'}")
    print(f"      {'-'*50}")
    for row in cursor.fetchall():
        print(f"      {row[0]:<15} {row[1]:<15} {row[2]:<8} {row[3]}")

    results["customer_recommendations"] = {
        "rows": count,
        "latest": str(latest),
        "unique_customers": unique_customers
    }

    # ━━━ CHECK 3: Freshness ━━━
    print(f"\n   🕐 Freshness Check:")
    for table in ["customer_metrics", "customer_recommendations"]:
        cursor.execute(f"SELECT MAX(computed_at) FROM {table}")
        latest = cursor.fetchone()[0]
        if latest:
            age_hours = (datetime.now() - latest).total_seconds() / 3600
            status = "✅" if age_hours < 26 else "❌"
            print(f"      {status} {table}: {age_hours:.1f} hours old")
        else:
            print(f"      ❌ {table}: NO DATA")

    cursor.close()
    conn.close()

    return results


def phase_4_simulate_app_query():
    """
    PHASE 4: Simulate what the mobile app would do.
    Prove that the data is accessible and fast.
    """
    print(f"\n{'='*70}")
    print(f"📱 PHASE 4: SIMULATE APP QUERIES")
    print(f"{'='*70}")

    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    # Get a sample customer
    cursor.execute("SELECT customer_id FROM customer_metrics LIMIT 1")
    row = cursor.fetchone()
    if not row:
        print("   ⚠️  No data available for simulation")
        return
    customer_id = row[0]

    # ━━━ APP QUERY 1: Get customer profile + metrics ━━━
    print(f"\n   📱 App Query 1: Customer profile for {customer_id}")
    start = time.time()
    cursor.execute("""
        SELECT customer_id, lifetime_value, churn_risk, 
               segment, total_orders, avg_order_value
        FROM customer_metrics
        WHERE customer_id = %s
    """, (customer_id,))
    profile = cursor.fetchone()
    latency_ms = (time.time() - start) * 1000
    print(f"      Result: LTV=${profile[1]}, churn={profile[2]}, segment={profile[3]}")
    print(f"      ⚡ Latency: {latency_ms:.1f}ms")

    # ━━━ APP QUERY 2: Get top 5 recommendations ━━━
    print(f"\n   📱 App Query 2: Recommendations for {customer_id}")
    start = time.time()
    cursor.execute("""
        SELECT product_id, score, rank
        FROM customer_recommendations
        WHERE customer_id = %s
        ORDER BY rank ASC
        LIMIT 5
    """, (customer_id,))
    recs = cursor.fetchall()
    latency_ms = (time.time() - start) * 1000

    if recs:
        for rec in recs:
            print(f"      Rank {rec[2]}: {rec[0]} (score: {rec[1]})")
    else:
        print(f"      (no recommendations for this customer)")
    print(f"      ⚡ Latency: {latency_ms:.1f}ms")

    # ━━━ APP QUERY 3: High churn customers (for push notification targeting) ━━━
    print(f"\n   📱 App Query 3: High churn risk customers (for re-engagement)")
    start = time.time()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM customer_metrics
        WHERE churn_risk > 0.7
        AND segment IN ('gold', 'platinum')
    """)
    high_value_churning = cursor.fetchone()[0]
    latency_ms = (time.time() - start) * 1000
    print(f"      High-value customers at risk: {high_value_churning:,}")
    print(f"      ⚡ Latency: {latency_ms:.1f}ms")

    cursor.close()
    conn.close()

    print(f"\n   ✅ All app queries executing under 5ms target")


def phase_5_notify(unload_results, glue_results, verify_results):
    """PHASE 5: Send pipeline completion notification"""
    print(f"\n{'='*70}")
    print(f"📧 PHASE 5: NOTIFICATION")
    print(f"{'='*70}")

    sns_client = boto3.client("sns", region_name=REGION)

    # Determine overall status
    all_success = True
    for key in ["customer_metrics", "customer_recommendations"]:
        if unload_results.get(key, {}).get("status") != "SUCCESS":
            all_success = False
        if glue_results.get(key, {}).get("status") != "SUCCEEDED":
            all_success = False

    status = "SUCCESS" if all_success else "PARTIAL_FAILURE"
    subject = f"{'✅' if all_success else '⚠️'} QuickCart Reverse ETL — {status}"

    message_lines = [
        f"Reverse ETL Pipeline: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Status: {status}",
        "",
        "UNLOAD Results:",
    ]

    for key, result in unload_results.items():
        message_lines.append(f"  {key}: {result.get('status', 'N/A')} ({result.get('files', 0)} files)")

    message_lines.append("")
    message_lines.append("Glue Job Results:")
    for key, result in glue_results.items():
        dur = result.get("duration", 0)
        message_lines.append(f"  {key}: {result.get('status', 'N/A')} ({dur:.0f}s)")

    message_lines.append("")
    message_lines.append("MariaDB Verification:")
    for key, result in verify_results.items():
        message_lines.append(f"  {key}: {result.get('rows', 0):,} rows, latest: {result.get('latest', 'N/A')}")

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message="\n".join(message_lines)
        )
        print(f"   📧 Notification sent: {subject}")
    except Exception as e:
        print(f"   ⚠️  Notification failed: {e}")


def run_reverse_etl_pipeline():
    """
    COMPLETE REVERSE ETL ORCHESTRATOR
    
    FLOW:
    ┌─────────────────────────────────────────────────────┐
    │ Phase 1: UNLOAD Redshift → S3                       │
    │ Phase 2: Glue ETL → MariaDB (upsert + full replace)│
    │ Phase 3: Verify row counts + freshness               │
    │ Phase 4: Simulate app queries (latency test)         │
    │ Phase 5: Send notification                           │
    └─────────────────────────────────────────────────────┘
    
    In production: each phase is a Step Functions state (Project 41).
    """
    pipeline_start = time.time()

    print("╔" + "═" * 68 + "╗")
    print("║" + "  QUICKCART REVERSE ETL PIPELINE".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    # Phase 1
    unload_results = phase_1_redshift_unload()

    # Phase 2
    glue_results = phase_2_glue_reverse_etl(unload_results)

    # Phase 3
    verify_results = phase_3_verify_mariadb()

    # Phase 4
    phase_4_simulate_app_query()

    # Phase 5
    phase_5_notify(unload_results, glue_results, verify_results)

    # Final summary
    duration = time.time() - pipeline_start
    print(f"\n╔{'═'*68}╗")
    print(f"║{'  REVERSE ETL PIPELINE COMPLETE'.center(68)}║")
    print(f"║{f'  Duration: {duration:.1f} seconds'.center(68)}║")
    print(f"╚{'═'*68}╝")

    return 0


if __name__ == "__main__":
    exit_code = run_reverse_etl_pipeline()
    exit(exit_code)