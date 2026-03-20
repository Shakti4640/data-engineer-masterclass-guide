# file: 63_02_manage_materialized_views.py
# Run from: Account B
# Purpose: Create MVs, refresh them, monitor status

import psycopg2
import boto3
import json
from datetime import datetime

# --- CONFIGURATION ---
REDSHIFT_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "analytics"
REDSHIFT_USER = "etl_writer"
REDSHIFT_PASS = "R3dsh1ftWr1t3r#2025"
SCHEMA = "quickcart_ops"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:222222222222:data-platform-alerts"

MV_DEFINITIONS = [
    "mv_daily_revenue",
    "mv_category_revenue",
    "mv_customer_tier_spend",
    "mv_hourly_volume",
    "mv_cumulative_revenue",
    "mv_top_customers",
    "mv_return_rate",
    "mv_new_vs_returning",
    "mv_weekly_aov",
    "mv_geo_revenue",
    "mv_order_funnel",
    "mv_forecast_vs_actual"
]


def get_connection():
    """Get Redshift connection"""
    return psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=[REDACTED:PASSWORD]
    )


def create_all_mvs(sql_file_content):
    """
    Execute all CREATE MATERIALIZED VIEW statements
    
    Each CREATE is idempotent:
    → If MV exists: skip (catch error)
    → If MV doesn't exist: create
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # Split SQL by semicolons, filter empty statements
    statements = [s.strip() for s in sql_file_content.split(";") if s.strip()]

    print("=" * 60)
    print("📦 CREATING MATERIALIZED VIEWS")
    print("=" * 60)

    created = 0
    skipped = 0
    failed = 0

    for stmt in statements:
        if not stmt.upper().startswith("CREATE MATERIALIZED VIEW"):
            continue

        # Extract MV name from statement
        mv_name = stmt.split("VIEW")[1].split("DISTSTYLE")[0].split("AUTO")[0].split("AS")[0].strip()

        try:
            print(f"\n📋 Creating: {mv_name}")
            cur.execute(stmt)
            print(f"   ✅ Created successfully")
            created += 1
        except psycopg2.errors.DuplicateTable:
            conn.rollback() if not conn.autocommit else None
            print(f"   ℹ️  Already exists — skipping")
            skipped += 1
        except Exception as e:
            conn.rollback() if not conn.autocommit else None
            print(f"   ❌ Error: {str(e)[:100]}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"📊 CREATION SUMMARY: {created} created, {skipped} skipped, {failed} failed")
    print(f"{'='*60}")

    cur.close()
    conn.close()


def refresh_all_mvs():
    """
    Refresh all materialized views after ETL completion
    
    ORDER MATTERS:
    → If MV-B depends on MV-A (nested MV): refresh A first
    → Our MVs are independent (all query base tables directly)
    → So order doesn't matter — but we track timing for each
    
    CALLED BY: Step Functions after ETL job completes
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("=" * 60)
    print("🔄 REFRESHING ALL MATERIALIZED VIEWS")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    results = []
    total_start = datetime.now()

    for mv_name in MV_DEFINITIONS:
        full_mv = f"{SCHEMA}.{mv_name}"
        print(f"\n🔄 Refreshing: {full_mv}")

        start = datetime.now()
        try:
            cur.execute(f"REFRESH MATERIALIZED VIEW {full_mv};")
            duration = (datetime.now() - start).total_seconds()
            
            # Get row count after refresh
            cur.execute(f"SELECT COUNT(*) FROM {full_mv}")
            row_count = cur.fetchone()[0]

            print(f"   ✅ Refreshed in {duration:.2f}s — {row_count:,} rows")
            results.append({
                "mv": mv_name,
                "status": "success",
                "duration_s": round(duration, 2),
                "rows": row_count
            })

        except Exception as e:
            duration = (datetime.now() - start).total_seconds()
            error_msg = str(e)[:200]
            print(f"   ❌ Failed in {duration:.2f}s: {error_msg}")
            results.append({
                "mv": mv_name,
                "status": "failed",
                "duration_s": round(duration, 2),
                "error": error_msg
            })

    total_duration = (datetime.now() - total_start).total_seconds()

    # --- SUMMARY ---
    succeeded = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]

    print(f"\n{'='*60}")
    print(f"📊 REFRESH SUMMARY")
    print(f"   Total time: {total_duration:.2f}s")
    print(f"   Succeeded: {len(succeeded)}/{len(results)}")
    print(f"   Failed: {len(failed)}/{len(results)}")

    if succeeded:
        print(f"\n   ✅ Successful refreshes:")
        print(f"   {'MV Name':<30} {'Time':>8} {'Rows':>10}")
        print(f"   {'-'*50}")
        for r in succeeded:
            print(f"   {r['mv']:<30} {r['duration_s']:>6.2f}s {r['rows']:>10,}")

    if failed:
        print(f"\n   ❌ Failed refreshes:")
        for r in failed:
            print(f"   {r['mv']}: {r['error']}")

    print(f"{'='*60}")

    cur.close()
    conn.close()

    # Alert on failures
    if failed:
        send_refresh_failure_alert(failed, total_duration)

    return results


def check_mv_status():
    """
    Check current status of all MVs
    
    SYSTEM TABLES:
    → STV_MV_INFO: MV metadata (state, staleness, auto-refresh)
    → SVV_TABLE_INFO: storage info (size, rows)
    → SVL_MV_REFRESH_STATUS: refresh history
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("=" * 70)
    print("📋 MATERIALIZED VIEW STATUS REPORT")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # --- MV Info ---
    cur.execute("""
        SELECT
            schema_name || '.' || mv_name AS full_name,
            state,
            is_stale,
            autorefresh,
            CASE WHEN is_stale = 0 THEN 'FRESH' ELSE 'STALE' END AS freshness
        FROM STV_MV_INFO
        WHERE schema_name = %s
        ORDER BY mv_name
    """, (SCHEMA,))

    rows = cur.fetchall()

    print(f"\n{'MV Name':<45} {'State':>6} {'Fresh':>8} {'Auto':>6}")
    print("-" * 70)

    for row in rows:
        full_name, state, is_stale, autorefresh, freshness = row
        state_icon = "✅" if state == 1 else "❌"
        fresh_icon = "🟢" if freshness == "FRESH" else "🔴"
        auto_icon = "🔄" if autorefresh == 1 else "⏸️"
        mv_short = full_name.split(".")[-1]
        print(f"  {mv_short:<45} {state_icon:<6} {fresh_icon} {freshness:<6} {auto_icon}")

    # --- Storage Usage ---
    print(f"\n{'MV Name':<45} {'Rows':>10} {'Size MB':>10}")
    print("-" * 70)

    for mv_name in MV_DEFINITIONS:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{mv_name}")
            row_count = cur.fetchone()[0]

            cur.execute(f"""
                SELECT size 
                FROM SVV_TABLE_INFO 
                WHERE schema = '{SCHEMA}' AND "table" = '{mv_name}'
            """)
            size_result = cur.fetchone()
            size_mb = size_result[0] if size_result else 0

            print(f"  {mv_name:<45} {row_count:>10,} {size_mb:>8.1f} MB")
        except Exception as e:
            print(f"  {mv_name:<45} {'ERROR':>10} {str(e)[:30]}")

    # --- Recent Refresh History ---
    print(f"\n📜 Last 5 Refresh Events:")
    print("-" * 70)

    cur.execute("""
        SELECT
            mv_name,
            refresh_type,
            status,
            starttime,
            endtime,
            DATEDIFF(second, starttime, endtime) AS duration_s
        FROM SVL_MV_REFRESH_STATUS
        ORDER BY starttime DESC
        LIMIT 5
    """)

    refresh_rows = cur.fetchall()
    if refresh_rows:
        print(f"  {'MV':<30} {'Type':>12} {'Status':>10} {'Duration':>10} {'Time'}")
        print(f"  {'-'*80}")
        for r in refresh_rows:
            mv, rtype, status, start, end, dur = r
            status_icon = "✅" if status == 'Refreshed' else "❌"
            print(f"  {mv:<30} {rtype:>12} {status_icon} {status:<8} {dur:>6}s   {start}")
    else:
        print("  No refresh history found")

    print(f"\n{'='*70}")

    cur.close()
    conn.close()


def verify_query_rewrite():
    """
    Verify that Redshift uses MVs instead of base tables
    
    HOW TO CHECK:
    → Run EXPLAIN on the dashboard query
    → Look for MV table name in scan step
    → If MV appears: rewrite working ✅
    → If base table appears: rewrite NOT working ❌
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("=" * 60)
    print("🔍 QUERY REWRITE VERIFICATION")
    print("=" * 60)

    # Test queries that SHOULD trigger rewrite
    test_queries = [
        {
            "name": "Q1 — Daily Revenue",
            "expected_mv": "mv_daily_revenue",
            "sql": """
                SELECT order_date, COUNT(order_id), SUM(total_amount)
                FROM quickcart_ops.orders
                WHERE order_date >= CURRENT_DATE - 30
                  AND status != 'cancelled'
                GROUP BY order_date
            """
        },
        {
            "name": "Q2 — Category Revenue",
            "expected_mv": "mv_category_revenue",
            "sql": """
                SELECT p.category, SUM(o.total_amount)
                FROM quickcart_ops.orders o
                JOIN quickcart_ops.dim_products p
                    ON o.product_id = p.product_id
                WHERE o.order_date >= CURRENT_DATE - 30
                  AND o.status != 'cancelled'
                GROUP BY p.category
            """
        },
        {
            "name": "Q3 — Customer Tier Spend",
            "expected_mv": "mv_customer_tier_spend",
            "sql": """
                SELECT c.tier, COUNT(DISTINCT c.customer_id), SUM(o.total_amount)
                FROM quickcart_ops.dim_customers c
                LEFT JOIN quickcart_ops.orders o
                    ON c.customer_id = o.customer_id
                    AND o.order_date >= CURRENT_DATE - 90
                    AND o.status != 'cancelled'
                GROUP BY c.tier
            """
        },
        {
            "name": "Q7 — Return Rate",
            "expected_mv": "mv_return_rate",
            "sql": """
                SELECT p.category, COUNT(o.order_id),
                       SUM(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END)
                FROM quickcart_ops.orders o
                JOIN quickcart_ops.dim_products p
                    ON o.product_id = p.product_id
                WHERE o.order_date >= CURRENT_DATE - 90
                GROUP BY p.category
            """
        },
        {
            "name": "Q11 — Order Funnel",
            "expected_mv": "mv_order_funnel",
            "sql": """
                SELECT status, COUNT(order_id), SUM(total_amount)
                FROM quickcart_ops.orders
                WHERE order_date >= CURRENT_DATE - 7
                GROUP BY status
            """
        }
    ]

    rewrite_success = 0
    rewrite_failed = 0

    for test in test_queries:
        print(f"\n📋 Testing: {test['name']}")
        print(f"   Expected MV: {test['expected_mv']}")

        try:
            # Run EXPLAIN to see query plan
            cur.execute(f"EXPLAIN {test['sql']}")
            plan_rows = cur.fetchall()
            plan_text = "\n".join([row[0] for row in plan_rows])

            # Check if MV appears in plan
            mv_used = test["expected_mv"] in plan_text.lower()

            if mv_used:
                print(f"   ✅ REWRITE WORKING — Redshift uses {test['expected_mv']}")
                rewrite_success += 1
            else:
                print(f"   ❌ REWRITE NOT TRIGGERED — Redshift scans base tables")
                print(f"   Plan excerpt:")
                # Show first 5 lines of plan for debugging
                for line in plan_text.split("\n")[:5]:
                    print(f"      {line}")
                rewrite_failed += 1

                # Common reasons for rewrite failure
                print(f"\n   🔍 Debug checklist:")
                print(f"      [ ] MV is in FRESH state (not stale)?")
                print(f"      [ ] MV query matches this query exactly?")
                print(f"      [ ] Column order and aliases match?")
                print(f"      [ ] WHERE clause in MV is superset?")
                print(f"      [ ] enable_case_sensitive_identifier = off?")

        except Exception as e:
            print(f"   ❌ EXPLAIN failed: {str(e)[:100]}")
            rewrite_failed += 1

    # --- SUMMARY ---
    print(f"\n{'='*60}")
    print(f"📊 QUERY REWRITE SUMMARY")
    print(f"   Tested: {len(test_queries)}")
    print(f"   Rewrite working: {rewrite_success} ✅")
    print(f"   Rewrite failed: {rewrite_failed} ❌")

    if rewrite_failed > 0:
        print(f"\n   ⚠️ FALLBACK OPTION:")
        print(f"   Point dashboard directly at MV tables:")
        print(f"   SELECT * FROM quickcart_ops.mv_daily_revenue;")
        print(f"   (instead of original base table query)")

    print(f"{'='*60}")

    cur.close()
    conn.close()

    return rewrite_success, rewrite_failed


def benchmark_before_after():
    """
    Run each dashboard query against base tables AND MVs
    Compare execution times to quantify improvement
    
    METHODOLOGY:
    → Disable result cache (force actual computation)
    → Run base table query → record time
    → Run MV query → record time
    → Calculate speedup factor
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # Disable result cache for honest benchmarking
    cur.execute("SET enable_result_cache_for_session = off;")

    print("=" * 70)
    print("⏱️  PERFORMANCE BENCHMARK: Base Tables vs Materialized Views")
    print("=" * 70)

    benchmarks = [
        {
            "name": "Q1 — Daily Revenue (30d)",
            "base_sql": """
                SELECT order_date, COUNT(order_id), SUM(total_amount),
                       AVG(total_amount), COUNT(DISTINCT customer_id)
                FROM quickcart_ops.orders
                WHERE order_date >= CURRENT_DATE - 30 AND status != 'cancelled'
                GROUP BY order_date
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_daily_revenue"
        },
        {
            "name": "Q2 — Category Revenue",
            "base_sql": """
                SELECT p.category, COUNT(o.order_id), SUM(o.total_amount),
                       AVG(o.total_amount), COUNT(DISTINCT o.customer_id)
                FROM quickcart_ops.orders o
                JOIN quickcart_ops.dim_products p ON o.product_id = p.product_id
                WHERE o.order_date >= CURRENT_DATE - 30 AND o.status != 'cancelled'
                GROUP BY p.category
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_category_revenue"
        },
        {
            "name": "Q5 — Cumulative Revenue (90d)",
            "base_sql": """
                SELECT order_date, daily_revenue,
                       SUM(daily_revenue) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING)
                FROM (
                    SELECT order_date, SUM(total_amount) AS daily_revenue
                    FROM quickcart_ops.orders
                    WHERE order_date >= CURRENT_DATE - 90 AND status != 'cancelled'
                    GROUP BY order_date
                )
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_cumulative_revenue"
        },
        {
            "name": "Q6 — Top Customers LTV",
            "base_sql": """
                SELECT c.customer_id, c.name, c.tier,
                       COUNT(o.order_id), SUM(o.total_amount),
                       MIN(o.order_date), MAX(o.order_date)
                FROM quickcart_ops.dim_customers c
                JOIN quickcart_ops.orders o ON c.customer_id = o.customer_id
                WHERE o.status != 'cancelled'
                GROUP BY c.customer_id, c.name, c.tier
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_top_customers"
        },
        {
            "name": "Q8 — New vs Returning (30d)",
            "base_sql": """
                SELECT o.order_date,
                    SUM(CASE WHEN c.signup_date >= CURRENT_DATE - 30 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN c.signup_date < CURRENT_DATE - 30 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN c.signup_date >= CURRENT_DATE - 30 THEN o.total_amount ELSE 0 END),
                    SUM(CASE WHEN c.signup_date < CURRENT_DATE - 30 THEN o.total_amount ELSE 0 END)
                FROM quickcart_ops.orders o
                JOIN quickcart_ops.dim_customers c ON o.customer_id = c.customer_id
                WHERE o.order_date >= CURRENT_DATE - 30 AND o.status != 'cancelled'
                GROUP BY o.order_date
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_new_vs_returning"
        },
        {
            "name": "Q12 — Monthly Forecast vs Actual",
            "base_sql": """
                SELECT DATE_TRUNC('month', order_date), SUM(total_amount),
                       COUNT(order_id), COUNT(DISTINCT customer_id), AVG(total_amount)
                FROM quickcart_ops.orders
                WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
                  AND status != 'cancelled'
                GROUP BY DATE_TRUNC('month', order_date)
            """,
            "mv_sql": "SELECT * FROM quickcart_ops.mv_forecast_vs_actual"
        }
    ]

    print(f"\n{'Query':<35} {'Base Table':>12} {'MV':>12} {'Speedup':>10}")
    print("-" * 72)

    total_base_time = 0
    total_mv_time = 0

    for bm in benchmarks:
        # --- BASE TABLE QUERY ---
        import time
        start = time.time()
        try:
            cur.execute(bm["base_sql"])
            _ = cur.fetchall()
            base_time = time.time() - start
        except Exception as e:
            base_time = -1
            print(f"  {bm['name']}: base query error — {str(e)[:50]}")
            continue

        # --- MV QUERY ---
        start = time.time()
        try:
            cur.execute(bm["mv_sql"])
            _ = cur.fetchall()
            mv_time = time.time() - start
        except Exception as e:
            mv_time = -1
            print(f"  {bm['name']}: MV query error — {str(e)[:50]}")
            continue

        # --- COMPARISON ---
        speedup = base_time / mv_time if mv_time > 0 else 0
        total_base_time += base_time
        total_mv_time += mv_time

        speedup_display = f"{speedup:.0f}x"
        print(f"  {bm['name']:<35} {base_time:>10.3f}s {mv_time:>10.3f}s {speedup_display:>10}")

    # --- TOTAL ---
    total_speedup = total_base_time / total_mv_time if total_mv_time > 0 else 0
    print("-" * 72)
    print(f"  {'TOTAL':<35} {total_base_time:>10.3f}s {total_mv_time:>10.3f}s {total_speedup:.0f}x")

    # --- DAILY IMPACT ---
    refreshes_per_day = 288  # every 5 minutes
    queries_per_refresh = 12
    total_queries_per_day = refreshes_per_day * queries_per_refresh

    daily_base_compute = total_base_time * refreshes_per_day
    daily_mv_compute = total_mv_time * refreshes_per_day
    daily_savings = daily_base_compute - daily_mv_compute

    print(f"\n📊 DAILY IMPACT PROJECTION:")
    print(f"   Dashboard refreshes/day: {refreshes_per_day}")
    print(f"   Queries/day: {total_queries_per_day:,}")
    print(f"   Without MVs: {daily_base_compute/3600:.1f} hours of compute/day")
    print(f"   With MVs: {daily_mv_compute/3600:.2f} hours of compute/day")
    print(f"   Saved: {daily_savings/3600:.1f} hours of compute/day")
    print(f"   Cluster CPU freed: ~{(daily_savings/daily_base_compute)*100:.0f}%")

    # Re-enable result cache
    cur.execute("SET enable_result_cache_for_session = on;")

    cur.close()
    conn.close()


def send_refresh_failure_alert(failed_mvs, total_duration):
    """
    Send SNS alert when MV refresh fails
    Builds on Project 12 (SNS) and Project 54 (alerting)
    """
    sns = boto3.client("sns", region_name="us-east-2")

    subject = f"❌ MV Refresh FAILED — {len(failed_mvs)} materialized view(s)"

    message_lines = [
        "MATERIALIZED VIEW REFRESH FAILURE",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total refresh duration: {total_duration:.2f}s",
        "",
        "Failed MVs:",
        "-" * 50
    ]

    for mv in failed_mvs:
        message_lines.extend([
            f"  MV: {mv['mv']}",
            f"  Error: {mv.get('error', 'Unknown')}",
            f"  Duration: {mv['duration_s']}s",
            ""
        ])

    message_lines.extend([
        "IMPACT:",
        "→ Dashboard showing STALE data for failed MVs",
        "→ Analysts may see yesterday's numbers",
        "",
        "ACTION REQUIRED:",
        "1. Check Redshift cluster status",
        "2. Review error messages above",
        "3. Manually run: REFRESH MATERIALIZED VIEW <mv_name>;",
        "4. If persistent: check base table locks from ETL"
    ])

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message="\n".join(message_lines)
    )
    print(f"📧 Failure alert sent to {SNS_TOPIC_ARN}")


def grant_mv_access():
    """
    Grant SELECT access on all MVs to analyst group
    
    MVs are treated as tables for permission purposes
    → Must explicitly GRANT
    → Or use: GRANT SELECT ON ALL TABLES IN SCHEMA
    """
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("🔐 Granting MV access to analysts...")

    # Grant on all current MVs
    cur.execute(f"""
        GRANT SELECT ON ALL TABLES IN SCHEMA {SCHEMA} TO GROUP analysts;
    """)
    print(f"   ✅ SELECT granted on all tables in {SCHEMA} to analysts group")

    # Set default for future MVs
    cur.execute(f"""
        ALTER DEFAULT PRIVILEGES IN SCHEMA {SCHEMA}
        GRANT SELECT ON TABLES TO GROUP analysts;
    """)
    print(f"   ✅ Default privileges set for future tables")

    cur.close()
    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python 63_02_manage_materialized_views.py <command>")
        print("Commands: create | refresh | status | verify | benchmark | grant")
        sys.exit(1)

    command = sys.argv[1]

    if command == "create":
        # Read SQL from Part 1
        create_all_mvs(MATERIALIZED_VIEWS_SQL)
    elif command == "refresh":
        refresh_all_mvs()
    elif command == "status":
        check_mv_status()
    elif command == "verify":
        verify_query_rewrite()
    elif command == "benchmark":
        benchmark_before_after()
    elif command == "grant":
        grant_mv_access()
    else:
        print(f"Unknown command: {command}")