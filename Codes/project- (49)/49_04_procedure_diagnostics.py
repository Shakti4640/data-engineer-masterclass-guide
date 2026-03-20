# file: 49_04_procedure_diagnostics.py
# Purpose: Monitor stored procedure health, check messages, diagnose failures

import psycopg2
from datetime import datetime

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def check_procedure_messages(run_group_id=None, limit=50):
    """
    View RAISE INFO/WARNING messages from stored procedure executions.
    
    These messages are stored in SVL_STORED_PROC_MESSAGES system view.
    Essential for debugging: "What did the procedure actually do?"
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n{'='*70}")
    print(f"📋 STORED PROCEDURE MESSAGES")
    print(f"{'='*70}")

    if run_group_id:
        cursor.execute("""
            SELECT recordtime, loglevel, message
            FROM svl_stored_proc_messages
            WHERE message LIKE %s
            ORDER BY recordtime DESC
            LIMIT %s
        """, (f'%{run_group_id}%', limit))
    else:
        cursor.execute("""
            SELECT recordtime, loglevel, message
            FROM svl_stored_proc_messages
            ORDER BY recordtime DESC
            LIMIT %s
        """, (limit,))

    rows = cursor.fetchall()

    if not rows:
        print(f"   ℹ️  No procedure messages found")
    else:
        for row in rows:
            timestamp, level, message = row
            level_icon = {
                "INFO": "ℹ️",
                "WARNING": "⚠️",
                "NOTICE": "📋",
                "LOG": "📝"
            }.get(level, "❓")

            time_str = timestamp.strftime("%H:%M:%S") if timestamp else "—"
            print(f"   {time_str} {level_icon} [{level}] {message[:120]}")

    cursor.close()
    conn.close()


def check_orphaned_staging_tables():
    """
    Find staging tables left behind from failed procedure runs.
    These should be cleaned up to avoid confusion and wasted storage.
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n{'='*70}")
    print(f"🧹 ORPHANED STAGING TABLES CHECK")
    print(f"{'='*70}")

    cursor.execute("""
        SELECT schemaname, tablename, 
               pg_size_pretty(size) as table_size
        FROM (
            SELECT schemaname, tablename,
                   SUM(size) as size
            FROM svv_table_info
            WHERE tablename LIKE 'staging_%'
            GROUP BY schemaname, tablename
        )
        ORDER BY schemaname, tablename
    """)

    rows = cursor.fetchall()

    if not rows:
        print(f"   ✅ No orphaned staging tables found")
    else:
        print(f"   ⚠️  Found {len(rows)} staging table(s):")
        for row in rows:
            schema, table, size = row
            print(f"   → {schema}.{table} ({size})")

        print(f"\n   To clean up:")
        for row in rows:
            schema, table, _ = row
            print(f"   DROP TABLE IF EXISTS {schema}.{table};")

    cursor.close()
    conn.close()


def check_procedure_performance(days_back=7):
    """
    Analyze procedure execution performance trends.
    Identifies: slow runs, increasing duration, frequent failures.
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n{'='*70}")
    print(f"📊 PROCEDURE PERFORMANCE ANALYSIS — Last {days_back} days")
    print(f"{'='*70}")

    # Overall success rate
    cursor.execute("""
        SELECT 
            status,
            COUNT(*) as run_count,
            ROUND(AVG(duration_seconds), 1) as avg_duration,
            ROUND(MAX(duration_seconds), 1) as max_duration,
            SUM(rows_merged) as total_rows
        FROM etl.run_log
        WHERE procedure_name = 'load_daily_data'
        AND started_at >= DATEADD(day, -%s, GETDATE())
        GROUP BY status
    """, (days_back,))

    rows = cursor.fetchall()

    total_runs = sum(r[1] for r in rows)
    print(f"\n   Pipeline Runs: {total_runs}")
    for row in rows:
        status, count, avg_dur, max_dur, total_rows = row
        pct = (count / max(total_runs, 1)) * 100
        icon = "✅" if status == "SUCCESS" else "❌"
        print(f"   {icon} {status}: {count} runs ({pct:.0f}%), "
              f"avg: {avg_dur}s, max: {max_dur}s, total rows: {total_rows or 0:,}")

    # Per-entity performance
    print(f"\n   Per-Entity Performance:")
    cursor.execute("""
        SELECT 
            entity_name,
            COUNT(*) as loads,
            ROUND(AVG(duration_seconds), 1) as avg_duration,
            ROUND(AVG(rows_merged), 0) as avg_rows,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failures
        FROM etl.run_log
        WHERE procedure_name = 'load_entity'
        AND started_at >= DATEADD(day, -%s, GETDATE())
        GROUP BY entity_name
        ORDER BY avg_duration DESC
    """, (days_back,))

    entity_rows = cursor.fetchall()
    print(f"   {'Entity':<15} {'Loads':<8} {'Avg Duration':<15} {'Avg Rows':<12} {'Failures'}")
    print(f"   {'─'*60}")
    for row in entity_rows:
        entity, loads, avg_dur, avg_rows, failures = row
        fail_icon = "❌" if failures > 0 else "✅"
        print(f"   {entity:<15} {loads:<8} {avg_dur}s{'':<10} {int(avg_rows or 0):>8,}   {fail_icon} {failures}")

    # Duration trend (is it getting slower?)
    print(f"\n   Duration Trend (daily):")
    cursor.execute("""
        SELECT 
            DATE(started_at) as run_date,
            ROUND(MAX(duration_seconds), 1) as duration,
            SUM(rows_merged) as total_rows
        FROM etl.run_log
        WHERE procedure_name = 'load_daily_data'
        AND status = 'SUCCESS'
        AND started_at >= DATEADD(day, -%s, GETDATE())
        GROUP BY DATE(started_at)
        ORDER BY run_date
    """, (days_back,))

    trend_rows = cursor.fetchall()
    for row in trend_rows:
        run_date, duration, total_rows = row
        bar_length = int(duration / 5) if duration else 0
        bar = "█" * min(bar_length, 40)
        print(f"   {run_date} | {duration:>6.1f}s | {bar} | {total_rows or 0:>10,} rows")

    cursor.close()
    conn.close()


def verify_procedures_exist():
    """Verify all expected procedures are created and accessible"""
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n{'='*70}")
    print(f"🔍 PROCEDURE VERIFICATION")
    print(f"{'='*70}")

    expected_procedures = [
        "etl.load_entity",
        "etl.load_daily_data",
        "etl.refresh_views",
        "etl.run_analyze",
        "etl.get_run_summary"
    ]

    cursor.execute("""
        SELECT schemaname || '.' || proname as full_name
        FROM pg_proc_info
        WHERE schemaname = 'etl'
        AND prokind = 'p'
        ORDER BY proname
    """)

    found = {row[0] for row in cursor.fetchall()}

    for proc in expected_procedures:
        if proc in found:
            print(f"   ✅ {proc}")
        else:
            print(f"   ❌ {proc} — NOT FOUND")

    # Check entity config
    cursor.execute("SELECT COUNT(*) FROM etl.entity_config WHERE is_active = TRUE")
    active_count = cursor.fetchone()[0]
    print(f"\n   📋 Active entities in config: {active_count}")

    cursor.close()
    conn.close()


def run_full_diagnostics():
    """Run all diagnostic checks"""
    print("╔" + "═" * 68 + "╗")
    print("║" + "  REDSHIFT ETL DIAGNOSTICS".center(68) + "║")
    print("║" + f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    verify_procedures_exist()
    check_orphaned_staging_tables()
    check_procedure_performance(days_back=7)
    check_procedure_messages(limit=20)


if __name__ == "__main__":
    run_full_diagnostics()