# file: 49_03_call_procedures.py
# Purpose: How to call stored procedures from Glue/Step Functions/Python
# This replaces the 20-statement approach from Project 42

import psycopg2
import time
from datetime import datetime, date

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def call_daily_load(process_date):
    """
    Call the master ETL procedure — ONE call replaces 20 individual statements.
    
    BEFORE (Project 42):
    → 20 individual SQL statements via psycopg2
    → 20 network round trips
    → No atomicity across entities
    → Error handling: manual try/except per statement
    → Duration: ~33 seconds
    
    AFTER (this project):
    → 1 CALL statement
    → 1 network round trip (procedure executes server-side)
    → Full atomicity (all-or-nothing)
    → Error handling: EXCEPTION block inside procedure
    → Duration: ~31 seconds (same data work, less overhead)
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print("╔" + "═" * 68 + "╗")
    print("║" + f"  REDSHIFT ETL — Stored Procedure Call".center(68) + "║")
    print("║" + f"  Date: {process_date}".center(68) + "║")
    print("╚" + "═" * 68 + "╝")

    start_time = time.time()

    try:
        # Set statement timeout (safety net)
        cursor.execute("SET statement_timeout TO 1800000;")  # 30 minutes

        # THE ONE CALL THAT DOES EVERYTHING
        print(f"\n🚀 Calling: etl.load_daily_data('{process_date}')")
        cursor.execute(
            "CALL etl.load_daily_data(%s, %s)",
            (process_date, 'PENDING')
        )

        # Get return value (INOUT parameter)
        # For Redshift, INOUT params are returned as result set
        result = cursor.fetchone()
        status = result[0] if result else "UNKNOWN"

        conn.commit()
        duration = time.time() - start_time

        # Collect RAISE INFO messages
        notices = conn.notices
        print(f"\n📋 Procedure Messages:")
        for notice in notices:
            # Clean up the notice format
            msg = notice.strip()
            if msg:
                print(f"   {msg}")

        print(f"\n{'='*60}")
        print(f"📊 RESULT: {status}")
        print(f"⏱  Duration: {duration:.1f} seconds")
        print(f"{'='*60}")

        return status

    except psycopg2.Error as e:
        conn.rollback()
        duration = time.time() - start_time

        # Collect any notices before the error
        for notice in conn.notices:
            print(f"   {notice.strip()}")

        print(f"\n❌ PROCEDURE FAILED")
        print(f"   Error: {e.pgerror}")
        print(f"   Code: {e.pgcode}")
        print(f"   Duration: {duration:.1f}s")

        return "FAILED"

    finally:
        cursor.close()
        conn.close()


def query_run_history(days_back=7):
    """Query the ETL audit log to see recent runs"""
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n{'='*70}")
    print(f"📊 ETL RUN HISTORY — Last {days_back} days")
    print(f"{'='*70}")

    # Pipeline-level summary
    cursor.execute("""
        SELECT 
            run_group_id,
            process_date,
            status,
            rows_merged as total_rows,
            duration_seconds,
            error_message,
            started_at
        FROM etl.run_log
        WHERE procedure_name = 'load_daily_data'
        AND started_at >= DATEADD(day, -%s, GETDATE())
        ORDER BY started_at DESC
        LIMIT 20
    """, (days_back,))

    rows = cursor.fetchall()

    if not rows:
        print(f"   ℹ️  No ETL runs found in last {days_back} days")
    else:
        print(f"\n   {'Run ID':<30} {'Date':<12} {'Status':<10} {'Rows':<10} {'Duration':<10}")
        print(f"   {'─'*72}")

        for row in rows:
            run_id, proc_date, status, total_rows, duration, error, started = row
            status_icon = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "⏳"
            dur_str = f"{duration:.1f}s" if duration else "—"
            rows_str = f"{total_rows:,}" if total_rows else "—"

            print(f"   {status_icon} {run_id:<28} {str(proc_date):<12} {status:<10} {rows_str:<10} {dur_str}")

            if status == "FAILED" and error:
                print(f"      Error: {error[:80]}")

    # Entity-level detail for latest run
    cursor.execute("""
        SELECT run_group_id
        FROM etl.run_log
        WHERE procedure_name = 'load_daily_data'
        ORDER BY started_at DESC
        LIMIT 1
    """)
    latest = cursor.fetchone()

    if latest:
        latest_run = latest[0]
        print(f"\n   📋 DETAIL for latest run: {latest_run}")
        print(f"   {'─'*60}")

        cursor.execute("""
            SELECT 
                entity_name,
                status,
                rows_staged,
                rows_deduped,
                rows_merged,
                rows_inserted,
                rows_updated,
                duration_seconds,
                error_message
            FROM etl.run_log
            WHERE run_group_id = %s
            AND procedure_name = 'load_entity'
            ORDER BY started_at
        """, (latest_run,))

        entity_rows = cursor.fetchall()
        for erow in entity_rows:
            entity, status, staged, deduped, merged, inserted, updated, dur, err = erow
            status_icon = "✅" if status == "SUCCESS" else "❌"
            print(f"   {status_icon} {entity}:")
            print(f"      Staged: {staged:,} → Deduped: {deduped:,} → "
                  f"Merged: {merged:,} (new: {inserted:,}, updated: {updated:,})")
            print(f"      Duration: {dur:.1f}s")
            if err:
                print(f"      Error: {err[:100]}")

    cursor.close()
    conn.close()


def test_individual_entity():
    """
    Test loading a single entity — useful for debugging.
    Can be called from Redshift console or Python.
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    cursor = conn.cursor()

    print(f"\n🧪 TESTING: Load single entity (orders)")

    try:
        cursor.execute("SET statement_timeout TO 600000;")  # 10 minutes

        cursor.execute("""
            CALL etl.load_entity(
                'orders',
                's3://quickcart-datalake-prod/silver/orders/year=2025/month=01/day=15/',
                'order_id',
                'public',
                'orders',
                'arn:aws:iam::123456789012:role/quickcart-redshift-s3-role',
                'test_run_001',
                0
            )
        """)

        result = cursor.fetchone()
        rows_loaded = result[0] if result else 0
        conn.commit()

        print(f"   ✅ Entity loaded: {rows_loaded} rows")

        # Show notices
        for notice in conn.notices:
            print(f"   {notice.strip()}")

    except psycopg2.Error as e:
        conn.rollback()
        print(f"   ❌ Failed: {e.pgerror}")
    finally:
        cursor.close()
        conn.close()


def compare_before_after():
    """
    Compare the old approach (20 statements) vs new approach (1 CALL).
    Shows the architectural improvement.
    """
    print(f"\n{'='*70}")
    print(f"📊 BEFORE vs AFTER COMPARISON")
    print(f"{'='*70}")

    comparison = [
        ("Network round trips", "20", "1"),
        ("Lines of Python code", "~150", "~10"),
        ("Transaction control", "Manual (fragile)", "Automatic (EXCEPTION)"),
        ("Atomicity", "None (partial state)", "All-or-nothing"),
        ("Error cleanup", "Manual try/except", "EXCEPTION handler"),
        ("Code reuse", "Copy-paste per entity", "One procedure for all"),
        ("Testing", "Run entire Glue job", "CALL from SQL console"),
        ("Audit trail", "CloudWatch logs", "etl.run_log table (queryable)"),
        ("Adding new entity", "Copy 20 statements", "Add row to entity_config"),
        ("Debugging", "Read Glue job logs", "SVL_STORED_PROC_MESSAGES"),
    ]

    print(f"\n   {'Aspect':<25} {'Before (Project 42)':<30} {'After (Project 49)':<30}")
    print(f"   {'─'*85}")
    for aspect, before, after in comparison:
        print(f"   {aspect:<25} {before:<30} {after:<30}")

    print(f"   {'─'*85}")
    print(f"\n   💡 Key insight: same data operations, better engineering")
    print(f"      The MERGE, COPY, ANALYZE still run identically.")
    print(f"      What changed: orchestration, error handling, reusability.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 49_03_call_procedures.py load 2025-01-15")
        print("  python 49_03_call_procedures.py history")
        print("  python 49_03_call_procedures.py test_entity")
        print("  python 49_03_call_procedures.py compare")
        sys.exit(0)

    action = sys.argv[1]

    if action == "load":
        process_date = sys.argv[2] if len(sys.argv) > 2 else date.today().isoformat()
        status = call_daily_load(process_date)
        exit(0 if status == "SUCCESS" else 1)

    elif action == "history":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
        query_run_history(days)

    elif action == "test_entity":
        test_individual_entity()

    elif action == "compare":
        compare_before_after()

    else:
        print(f"Unknown action: {action}")