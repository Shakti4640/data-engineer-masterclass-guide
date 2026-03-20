# file: 05_scd2_merge.py
# Purpose: Demonstrate how to process SCD Type 2 changes
# This is the CORE pattern used in Projects 42, 65 for incremental CDC

import psycopg2
import time

REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "etl_loader",
    "password": "[REDACTED:PASSWORD]"
}


def simulate_customer_changes(cursor):
    """
    Simulate customer changes arriving in staging table
    In production: staging is populated by COPY from S3 (Project 21)
    Here: manually insert changed records for demonstration
    """
    print("\n📦 Simulating customer changes in staging...")
    
    cursor.execute("TRUNCATE staging.stg_customers;")
    
    # Simulate 3 types of changes:
    # 1. EXISTING customer with CHANGED tier (SCD2 → new version)
    # 2. EXISTING customer with UNCHANGED data (skip)
    # 3. BRAND NEW customer (insert first version)
    
    cursor.execute("""
        INSERT INTO staging.stg_customers VALUES
        -- Change 1: CUST-000001 tier changed from current to 'gold'
        ('CUST-000001', 'Customer 1', 'cust1@quickcart.com', 
         'New York', 'NY', 'gold', '2024-01-15', GETDATE(), GETDATE()),
        
        -- Change 2: CUST-000002 — name changed (SCD1 attribute, not tier)
        -- For simplicity, we'll track ALL changes via SCD2
        ('CUST-000002', 'Customer 2 UPDATED', 'cust2@quickcart.com', 
         'Los Angeles', 'CA', 'silver', '2024-03-20', GETDATE(), GETDATE()),
        
        -- Change 3: CUST-000003 — no actual change (same data as current)
        ('CUST-000003', 'Customer 3', 'cust3@quickcart.com', 
         'Chicago', 'IL', 'bronze', '2024-02-10', GETDATE(), GETDATE()),
        
        -- Change 4: BRAND NEW customer — never seen before
        ('CUST-NEW-001', 'New Customer Alpha', 'alpha@quickcart.com', 
         'Seattle', 'WA', 'bronze', '2025-07-01', GETDATE(), GETDATE()),
        
        -- Change 5: Another new customer
        ('CUST-NEW-002', 'New Customer Beta', 'beta@quickcart.com', 
         'Denver', 'CO', 'bronze', '2025-07-01', GETDATE(), GETDATE())
    """)
    
    cursor.execute("SELECT COUNT(*) FROM staging.stg_customers")
    count = cursor.fetchone()[0]
    print(f"   ✅ {count} records in staging")


def identify_changes(cursor):
    """
    Compare staging vs current dimension to classify each record
    
    CLASSIFICATION:
    → NEW: customer_id not in dimension → INSERT first version
    → CHANGED: customer_id exists BUT attributes differ → expire old, insert new
    → UNCHANGED: customer_id exists AND attributes identical → skip
    """
    print("\n🔍 Identifying changes...")
    
    cursor.execute("""
        SELECT 
            stg.customer_id,
            CASE 
                WHEN dim.customer_id IS NULL THEN 'NEW'
                WHEN stg.name != dim.name 
                  OR stg.email != dim.email
                  OR stg.city != COALESCE(dim.city, '')
                  OR stg.state != COALESCE(dim.state, '')
                  OR stg.tier != dim.tier 
                THEN 'CHANGED'
                ELSE 'UNCHANGED'
            END AS change_type,
            stg.tier AS new_tier,
            dim.tier AS current_tier,
            stg.name AS new_name,
            dim.name AS current_name
        FROM staging.stg_customers stg
        LEFT JOIN analytics.dim_customers_scd2 dim
            ON stg.customer_id = dim.customer_id
           AND dim.is_current = TRUE
        ORDER BY change_type, stg.customer_id
    """)
    
    columns = [desc[0] for desc in cursor.description]
    results = cursor.fetchall()
    
    print(f"\n   {'Change Type':<12} {'Customer ID':<15} {'Detail'}")
    print(f"   {'-'*60}")
    
    change_counts = {"NEW": 0, "CHANGED": 0, "UNCHANGED": 0}
    
    for row in results:
        row_dict = dict(zip(columns, row))
        ctype = row_dict["change_type"]
        cid = row_dict["customer_id"]
        change_counts[ctype] += 1
        
        if ctype == "NEW":
            detail = f"New customer → tier={row_dict['new_tier']}"
        elif ctype == "CHANGED":
            if row_dict["new_tier"] != row_dict["current_tier"]:
                detail = f"Tier: {row_dict['current_tier']} → {row_dict['new_tier']}"
            elif row_dict["new_name"] != row_dict["current_name"]:
                detail = f"Name: {row_dict['current_name']} → {row_dict['new_name']}"
            else:
                detail = "Other attribute changed"
        else:
            detail = "No changes detected — will skip"
        
        print(f"   {ctype:<12} {cid:<15} {detail}")
    
    print(f"\n   Summary: {change_counts['NEW']} new, "
          f"{change_counts['CHANGED']} changed, "
          f"{change_counts['UNCHANGED']} unchanged")
    
    return change_counts


def apply_scd2_merge(cursor, batch_id):
    """
    Apply SCD Type 2 merge logic:
    
    STEP 1: Expire old versions for CHANGED customers
    STEP 2: Insert new versions for CHANGED customers  
    STEP 3: Insert first versions for NEW customers
    STEP 4: Skip UNCHANGED customers (do nothing)
    
    ALL within a single transaction for atomicity
    """
    print(f"\n🔄 Applying SCD2 merge (batch: {batch_id})...")
    today = time.strftime('%Y-%m-%d')
    
    # Get current max surrogate key
    cursor.execute("SELECT COALESCE(MAX(surrogate_key), 0) FROM analytics.dim_customers_scd2")
    max_sk = cursor.fetchone()[0]
    print(f"   Current max surrogate_key: {max_sk}")
    
    # ---------------------------------------------------------------
    # STEP 1: Expire old versions for CHANGED customers
    # Set effective_to = today, is_current = FALSE
    # ---------------------------------------------------------------
    cursor.execute(f"""
        UPDATE analytics.dim_customers_scd2
        SET 
            effective_to = '{today}'::DATE,
            is_current = FALSE,
            etl_loaded_at = GETDATE(),
            etl_batch_id = '{batch_id}'
        WHERE customer_id IN (
            SELECT stg.customer_id
            FROM staging.stg_customers stg
            JOIN analytics.dim_customers_scd2 dim
                ON stg.customer_id = dim.customer_id
               AND dim.is_current = TRUE
            WHERE stg.name != dim.name 
               OR stg.email != dim.email
               OR stg.city != COALESCE(dim.city, '')
               OR stg.state != COALESCE(dim.state, '')
               OR stg.tier != dim.tier
        )
        AND is_current = TRUE
    """)
    
    expired_count = cursor.rowcount
    print(f"   Step 1: Expired {expired_count} old versions")
    
    # ---------------------------------------------------------------
    # STEP 2: Insert new versions for CHANGED customers
    # ---------------------------------------------------------------
    cursor.execute(f"""
        INSERT INTO analytics.dim_customers_scd2 (
            surrogate_key, customer_id, name, email, city, state, tier,
            signup_date, effective_from, effective_to, is_current,
            etl_loaded_at, etl_batch_id, source_system
        )
        SELECT 
            {max_sk} + ROW_NUMBER() OVER (ORDER BY stg.customer_id),
            stg.customer_id,
            stg.name,
            stg.email,
            stg.city,
            stg.state,
            stg.tier,
            stg.signup_date,
            '{today}'::DATE AS effective_from,
            '9999-12-31'::DATE AS effective_to,
            TRUE AS is_current,
            GETDATE(),
            '{batch_id}',
            'mariadb'
        FROM staging.stg_customers stg
        WHERE stg.customer_id IN (
            -- Only customers that had a version expired in Step 1
            SELECT customer_id FROM analytics.dim_customers_scd2
            WHERE etl_batch_id = '{batch_id}'
              AND is_current = FALSE
        )
    """)
    
    changed_count = cursor.rowcount
    print(f"   Step 2: Inserted {changed_count} new versions for changed customers")
    
    # Update max SK for new customers
    cursor.execute("SELECT COALESCE(MAX(surrogate_key), 0) FROM analytics.dim_customers_scd2")
    max_sk = cursor.fetchone()[0]
    
    # ---------------------------------------------------------------
    # STEP 3: Insert first versions for NEW customers
    # ---------------------------------------------------------------
    cursor.execute(f"""
        INSERT INTO analytics.dim_customers_scd2 (
            surrogate_key, customer_id, name, email, city, state, tier,
            signup_date, effective_from, effective_to, is_current,
            etl_loaded_at, etl_batch_id, source_system
        )
        SELECT 
            {max_sk} + ROW_NUMBER() OVER (ORDER BY stg.customer_id),
            stg.customer_id,
            stg.name,
            stg.email,
            stg.city,
            stg.state,
            stg.tier,
            stg.signup_date,
            stg.signup_date AS effective_from,
            '9999-12-31'::DATE AS effective_to,
            TRUE AS is_current,
            GETDATE(),
            '{batch_id}',
            'mariadb'
        FROM staging.stg_customers stg
        WHERE stg.customer_id NOT IN (
            SELECT DISTINCT customer_id FROM analytics.dim_customers_scd2
        )
    """)
    
    new_count = cursor.rowcount
    print(f"   Step 3: Inserted {new_count} brand new customers")
    
    return {
        "expired": expired_count,
        "changed": changed_count,
        "new": new_count
    }


def verify_scd2_integrity(cursor):
    """
    Run integrity checks on SCD2 dimension
    These checks should ALWAYS pass — if they don't, ETL has a bug
    """
    print("\n🔍 VERIFYING SCD2 INTEGRITY...")
    
    checks = [
        {
            "name": "Every customer has exactly ONE current version",
            "sql": """
                SELECT customer_id, COUNT(*) AS current_versions
                FROM analytics.dim_customers_scd2
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) != 1
            """,
            "expected_rows": 0
        },
        {
            "name": "No gaps in version timeline",
            "sql": """
                SELECT 
                    a.customer_id,
                    a.effective_to AS version_a_end,
                    b.effective_from AS version_b_start
                FROM analytics.dim_customers_scd2 a
                JOIN analytics.dim_customers_scd2 b
                    ON a.customer_id = b.customer_id
                   AND a.effective_to = b.effective_from
                   AND a.surrogate_key != b.surrogate_key
                WHERE a.effective_to != b.effective_from
                  AND a.is_current = FALSE
                LIMIT 10
            """,
            "expected_rows": 0
        },
        {
            "name": "All surrogate keys are unique",
            "sql": """
                SELECT surrogate_key, COUNT(*)
                FROM analytics.dim_customers_scd2
                GROUP BY surrogate_key
                HAVING COUNT(*) > 1
            """,
            "expected_rows": 0
        },
        {
            "name": "Current version has effective_to = 9999-12-31",
            "sql": """
                SELECT customer_id, effective_to
                FROM analytics.dim_customers_scd2
                WHERE is_current = TRUE
                  AND effective_to != '9999-12-31'
            """,
            "expected_rows": 0
        }
    ]
    
    all_passed = True
    for check in checks:
        cursor.execute(check["sql"])
        results = cursor.fetchall()
        actual_rows = len(results)
        passed = (actual_rows == check["expected_rows"])
        
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {check['name']}")
        
        if not passed:
            all_passed = False
            print(f"     Expected {check['expected_rows']} rows, got {actual_rows}")
            for row in results[:5]:
                print(f"     → {row}")
    
    return all_passed


def show_customer_history(cursor, customer_id):
    """Show full version history for a customer"""
    print(f"\n📜 VERSION HISTORY: {customer_id}")
    print(f"   {'-'*80}")
    
    cursor.execute("""
        SELECT 
            surrogate_key AS sk,
            customer_id,
            name,
            tier,
            effective_from,
            effective_to,
            is_current,
            etl_batch_id
        FROM analytics.dim_customers_scd2
        WHERE customer_id = %s
        ORDER BY effective_from
    """, (customer_id,))
    
    columns = [desc[0] for desc in cursor.description]
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))
        current_marker = " ◄ CURRENT" if row_dict["is_current"] else ""
        print(f"   SK={row_dict['sk']:<5} | {row_dict['tier']:<10} | "
              f"{row_dict['effective_from']} to {row_dict['effective_to']} | "
              f"batch={row_dict['etl_batch_id']}{current_marker}")


def run_scd2_demo():
    """Full SCD2 demonstration"""
    batch_id = f"SCD2-DEMO-{time.strftime('%Y%m%d-%H%M%S')}"
    
    print("=" * 65)
    print(f"🔄 SCD TYPE 2 MERGE DEMONSTRATION")
    print(f"   Batch: {batch_id}")
    print("=" * 65)
    
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = False        # Transaction control — all or nothing
    cursor = conn.cursor()
    
    try:
        # Show BEFORE state
        print("\n📋 BEFORE STATE:")
        show_customer_history(cursor, "CUST-000001")
        show_customer_history(cursor, "CUST-000002")
        
        cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) AS unique_customers,
                   COUNT(*) AS total_versions,
                   SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions
            FROM analytics.dim_customers_scd2
        """)
        before = cursor.fetchone()
        print(f"\n   Totals: {before[0]} customers, {before[1]} versions, "
              f"{before[2]} current")
        
        # Simulate changes arriving in staging
        simulate_customer_changes(cursor)
        
        # Identify what changed
        change_counts = identify_changes(cursor)
        
        # Apply SCD2 merge
        merge_results = apply_scd2_merge(cursor, batch_id)
        
        # Commit transaction
        conn.commit()
        print(f"\n   ✅ Transaction COMMITTED")
        
        # Verify integrity
        integrity_ok = verify_scd2_integrity(cursor)
        
        # Show AFTER state
        print(f"\n📋 AFTER STATE:")
        show_customer_history(cursor, "CUST-000001")
        show_customer_history(cursor, "CUST-000002")
        show_customer_history(cursor, "CUST-NEW-001")
        
        cursor.execute("""
            SELECT COUNT(DISTINCT customer_id) AS unique_customers,
                   COUNT(*) AS total_versions,
                   SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions
            FROM analytics.dim_customers_scd2
        """)
        after = cursor.fetchone()
        print(f"\n   Totals: {after[0]} customers, {after[1]} versions, "
              f"{after[2]} current")
        print(f"   Delta: +{after[0]-before[0]} customers, "
              f"+{after[1]-before[1]} versions")
        
        # Log to audit
        cursor.execute("""
            INSERT INTO audit.etl_job_log (
                job_name, batch_id, status, started_at, completed_at,
                rows_processed, rows_inserted, rows_updated,
                source_system, target_table
            ) VALUES (
                'scd2_customer_merge', %s, 'completed', GETDATE(), GETDATE(),
                %s, %s, %s, 'mariadb', 'dim_customers_scd2'
            )
        """, (
            batch_id,
            change_counts["NEW"] + change_counts["CHANGED"] + change_counts["UNCHANGED"],
            merge_results["new"] + merge_results["changed"],
            merge_results["expired"]
        ))
        conn.commit()
        
        # Summary
        print(f"\n{'='*65}")
        print(f"✅ SCD2 MERGE COMPLETE")
        print(f"   Expired:   {merge_results['expired']} old versions")
        print(f"   Changed:   {merge_results['changed']} new versions")
        print(f"   New:       {merge_results['new']} first versions")
        print(f"   Skipped:   {change_counts['UNCHANGED']} unchanged")
        print(f"   Integrity: {'✅ PASSED' if integrity_ok else '❌ FAILED'}")
        print(f"{'='*65}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ MERGE FAILED — ROLLED BACK: {e}")
        raise
    
    finally:
        conn.close()
        print("🔌 Connection closed")


if __name__ == "__main__":
    run_scd2_demo()