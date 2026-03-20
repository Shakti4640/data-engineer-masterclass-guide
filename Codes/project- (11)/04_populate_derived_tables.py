# file: 04_populate_derived_tables.py
# Purpose: Build wide fact table and aggregates from base fact/dim tables
# These are DERIVED tables — rebuilt after each ETL load

import psycopg2
import time

REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "etl_loader",
    "password": "[REDACTED:PASSWORD]"
}


def rebuild_wide_fact(cursor, batch_id):
    """
    Rebuild denormalized wide fact table
    
    PATTERN: TRUNCATE → INSERT SELECT (full rebuild)
    WHY full rebuild instead of incremental:
    → Dimension attributes change (customer tier upgrade)
    → Full rebuild guarantees consistency
    → At 100K rows: takes < 10 seconds
    → At 10M rows: takes < 2 minutes
    → At 100M rows: consider incremental (only rebuild recent dates)
    """
    print("\n📊 Rebuilding fact_orders_wide...")
    start = time.time()
    
    cursor.execute("TRUNCATE analytics.fact_orders_wide;")
    
    cursor.execute(f"""
        INSERT INTO analytics.fact_orders_wide (
            order_id, order_date, total_amount, item_count, net_amount, status,
            customer_id, customer_name, customer_city, customer_state,
            customer_tier, customer_signup,
            order_year, order_quarter, order_month, order_month_name,
            order_day_name, is_weekend,
            etl_batch_id
        )
        SELECT 
            f.order_id,
            f.order_date,
            f.total_amount,
            f.item_count,
            f.net_amount,
            f.status,
            
            -- Customer attributes (from SCD2 — version at order time)
            c.customer_id,
            c.name AS customer_name,
            c.city AS customer_city,
            c.state AS customer_state,
            c.tier AS customer_tier,
            c.signup_date AS customer_signup,
            
            -- Date attributes (from date dimension)
            d.year AS order_year,
            d.quarter AS order_quarter,
            d.month AS order_month,
            d.month_name AS order_month_name,
            d.day_name AS order_day_name,
            d.is_weekend,
            
            '{batch_id}' AS etl_batch_id
            
        FROM analytics.fact_orders f
        
        -- SCD2 join: get customer version that was active at order time
        JOIN analytics.dim_customers_scd2 c
            ON f.customer_sk = c.surrogate_key
        
        -- Date dimension join
        JOIN analytics.dim_dates d
            ON f.order_date = d.date_key
    """)
    
    cursor.execute("SELECT COUNT(*) FROM analytics.fact_orders_wide")
    count = cursor.fetchone()[0]
    
    elapsed = round(time.time() - start, 2)
    print(f"   ✅ {count:,} rows in {elapsed}s")
    return count


def rebuild_daily_revenue(cursor, batch_id):
    """Rebuild daily revenue aggregate"""
    print("\n📊 Rebuilding agg_daily_revenue...")
    start = time.time()
    
    cursor.execute("TRUNCATE agg.agg_daily_revenue;")
    
    cursor.execute(f"""
        INSERT INTO agg.agg_daily_revenue (
            date_key, status, order_count, total_revenue,
            avg_order_value, total_items, unique_customers
        )
        SELECT 
            order_date AS date_key,
            status,
            COUNT(*) AS order_count,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value,
            SUM(item_count) AS total_items,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM analytics.fact_orders
        GROUP BY order_date, status
        ORDER BY order_date, status
    """)
    
    cursor.execute("SELECT COUNT(*) FROM agg.agg_daily_revenue")
    count = cursor.fetchone()[0]
    
    elapsed = round(time.time() - start, 2)
    print(f"   ✅ {count:,} rows in {elapsed}s")
    return count


def rebuild_monthly_category(cursor, batch_id):
    """Rebuild monthly category aggregate"""
    print("\n📊 Rebuilding agg_monthly_category...")
    start = time.time()
    
    cursor.execute("TRUNCATE agg.agg_monthly_category;")
    
    cursor.execute(f"""
        INSERT INTO agg.agg_monthly_category (
            year_month, category, order_count, units_sold,
            revenue, unique_customers, unique_products
        )
        SELECT 
            TO_CHAR(fi.order_date, 'YYYY-MM') AS year_month,
            p.category,
            COUNT(DISTINCT fi.order_id) AS order_count,
            SUM(fi.quantity) AS units_sold,
            ROUND(SUM(fi.line_total), 2) AS revenue,
            COUNT(DISTINCT fi.customer_id) AS unique_customers,
            COUNT(DISTINCT fi.product_id) AS unique_products
        FROM analytics.fact_order_items fi
        JOIN analytics.dim_products p ON fi.product_id = p.product_id
        GROUP BY TO_CHAR(fi.order_date, 'YYYY-MM'), p.category
        ORDER BY year_month, category
    """)
    
    cursor.execute("SELECT COUNT(*) FROM agg.agg_monthly_category")
    count = cursor.fetchone()[0]
    
    elapsed = round(time.time() - start, 2)
    print(f"   ✅ {count:,} rows in {elapsed}s")
    return count


def validate_aggregates(cursor):
    """Cross-check aggregate totals with fact table"""
    print("\n🔍 VALIDATING AGGREGATES vs FACT TABLES...")
    
    checks = [
        {
            "name": "Daily Revenue Total",
            "agg_sql": "SELECT ROUND(SUM(total_revenue), 2) FROM agg.agg_daily_revenue",
            "fact_sql": "SELECT ROUND(SUM(total_amount), 2) FROM analytics.fact_orders"
        },
        {
            "name": "Daily Order Count",
            "agg_sql": "SELECT SUM(order_count) FROM agg.agg_daily_revenue",
            "fact_sql": "SELECT COUNT(*) FROM analytics.fact_orders"
        },
        {
            "name": "Wide Fact Row Count",
            "agg_sql": "SELECT COUNT(*) FROM analytics.fact_orders_wide",
            "fact_sql": "SELECT COUNT(*) FROM analytics.fact_orders"
        }
    ]
    
    all_passed = True
    for check in checks:
        cursor.execute(check["agg_sql"])
        agg_val = cursor.fetchone()[0]
        
        cursor.execute(check["fact_sql"])
        fact_val = cursor.fetchone()[0]
        
        match = (agg_val == fact_val)
        status = "✅ MATCH" if match else "❌ MISMATCH"
        
        print(f"   {status}: {check['name']}")
        print(f"     Aggregate: {agg_val}")
        print(f"     Fact:      {fact_val}")
        
        if not match:
            all_passed = False
    
    return all_passed


def log_etl_job(cursor, batch_id, job_name, status, rows, start_time, error=None):
    """Record ETL job execution in audit log"""
    cursor.execute("""
        INSERT INTO audit.etl_job_log (
            job_name, batch_id, status, started_at, completed_at,
            rows_processed, source_system, target_table, error_message
        ) VALUES (%s, %s, %s, %s, GETDATE(), %s, %s, %s, %s)
    """, (
        job_name, batch_id, status, start_time,
        rows, 'redshift', 'derived_tables', error
    ))


def run_derived_rebuild():
    """Main execution: rebuild all derived tables"""
    batch_id = f"DERIVED-{time.strftime('%Y%m%d-%H%M%S')}"
    
    print("=" * 60)
    print(f"🚀 REBUILDING DERIVED TABLES — Batch: {batch_id}")
    print("=" * 60)
    
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    total_rows = 0
    
    try:
        # Rebuild in dependency order
        total_rows += rebuild_wide_fact(cursor, batch_id)
        total_rows += rebuild_daily_revenue(cursor, batch_id)
        total_rows += rebuild_monthly_category(cursor, batch_id)
        
        # Validate
        all_valid = validate_aggregates(cursor)
        
        # Update statistics for query optimizer
        print("\n📊 Running ANALYZE...")
        cursor.execute("ANALYZE analytics.fact_orders_wide")
        cursor.execute("ANALYZE agg.agg_daily_revenue")
        cursor.execute("ANALYZE agg.agg_monthly_category")
        print("   ✅ Statistics updated")
        # Log success
        log_etl_job(cursor, batch_id, "rebuild_derived_tables", "completed",
                    total_rows, start_time)
        
        status_msg = "HEALTHY" if all_valid else "VALIDATION WARNINGS"
        
        print(f"\n{'='*60}")
        print(f"✅ DERIVED TABLE REBUILD COMPLETE")
        print(f"   Batch ID:    {batch_id}")
        print(f"   Total rows:  {total_rows:,}")
        print(f"   Validation:  {status_msg}")
        print(f"{'='*60}")
        
    except Exception as e:
        log_etl_job(cursor, batch_id, "rebuild_derived_tables", "failed",
                    total_rows, start_time, str(e))
        print(f"\n❌ REBUILD FAILED: {e}")
        raise
    
    finally:
        conn.close()
        print("🔌 Connection closed")


if __name__ == "__main__":
    run_derived_rebuild()