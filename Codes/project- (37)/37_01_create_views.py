# file: 37_01_create_views.py
# Purpose: Create standardized Athena views for analyst self-service
# Run from: Your local machine or CI/CD pipeline

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

VIEWS_DATABASE = "reporting_views"
SILVER_DATABASE = "silver_quickcart"
GOLD_DATABASE = "gold_quickcart"

athena = boto3.client("athena", region_name=AWS_REGION)
glue = boto3.client("glue", region_name=AWS_REGION)


def run_ddl(sql, database=VIEWS_DATABASE, description=""):
    """Execute DDL statement on Athena and wait"""
    if description:
        print(f"   📝 {description}")

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )
    qid = response["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state == "SUCCEEDED":
        print(f"   ✅ {description or 'Query'}: succeeded")
    else:
        error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        print(f"   ❌ {description or 'Query'}: FAILED — {error[:150]}")

    return state


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create Views Database
# ═══════════════════════════════════════════════════════════════════
def setup_views_database():
    """Create dedicated database for all reporting views"""
    print("\n📚 Setting up views database...")

    try:
        glue.create_database(
            DatabaseInput={
                "Name": VIEWS_DATABASE,
                "Description": (
                    "Standardized reporting views for analyst self-service. "
                    "All views use consistent business metric definitions."
                )
            }
        )
        print(f"   ✅ Created: {VIEWS_DATABASE}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Already exists: {VIEWS_DATABASE}")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Base Views (Pre-Joined, Filtered)
# ═══════════════════════════════════════════════════════════════════
def create_base_views():
    """
    BASE VIEWS: Hide JOINs and standard filters
    
    These are the foundation — other views can build on them
    Analysts query these for ad-hoc analysis
    """
    print("\n🔨 Creating base views...")

    # ── VIEW 1: Completed Order Details ──
    # Most commonly needed: all completed orders with customer + product info
    # Points to silver pre-joined table (Project 34 already joined these)
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_completed_order_details AS
        SELECT
            order_id,
            customer_id,
            customer_name,
            customer_email,
            customer_city,
            customer_tier,
            product_id,
            product_name,
            product_category,
            price_tier,
            quantity,
            unit_price,
            revenue,
            discount_pct,
            order_date,
            order_year,
            order_month,
            order_day_of_week,
            -- Partition columns included for pruning
            year,
            month,
            day
        FROM {SILVER_DATABASE}.order_details
        WHERE status = 'completed'
    """, description="v_completed_order_details (silver — completed orders only)")

    # ── VIEW 2: All Orders (including cancelled/refunded) ──
    # For operations team that needs to see all statuses
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_all_order_details AS
        SELECT
            order_id,
            customer_id,
            customer_name,
            customer_tier,
            product_id,
            product_name,
            product_category,
            quantity,
            unit_price,
            revenue,
            status,
            order_date,
            order_year,
            order_month,
            year,
            month,
            day
        FROM {SILVER_DATABASE}.order_details
    """, description="v_all_order_details (silver — all statuses)")

    # ── VIEW 3: Customer 360 Profile ──
    # Full customer view with RFM data
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_customer_360 AS
        SELECT
            c.customer_id,
            c.name AS customer_name,
            c.email,
            c.city,
            c.tier,
            c.signup_date,
            c.tenure_days,
            c.email_valid,
            r.recency_days,
            r.frequency AS order_count,
            r.monetary AS lifetime_value,
            r.avg_order_value,
            r.first_order_date,
            r.last_order_date,
            r.rfm_segment
        FROM {SILVER_DATABASE}.clean_customers c
        LEFT JOIN {SILVER_DATABASE}.customer_rfm r
            ON c.customer_id = r.customer_id
    """, description="v_customer_360 (silver — full customer profile)")

    # ── VIEW 4: Active Products ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_active_products AS
        SELECT
            product_id,
            name AS product_name,
            category,
            price,
            price_tier,
            stock_quantity,
            stock_status,
            is_active
        FROM {SILVER_DATABASE}.clean_products
        WHERE is_active = 'true'
    """, description="v_active_products (silver — active only)")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Gold-Backed Views (Fast + Cheap)
# ═══════════════════════════════════════════════════════════════════
def create_gold_views():
    """
    GOLD VIEWS: Point to materialized gold tables (Project 36)
    
    These are VIRTUALLY FREE to query:
    → Gold table: 3 KB → scan cost: $0.00005
    → Analysts don't know/care that it's gold vs silver
    → They just query: SELECT * FROM v_revenue_summary
    
    WHY wrap gold tables in views:
    → Standardized naming (v_ prefix)
    → Can add business logic on top of gold data
    → Can switch underlying source without analyst knowing
    → Documentation via view definition
    """
    print("\n🥇 Creating gold-backed views (fast + cheap)...")

    # ── VIEW 5: Revenue Summary (backed by gold) ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_revenue_summary AS
        SELECT
            product_category AS category,
            month,
            year,
            order_count,
            unique_customers,
            total_revenue,
            avg_order_value,
            -- Derived: revenue per customer
            ROUND(total_revenue / NULLIF(unique_customers, 0), 2)
                AS revenue_per_customer
        FROM {GOLD_DATABASE}.revenue_by_category_month
    """, description="v_revenue_summary (gold-backed — virtually free)")

    # ── VIEW 6: Customer Segments (backed by gold) ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_customer_segments AS
        SELECT
            rfm_segment AS segment,
            customer_tier AS tier,
            customer_count,
            avg_lifetime_value,
            avg_frequency,
            avg_recency AS avg_days_since_last_order,
            total_segment_revenue,
            -- Derived: percentage of total revenue
            ROUND(
                total_segment_revenue / NULLIF(SUM(total_segment_revenue) OVER (), 0) * 100
            , 1) AS revenue_share_pct
        FROM {GOLD_DATABASE}.customer_segments_summary
    """, description="v_customer_segments (gold-backed — virtually free)")

    # ── VIEW 7: Product Leaderboard (backed by gold) ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_product_leaderboard AS
        SELECT
            product_name,
            product_category AS category,
            price_tier,
            times_ordered,
            unique_buyers,
            total_units_sold,
            total_revenue,
            avg_revenue_per_order,
            -- Derived: rank within category
            ROW_NUMBER() OVER (
                PARTITION BY product_category 
                ORDER BY total_revenue DESC
            ) AS rank_in_category
        FROM {GOLD_DATABASE}.product_performance
    """, description="v_product_leaderboard (gold-backed — virtually free)")

    # ── VIEW 8: Conversion Trend (backed by gold) ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_conversion_trend AS
        SELECT
            snapshot_date AS date,
            total_orders,
            completed_orders,
            cancelled_orders,
            completion_rate,
            total_revenue,
            avg_order_value,
            unique_customers,
            -- Derived: daily change
            total_revenue - LAG(total_revenue) OVER (ORDER BY snapshot_date)
                AS revenue_change,
            completion_rate - LAG(completion_rate) OVER (ORDER BY snapshot_date)
                AS completion_rate_change
        FROM {GOLD_DATABASE}.daily_conversion_metrics
    """, description="v_conversion_trend (gold-backed — virtually free)")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Metric Views (Standardized Business Calculations)
# ═══════════════════════════════════════════════════════════════════
def create_metric_views():
    """
    METRIC VIEWS: Enforce standard calculation definitions
    
    SOLVES: "Why does every report show different revenue numbers?"
    ANSWER: Everyone uses these views → same calculation → same number
    """
    print("\n📐 Creating metric views (standardized calculations)...")

    # ── VIEW 9: Standard Revenue Metrics ──
    # THE definitive revenue calculation for the company
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_standard_revenue_metrics AS
        SELECT
            order_date,
            product_category,
            customer_tier,

            -- OFFICIAL REVENUE DEFINITION:
            -- Sum of (quantity × unit_price) for COMPLETED orders only
            -- This is THE number that goes in financial reports
            COUNT(DISTINCT order_id) AS completed_orders,
            ROUND(SUM(revenue), 2) AS gross_revenue,

            -- Average metrics
            ROUND(AVG(revenue), 2) AS avg_order_value,
            ROUND(STDDEV(revenue), 2) AS stddev_order_value,

            -- Customer metrics
            COUNT(DISTINCT customer_id) AS unique_customers,
            ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT customer_id), 0), 2)
                AS revenue_per_customer,

            -- Volume metrics
            SUM(quantity) AS total_units,
            ROUND(AVG(quantity), 1) AS avg_units_per_order,

            -- Partition columns for pruning
            year,
            month

        FROM {SILVER_DATABASE}.order_details
        WHERE status = 'completed'
          AND revenue > 0
        GROUP BY order_date, product_category, customer_tier, year, month
    """, description="v_standard_revenue_metrics (THE official revenue definition)")

    # ── VIEW 10: Customer Health Metrics ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_customer_health AS
        SELECT
            rfm_segment,
            COUNT(*) AS customer_count,
            
            -- Active: ordered in last 90 days
            COUNT(CASE WHEN recency_days <= 90 THEN 1 END) AS active_customers,
            -- At risk: no order in 90-180 days
            COUNT(CASE WHEN recency_days BETWEEN 91 AND 180 THEN 1 END) AS at_risk_customers,
            -- Churned: no order in 180+ days
            COUNT(CASE WHEN recency_days > 180 THEN 1 END) AS churned_customers,
            
            -- Percentages
            ROUND(CAST(COUNT(CASE WHEN recency_days <= 90 THEN 1 END) AS DOUBLE) 
                  / COUNT(*) * 100, 1) AS active_pct,
            ROUND(CAST(COUNT(CASE WHEN recency_days > 180 THEN 1 END) AS DOUBLE) 
                  / COUNT(*) * 100, 1) AS churned_pct,
            
            -- Value metrics
            ROUND(AVG(monetary), 2) AS avg_lifetime_value,
            ROUND(SUM(monetary), 2) AS total_segment_value

        FROM {SILVER_DATABASE}.customer_rfm
        GROUP BY rfm_segment
    """, description="v_customer_health (standard customer health definitions)")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Security Views (Column Restriction)
# ═══════════════════════════════════════════════════════════════════
def create_security_views():
    """
    SECURITY VIEWS: Restrict sensitive columns
    
    NOTE: This is NOT true security — analyst can still query source tables
    For real column-level security: use Lake Formation (Project 74)
    These views are CONVENIENCE — guide analysts to use safe columns
    """
    print("\n🔒 Creating security views (column-restricted)...")

    # ── VIEW 11: Orders without PII (for junior analysts) ──
    # No customer email, no customer name
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_orders_no_pii AS
        SELECT
            order_id,
            customer_id,
            -- customer_name EXCLUDED (PII)
            -- customer_email EXCLUDED (PII)
            customer_city,
            customer_tier,
            product_id,
            product_name,
            product_category,
            quantity,
            unit_price,
            revenue,
            status,
            order_date,
            year, month, day
        FROM {SILVER_DATABASE}.order_details
    """, description="v_orders_no_pii (no email, no name — for junior analysts)")

    # ── VIEW 12: Customers without contact info ──
    run_ddl(f"""
        CREATE OR REPLACE VIEW {VIEWS_DATABASE}.v_customers_anonymous AS
        SELECT
            customer_id,
            -- name EXCLUDED
            -- email EXCLUDED
            city,
            tier,
            signup_date,
            tenure_days
        FROM {SILVER_DATABASE}.clean_customers
    """, description="v_customers_anonymous (no contact info)")


# ═══════════════════════════════════════════════════════════════════
# STEP 6: List All Created Views
# ═══════════════════════════════════════════════════════════════════
def list_views():
    """List all views in the reporting_views database"""
    print(f"\n📋 Views in {VIEWS_DATABASE}:")
    print("-" * 70)

    response = glue.get_tables(DatabaseName=VIEWS_DATABASE)

    for table in response.get("TableList", []):
        table_type = table.get("TableType", "")
        name = table.get("Name", "")
        description = table.get("Parameters", {}).get("comment", "")

        if table_type == "VIRTUAL_VIEW":
            print(f"   📊 {name}")
            if description:
                print(f"      {description}")

    print("-" * 70)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 70)
    print("🚀 CREATING ATHENA VIEWS FOR REPORTING")
    print("=" * 70)

    setup_views_database()
    create_base_views()
    create_gold_views()
    create_metric_views()
    create_security_views()
    list_views()

    print("\n" + "=" * 70)
    print("✅ ALL VIEWS CREATED")
    print("=" * 70)