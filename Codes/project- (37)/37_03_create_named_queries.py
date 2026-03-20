# file: 37_03_create_named_queries.py
# Purpose: Create shared query library for analysts
# Named queries appear in Athena Console under "Saved queries"

import boto3

AWS_REGION = "us-east-2"
athena = boto3.client("athena", region_name=AWS_REGION)

VIEWS_DB = "reporting_views"


def create_named_query(name, description, database, query, workgroup="primary"):
    """Create a named query in Athena"""
    print(f"   📝 {name}")

    try:
        athena.create_named_query(
            Name=name,
            Description=description,
            Database=database,
            QueryString=query,
            WorkGroup=workgroup
        )
        print(f"      ✅ Created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"      ℹ️  Already exists")
        else:
            print(f"      ❌ Error: {e}")


def create_all_named_queries():
    """Create shared query library"""
    print("=" * 60)
    print("📚 CREATING NAMED QUERY LIBRARY")
    print("=" * 60)

    # ── Category 1: Revenue Analysis ──
    print("\n📊 Revenue Analysis Queries:")

    create_named_query(
        "Revenue: Monthly by Category",
        "Monthly revenue breakdown by product category. Filter by year.",
        VIEWS_DB,
        """-- Monthly Revenue by Product Category
-- Change the year filter as needed
SELECT category, month, year,
       order_count, unique_customers,
       total_revenue, avg_order_value,
       revenue_per_customer
FROM v_revenue_summary
WHERE year = '2025'  -- ← Change this
ORDER BY total_revenue DESC"""
    )

    create_named_query(
        "Revenue: Top N Products",
        "Top N products by total revenue. Change LIMIT to adjust.",
        VIEWS_DB,
        """-- Top Products by Revenue
SELECT product_name, category, price_tier,
       times_ordered, unique_buyers,
       total_revenue, avg_revenue_per_order,
       rank_in_category
FROM v_product_leaderboard
ORDER BY total_revenue DESC
LIMIT 20  -- ← Change this"""
    )

    create_named_query(
        "Revenue: Year-over-Year Comparison",
        "Compare revenue between two years by category.",
        VIEWS_DB,
        """-- Year-over-Year Revenue Comparison
SELECT 
    category,
    SUM(CASE WHEN year = '2024' THEN total_revenue ELSE 0 END) AS revenue_2024,
    SUM(CASE WHEN year = '2025' THEN total_revenue ELSE 0 END) AS revenue_2025,
    ROUND(
        (SUM(CASE WHEN year = '2025' THEN total_revenue ELSE 0 END)
         - SUM(CASE WHEN year = '2024' THEN total_revenue ELSE 0 END))
        / NULLIF(SUM(CASE WHEN year = '2024' THEN total_revenue ELSE 0 END), 0) * 100
    , 1) AS yoy_growth_pct
FROM v_revenue_summary
GROUP BY category
ORDER BY yoy_growth_pct DESC"""
    )

    # ── Category 2: Customer Analysis ──
    print("\n👥 Customer Analysis Queries:")

    create_named_query(
        "Customers: Segment Distribution",
        "Customer count and value by RFM segment.",
        VIEWS_DB,
        """-- Customer Segment Distribution
SELECT segment, tier,
       customer_count,
       avg_lifetime_value,
       avg_frequency,
       avg_days_since_last_order,
       total_segment_revenue,
       revenue_share_pct
FROM v_customer_segments
ORDER BY total_segment_revenue DESC"""
    )

    create_named_query(
        "Customers: Health Dashboard",
        "Active, at-risk, and churned customer counts by segment.",
        VIEWS_DB,
        """-- Customer Health Dashboard
SELECT rfm_segment,
       customer_count,
       active_customers,
       at_risk_customers,
       churned_customers,
       active_pct,
       churned_pct,
       avg_lifetime_value,
       total_segment_value
FROM v_customer_health
ORDER BY total_segment_value DESC"""
    )

    create_named_query(
        "Customers: High-Value at Risk",
        "High-value customers who haven't ordered recently.",
        VIEWS_DB,
        """-- High-Value Customers at Risk of Churning
SELECT customer_id, customer_name, city, tier,
       lifetime_value, order_count,
       recency_days AS days_since_last_order,
       rfm_segment
FROM v_customer_360
WHERE lifetime_value > 500           -- ← High value threshold
  AND recency_days > 90              -- ← Days without order
ORDER BY lifetime_value DESC
LIMIT 100"""
    )

    # ── Category 3: Operations ──
    print("\n⚙️  Operations Queries:")

    create_named_query(
        "Operations: Conversion Trend",
        "Daily conversion rate and revenue trend.",
        VIEWS_DB,
        """-- Daily Conversion Trend
SELECT date,
       total_orders, completed_orders, cancelled_orders,
       completion_rate,
       total_revenue, avg_order_value,
       revenue_change,
       completion_rate_change
FROM v_conversion_trend
ORDER BY date DESC
LIMIT 30  -- ← Last N days"""
    )

    create_named_query(
        "Operations: Daily Summary by City",
        "Today's order volume by city and status.",
        VIEWS_DB,
        """-- Daily Operations Summary
-- Change the date filter
SELECT status, product_category, customer_city,
       order_count, total_revenue, total_items
FROM v_all_order_details
WHERE order_date = DATE '2025-01-15'  -- ← Change this
GROUP BY status, product_category, customer_city
ORDER BY total_revenue DESC"""
    )

    # ── Category 4: Data Quality ──
    print("\n🔍 Data Quality Queries:")

    create_named_query(
        "DQ: NULL Check Across Views",
        "Check for NULL values in critical columns across all views.",
        VIEWS_DB,
        """-- Data Quality: NULL Check
SELECT
    'v_completed_order_details' AS view_name,
    COUNT(*) AS total_rows,
    COUNT(customer_id) AS has_customer,
    COUNT(product_name) AS has_product_name,
    COUNT(revenue) AS has_revenue,
    COUNT(*) - COUNT(product_name) AS missing_product_name
FROM v_completed_order_details
WHERE year = '2025' AND month = '01'"""
    )

    create_named_query(
        "DQ: Row Count Comparison",
        "Compare row counts between bronze, silver, and gold layers.",
        VIEWS_DB,
        """-- Layer-by-Layer Row Count Comparison
SELECT 'bronze_orders' AS layer, COUNT(*) AS rows FROM bronze_mariadb.orders
UNION ALL
SELECT 'silver_order_details', COUNT(*) FROM silver_quickcart.order_details
UNION ALL
SELECT 'silver_completed', COUNT(*) FROM silver_quickcart.order_details WHERE status = 'completed'
UNION ALL
SELECT 'gold_revenue_summary', COUNT(*) FROM gold_quickcart.revenue_by_category_month"""
    )

    # ── Print Query Library ──
    print(f"\n{'=' * 60}")
    print("📚 NAMED QUERY LIBRARY CREATED")
    print(f"{'=' * 60}")
    print(f"""
   Access in Athena Console:
   → Open Athena → "Saved queries" tab → see all named queries
   → Click any query → opens in editor → modify and run

   Access via API:
   → athena.list_named_queries() → get all query IDs
   → athena.get_named_query(NamedQueryId=id) → get SQL

   Naming Convention:
   → "Revenue: ..."     — revenue analysis queries
   → "Customers: ..."   — customer analysis queries
   → "Operations: ..."  — operational monitoring
   → "DQ: ..."          — data quality checks
    """)


if __name__ == "__main__":
    create_all_named_queries()
    print("\n🏁 DONE")