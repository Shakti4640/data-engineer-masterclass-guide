# file: 03_create_named_queries.py
# Pre-built queries that analysts can find and reuse in Athena console
# Prevents reinventing the wheel and ensures consistent patterns

import boto3

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"


def create_named_queries():
    """
    Named queries appear in Athena console under "Saved Queries"
    Analysts can click to load and run — no typing needed
    """
    athena_client = boto3.client("athena", region_name=REGION)

    queries = [
        # --- FINANCE QUERIES ---
        {
            "name": "📊 Daily Revenue Summary",
            "description": "Revenue, order count, and avg order value per day",
            "workgroup": "finance_team",
            "sql": """-- Daily Revenue Summary
SELECT 
    order_date,
    COUNT(*) as total_orders,
    COUNT(DISTINCT customer_id) as unique_customers,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(AVG(total_amount), 2) as avg_order_value
FROM raw_orders
GROUP BY order_date
ORDER BY order_date DESC
LIMIT 30;"""
        },
        {
            "name": "📊 Revenue by Status",
            "description": "Breakdown of revenue and counts by order status",
            "workgroup": "finance_team",
            "sql": """-- Revenue by Order Status
SELECT 
    status,
    COUNT(*) as order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total,
    ROUND(SUM(total_amount), 2) as revenue,
    ROUND(AVG(total_amount), 2) as avg_amount
FROM raw_orders
GROUP BY status
ORDER BY revenue DESC;"""
        },

        # --- MARKETING QUERIES ---
        {
            "name": "🎯 Customer Tier Distribution",
            "description": "Count and percentage of customers by tier",
            "workgroup": "marketing_team",
            "sql": """-- Customer Tier Distribution
SELECT 
    tier,
    COUNT(*) as customer_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM raw_customers
GROUP BY tier
ORDER BY customer_count DESC;"""
        },
        {
            "name": "🎯 Top Spending Customers",
            "description": "Top 20 customers by total spend (JOIN example)",
            "workgroup": "marketing_team",
            "sql": """-- Top 20 Customers by Revenue
SELECT 
    c.customer_id,
    c.name,
    c.tier,
    c.city,
    COUNT(o.order_id) as order_count,
    ROUND(SUM(o.total_amount), 2) as total_spent,
    ROUND(AVG(o.total_amount), 2) as avg_order
FROM raw_orders o
JOIN raw_customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.tier, c.city
ORDER BY total_spent DESC
LIMIT 20;"""
        },

        # --- PRODUCT QUERIES ---
        {
            "name": "📦 Product Category Performance",
            "description": "Revenue and units sold by product category",
            "workgroup": "product_team",
            "sql": """-- Category Performance
SELECT 
    p.category,
    COUNT(DISTINCT p.product_id) as products,
    COUNT(o.order_id) as orders,
    SUM(o.quantity) as units_sold,
    ROUND(SUM(o.total_amount), 2) as revenue,
    ROUND(AVG(o.total_amount), 2) as avg_order_value
FROM raw_orders o
JOIN raw_products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;"""
        },

        # --- ENGINEERING QUERIES ---
        {
            "name": "🔧 Data Quality Check — Orders",
            "description": "Check for nulls, duplicates, and value ranges",
            "workgroup": "data_engineering",
            "sql": """-- Data Quality Check: Orders
SELECT 
    'total_rows' as metric, CAST(COUNT(*) AS VARCHAR) as value FROM raw_orders
UNION ALL
SELECT 'null_order_id', CAST(COUNT(*) AS VARCHAR) FROM raw_orders WHERE order_id IS NULL
UNION ALL
SELECT 'null_customer_id', CAST(COUNT(*) AS VARCHAR) FROM raw_orders WHERE customer_id IS NULL
UNION ALL
SELECT 'null_amount', CAST(COUNT(*) AS VARCHAR) FROM raw_orders WHERE total_amount IS NULL
UNION ALL
SELECT 'negative_amount', CAST(COUNT(*) AS VARCHAR) FROM raw_orders WHERE total_amount < 0
UNION ALL
SELECT 'zero_quantity', CAST(COUNT(*) AS VARCHAR) FROM raw_orders WHERE quantity = 0
UNION ALL
SELECT 'distinct_statuses', CAST(COUNT(DISTINCT status) AS VARCHAR) FROM raw_orders
UNION ALL
SELECT 'min_date', CAST(MIN(order_date) AS VARCHAR) FROM raw_orders
UNION ALL
SELECT 'max_date', CAST(MAX(order_date) AS VARCHAR) FROM raw_orders;"""
        },
        {
            "name": "🔧 Table Scan Cost Comparison",
            "description": "Compare data scanned between CSV and Parquet tables",
            "workgroup": "data_engineering",
            "sql": """-- Run these separately and compare "Data scanned" metric

-- CSV table (expect ~1.5 GB scanned)
-- SELECT status, COUNT(*) FROM raw_orders GROUP BY status;

-- Parquet table (expect ~50 MB scanned)
SELECT status, COUNT(*) FROM curated_orders_snapshot GROUP BY status;"""
        }
    ]

    print("=" * 60)
    print("📝 CREATING NAMED QUERIES")
    print("=" * 60)

    for q in queries:
        try:
            response = athena_client.create_named_query(
                Name=q["name"],
                Description=q["description"],
                Database=DATABASE_NAME,
                QueryString=q["sql"],
                WorkGroup=q["workgroup"]
            )
            query_id = response["NamedQueryId"]
            print(f"   ✅ {q['name']}")
            print(f"      Workgroup: {q['workgroup']}, ID: {query_id}")
        except Exception as e:
            print(f"   ⚠️  {q['name']}: {e}")

    print(f"\n{'=' * 60}")
    print(f"✅ {len(queries)} named queries created")
    print(f"   Find them in Athena Console → Saved Queries tab")


if __name__ == "__main__":
    create_named_queries()