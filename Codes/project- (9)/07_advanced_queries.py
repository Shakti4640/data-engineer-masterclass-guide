# file: 07_advanced_queries.py
# Purpose: Production-grade queries that combine ALL schema elements
# These queries demonstrate WHY the schema design matters

import pymysql
import time

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "etl_user",
    "password":[REDACTED:PASSWORD]5!",
    "database": "quickcart_db",
    "charset": "utf8mb4"
}


def timed_query(cursor, label, sql, params=None):
    """Execute and time a query"""
    print(f"\n{'='*70}")
    print(f"📊 {label}")
    print(f"{'='*70}")
    
    start = time.time()
    cursor.execute(sql, params)
    results = cursor.fetchall()
    elapsed_ms = round((time.time() - start) * 1000, 2)
    
    columns = [desc[0] for desc in cursor.description]
    
    # Print results
    for row in results[:10]:
        row_dict = dict(zip(columns, row))
        formatted = {k: str(v)[:40] for k, v in row_dict.items()}
        print(f"  {formatted}")
    
    if len(results) > 10:
        print(f"  ... and {len(results) - 10} more rows")
    
    print(f"\n  ⏱️  {len(results)} rows in {elapsed_ms} ms")
    return results


def run_advanced_queries():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # -----------------------------------------------------------------
    # QUERY 1: Customer 360 — everything about one customer
    # Joins: customers + addresses + orders + reviews
    # Real use case: customer support dashboard
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "CUSTOMER 360 VIEW — Complete customer profile",
        """
        SELECT 
            c.customer_id,
            c.name,
            c.email,
            c.tier,
            c.signup_date,
            COUNT(DISTINCT o.order_id) AS total_orders,
            COALESCE(ROUND(SUM(o.total_amount), 2), 0) AS lifetime_value,
            COALESCE(ROUND(AVG(o.total_amount), 2), 0) AS avg_order_value,
            COUNT(DISTINCT pr.review_id) AS reviews_written,
            COALESCE(ROUND(AVG(pr.rating), 1), 0) AS avg_rating_given,
            (SELECT COUNT(*) FROM customer_addresses ca 
             WHERE ca.customer_id = c.customer_id) AS address_count
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN product_reviews pr ON c.customer_id = pr.customer_id
        WHERE c.customer_id = %s
        GROUP BY c.customer_id, c.name, c.email, c.tier, c.signup_date
        """,
        ("CUST-000001",)
    )
    
    # -----------------------------------------------------------------
    # QUERY 2: Product Performance Scorecard
    # Combines: sales data + review data
    # Real use case: product manager dashboard
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "PRODUCT SCORECARD — Sales + Reviews combined",
        """
        SELECT 
            p.product_id,
            p.name,
            p.category,
            p.price AS list_price,
            COALESCE(sales.units_sold, 0) AS units_sold,
            COALESCE(sales.revenue, 0) AS revenue,
            COALESCE(reviews.review_count, 0) AS review_count,
            COALESCE(reviews.avg_rating, 0) AS avg_rating,
            CASE 
                WHEN COALESCE(sales.units_sold, 0) > 50 
                     AND COALESCE(reviews.avg_rating, 0) >= 4.0 
                THEN 'STAR'
                WHEN COALESCE(sales.units_sold, 0) > 20 
                THEN 'SOLID'
                WHEN COALESCE(sales.units_sold, 0) > 0 
                THEN 'LOW'
                ELSE 'NO SALES'
            END AS performance_tier
        FROM products p
        LEFT JOIN (
            SELECT 
                oi.product_id,
                SUM(oi.quantity) AS units_sold,
                ROUND(SUM(oi.line_total), 2) AS revenue
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.status IN ('completed', 'shipped')
            GROUP BY oi.product_id
        ) sales ON p.product_id = sales.product_id
        LEFT JOIN (
            SELECT 
                product_id,
                COUNT(*) AS review_count,
                ROUND(AVG(rating), 2) AS avg_rating
            FROM product_reviews
            GROUP BY product_id
        ) reviews ON p.product_id = reviews.product_id
        WHERE p.is_active = TRUE
        ORDER BY revenue DESC
        LIMIT 15
        """
    )
    
    # -----------------------------------------------------------------
    # QUERY 3: Order Journey Timeline
    # Uses: order_status_history for full lifecycle tracking
    # Real use case: customer support investigating delayed order
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "ORDER JOURNEY — Full status timeline for an order",
        """
        SELECT 
            o.order_id,
            o.customer_id,
            c.name AS customer_name,
            o.total_amount,
            o.order_date,
            osh.old_status,
            osh.new_status,
            osh.changed_by,
            osh.changed_at,
            TIMESTAMPDIFF(HOUR, 
                LAG(osh.changed_at) OVER (
                    PARTITION BY osh.order_id ORDER BY osh.changed_at
                ),
                osh.changed_at
            ) AS hours_since_last_change
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_status_history osh ON o.order_id = osh.order_id
        WHERE o.order_id = (
            SELECT order_id FROM order_status_history 
            GROUP BY order_id 
            HAVING COUNT(*) >= 3 
            LIMIT 1
        )
        ORDER BY osh.changed_at
        """
    )
    
    # -----------------------------------------------------------------
    # QUERY 4: Cohort Analysis — Revenue by signup month
    # Uses: customers.signup_date + orders aggregation
    # Real use case: marketing ROI by acquisition month
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "COHORT ANALYSIS — Revenue by customer signup month",
        """
        SELECT 
            DATE_FORMAT(c.signup_date, '%%Y-%%m') AS cohort_month,
            COUNT(DISTINCT c.customer_id) AS customers,
            COUNT(DISTINCT o.order_id) AS total_orders,
            ROUND(COUNT(DISTINCT o.order_id) / COUNT(DISTINCT c.customer_id), 1) 
                AS orders_per_customer,
            ROUND(COALESCE(SUM(o.total_amount), 0), 2) AS total_revenue,
            ROUND(
                COALESCE(SUM(o.total_amount), 0) / COUNT(DISTINCT c.customer_id), 2
            ) AS revenue_per_customer
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY DATE_FORMAT(c.signup_date, '%%Y-%%m')
        ORDER BY cohort_month
        """
    )
    
    # -----------------------------------------------------------------
    # QUERY 5: CDC Query — Changed rows since last extraction
    # Uses: _etl_watermarks + updated_at columns
    # Real use case: Glue job extracts only new/changed data (Project 42)
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "CDC EXTRACTION — Customers changed since last ETL run",
        """
        SELECT 
            c.*
        FROM customers c
        WHERE c.updated_at > (
            SELECT last_extracted 
            FROM _etl_watermarks 
            WHERE table_name = 'customers' 
              AND pipeline_name = 'glue_s3_export'
        )
        ORDER BY c.updated_at
        LIMIT 100
        """
    )
    
    # -----------------------------------------------------------------
    # QUERY 6: Tier Upgrade Candidates
    # Uses: customer tier + order aggregation + review data
    # Real use case: automated tier management
    # -----------------------------------------------------------------
    timed_query(
        cursor,
        "TIER UPGRADE CANDIDATES — Silver customers ready for Gold",
        """
        SELECT 
            c.customer_id,
            c.name,
            c.tier AS current_tier,
            COUNT(DISTINCT o.order_id) AS order_count,
            ROUND(SUM(o.total_amount), 2) AS total_spent,
            COALESCE(rv.avg_rating, 0) AS avg_review_rating,
            CASE
                WHEN SUM(o.total_amount) > 1000 
                     AND COUNT(DISTINCT o.order_id) > 5 THEN 'STRONG'
                WHEN SUM(o.total_amount) > 500 THEN 'MODERATE'
                ELSE 'WEAK'
            END AS upgrade_strength
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN (
            SELECT customer_id, ROUND(AVG(rating), 1) AS avg_rating
            FROM product_reviews
            GROUP BY customer_id
        ) rv ON c.customer_id = rv.customer_id
        WHERE c.tier = 'silver'
          AND o.status = 'completed'
          AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
        GROUP BY c.customer_id, c.name, c.tier, rv.avg_rating
        HAVING total_spent > 500
        ORDER BY total_spent DESC
        LIMIT 20
        """
    )
    
    conn.close()
    print("\n🔌 Connection closed")
    
    print(f"""
{'='*70}
💡 KEY INSIGHT — WHY SCHEMA DESIGN MATTERS:
{'='*70}

Every query above was possible ONLY because:
1. Proper FOREIGN KEYS enabled reliable JOINs
2. AUDIT COLUMNS (created_at, updated_at) enabled CDC query (#5)
3. HISTORY TABLE (order_status_history) enabled timeline query (#3)
4. SEPARATE ADDRESS TABLE enabled Customer 360 (#1)
5. PROPER INDEXES eliminated full scans on all queries
6. ENUM types prevented bad data from entering

If we had stored everything in ONE big CSV file (Project 1):
→ Query #1 would require downloading 3 CSVs + custom Python joins
→ Query #3 would be IMPOSSIBLE — no status history in CSV
→ Query #5 would require full file diff — hours instead of ms
→ Query #6 would require loading everything into memory

THIS is the difference between data storage and data engineering.
    """)


if __name__ == "__main__":
    run_advanced_queries()