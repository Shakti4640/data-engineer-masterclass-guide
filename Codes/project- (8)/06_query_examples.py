# file: 06_query_examples.py
# Purpose: Demonstrate the queries that CSVs COULDN'T do
# These are the exact questions from the problem statement

import pymysql
import time

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "etl_user",            # Read-only user — as referenced in Project 20, 31
    "password":[REDACTED:PASSWORD]5!",
    "database": "quickcart_db",
    "charset": "utf8mb4"
}


def timed_query(cursor, label, sql, params=None):
    """Execute query with timing — shows performance"""
    print(f"\n{'='*60}")
    print(f"📊 {label}")
    print(f"{'='*60}")
    print(f"SQL: {sql.strip()[:200]}...")
    
    start = time.time()
    cursor.execute(sql, params)
    results = cursor.fetchall()
    elapsed_ms = round((time.time() - start) * 1000, 2)
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Print header
    header = " | ".join(f"{col:<20}" for col in columns)
    print(f"\n{header}")
    print("-" * len(header))
    
    # Print rows
    for row in results[:20]:  # Limit display to 20 rows
        row_str = " | ".join(f"{str(val):<20}" for val in row)
        print(row_str)
    
    if len(results) > 20:
        print(f"... and {len(results) - 20} more rows")
    
    print(f"\n⏱️  {len(results)} rows in {elapsed_ms} ms")
    return results


def run_queries():
    connection = pymysql.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    # ---------------------------------------------------------------
    # QUERY 1: Customer Support — Look up specific order
    # "I can't look up order #ORD-20250115-000437 without 
    #  downloading the whole CSV and searching in Excel"
    # ---------------------------------------------------------------
    timed_query(
        cursor,
        "CUSTOMER SUPPORT: Look up specific order with customer details",
        """
        SELECT 
            o.order_id,
            o.order_date,
            o.total_amount,
            o.status,
            c.name AS customer_name,
            c.email,
            c.tier
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        WHERE o.order_id = %s
        """,
        ("ORD-20250115-000001",)
    )
    # WITH INDEX: < 1ms (B+Tree lookup)
    # WITHOUT INDEX (CSV): scan entire file — seconds to minutes
    
    # ---------------------------------------------------------------
    # QUERY 2: Marketing — Gold customers with orders > $500
    # "How many Gold-tier customers placed orders above $500 last month?"
    # ---------------------------------------------------------------
    timed_query(
        cursor,
        "MARKETING: Gold customers with high-value orders",
        """
        SELECT 
            c.customer_id,
            c.name,
            c.city,
            COUNT(o.order_id) AS order_count,
            ROUND(SUM(o.total_amount), 2) AS total_spent,
            ROUND(AVG(o.total_amount), 2) AS avg_order_value
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        WHERE c.tier = 'gold'
          AND o.total_amount > 500
          AND o.order_date >= '2025-01-01'
          AND o.order_date < '2025-07-01'
        GROUP BY c.customer_id, c.name, c.city
        HAVING COUNT(o.order_id) >= 1
        ORDER BY total_spent DESC
        LIMIT 20
        """
    )
    
    # ---------------------------------------------------------------
    # QUERY 3: Show EXPLAIN plan — prove index is used
    # ---------------------------------------------------------------
    print(f"\n{'='*60}")
    print("🔍 EXPLAIN PLAN: How MariaDB executes the order lookup")
    print(f"{'='*60}")
    cursor.execute("""
        EXPLAIN 
        SELECT * FROM orders 
        WHERE order_id = 'ORD-20250115-000001'
    """)
    columns = [desc[0] for desc in cursor.description]
    for row in cursor.fetchall():
        for col, val in zip(columns, row):
            print(f"   {col}: {val}")
    # Expected: type=const, key=PRIMARY → index lookup, 1 row examined
    
    # ---------------------------------------------------------------
    # QUERY 4: Revenue by category — requires JOIN across 3 tables
    # ---------------------------------------------------------------
    timed_query(
        cursor,
        "ANALYTICS: Revenue by product category",
        """
        SELECT 
            p.category,
            COUNT(DISTINCT o.order_id) AS num_orders,
            SUM(oi.quantity) AS units_sold,
            ROUND(SUM(oi.line_total), 2) AS revenue
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.status = 'completed'
        GROUP BY p.category
        ORDER BY revenue DESC
        """
    )
    
    # ---------------------------------------------------------------
    # QUERY 5: Daily order trend — time series
    # ---------------------------------------------------------------
    timed_query(
        cursor,
        "ANALYTICS: Daily order volume and revenue",
        """
        SELECT 
            order_date,
            COUNT(*) AS num_orders,
            ROUND(SUM(total_amount), 2) AS daily_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value
        FROM orders
        WHERE order_date >= '2025-01-01'
        GROUP BY order_date
        ORDER BY order_date
        LIMIT 30
        """
    )
    
    # ---------------------------------------------------------------
    # QUERY 6: Show database size
    # ---------------------------------------------------------------
    timed_query(
        cursor,
        "ADMIN: Database size summary",
        """
        SELECT 
            TABLE_NAME,
            TABLE_ROWS,
            ROUND(DATA_LENGTH / 1024 / 1024, 2) AS data_mb,
            ROUND(INDEX_LENGTH / 1024 / 1024, 2) AS index_mb,
            ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS total_mb
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'quickcart_db'
        ORDER BY total_mb DESC
        """
    )
    
    connection.close()
    print("\n🔌 Connection closed")
    
    print("\n" + "=" * 60)
    print("💡 KEY INSIGHT:")
    print("=" * 60)
    print("""
    CSV Approach (Project 1):
    → Download 50 MB file from S3
    → Open in Excel or parse with Python
    → Search manually — minutes per query
    → No JOINs across files without custom code
    → No concurrent access — one person at a time

    MariaDB Approach (This Project):
    → Indexed lookup by order_id: < 1 ms
    → JOIN across 3 tables: < 50 ms
    → GROUP BY with aggregation: < 100 ms
    → 100 users querying simultaneously: no problem
    → ACID transactions: no half-written data
    
    THIS is why relational databases exist.
    CSV files are for TRANSPORT, not for QUERYING.
    """)


if __name__ == "__main__":
    run_queries()