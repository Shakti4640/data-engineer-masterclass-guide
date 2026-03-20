# file: 19_03_verify_loaded_data.py
# Purpose: Query MariaDB to verify data was loaded correctly
# Compare with: Athena queries from Project 16 (same data, different engine)

import pymysql
import os

DB_CONFIG = {
    "host": os.environ.get("MARIADB_HOST", "localhost"),
    "port": int(os.environ.get("MARIADB_PORT", 3306)),
    "user": os.environ.get("MARIADB_USER", "quickcart_etl"),
    "password": os.environ.get("MARIADB_PASSWORD", "etl_password_123"),
    "database": "quickcart",
    "charset": "utf8mb4",
    "autocommit": True
}


def print_results(cursor, title):
    """Pretty-print query results"""
    rows = cursor.fetchall()
    if not rows:
        print("   (no results)")
        return

    # Get column names from cursor description
    columns = [desc[0] for desc in cursor.description]
    widths = [len(col) for col in columns]

    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    # Header
    header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    sep = "-+-".join("-" * w for w in widths)
    print(f"   {header}")
    print(f"   {sep}")

    # Rows
    for row in rows:
        line = " | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row))
        print(f"   {line}")

    print(f"   ({len(rows)} rows)")


def verify_data():
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ================================================================
    # QUERY 1: Table row counts
    # ================================================================
    print("=" * 70)
    print("QUERY 1: Row counts per table")
    print("=" * 70)

    tables = ["orders", "customers", "products", "load_metadata"]
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"   📋 {table:20s}: {count:>10,} rows")

    # ================================================================
    # QUERY 2: Preview orders (same as Athena Project 16 Query 1)
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 2: Preview orders (compare with Athena Project 16)")
    print("=" * 70)

    cursor.execute("SELECT * FROM orders LIMIT 5")
    print_results(cursor, "orders preview")

    # ================================================================
    # QUERY 3: Orders by status (same as Athena Project 16 Query 3)
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 3: Orders by status with revenue")
    print("=" * 70)

    cursor.execute("""
        SELECT
            status,
            COUNT(*) as order_count,
            ROUND(SUM(total_amount), 2) as total_revenue,
            ROUND(AVG(total_amount), 2) as avg_order_value
        FROM orders
        GROUP BY status
        ORDER BY order_count DESC
    """)
    print_results(cursor, "orders by status")

    # ================================================================
    # QUERY 4: Revenue by product category (JOIN — same as Athena P16 Q4)
    # ================================================================
    # NOTE: This query runs on MariaDB in <100ms
    #       Same query on Athena takes 3-5 seconds (S3 scan overhead)
    #       THIS is why we load into MariaDB for application queries
    print("\n" + "=" * 70)
    print("QUERY 4: Revenue by product category (JOIN)")
    print("=" * 70)

    import time
    start = time.time()

    cursor.execute("""
        SELECT
            p.category,
            COUNT(*) as order_count,
            ROUND(SUM(o.total_amount), 2) as total_revenue,
            ROUND(AVG(o.total_amount), 2) as avg_order_value
        FROM orders o
        INNER JOIN products p ON o.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_revenue DESC
    """)

    elapsed = (time.time() - start) * 1000
    print_results(cursor, "revenue by category")
    print(f"\n   ⚡ Query time: {elapsed:.0f}ms (vs Athena: ~4000ms)")
    print(f"   → This is why apps query MariaDB, not Athena")

    # ================================================================
    # QUERY 5: Customer tier distribution
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 5: Customer distribution by tier")
    print("=" * 70)

    cursor.execute("""
        SELECT
            tier,
            COUNT(*) as customer_count,
            COUNT(DISTINCT city) as unique_cities
        FROM customers
        GROUP BY tier
        ORDER BY customer_count DESC
    """)
    print_results(cursor, "customer tiers")

    # ================================================================
    # QUERY 6: Top 10 customers by spend (3-way JOIN)
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 6: Top 10 customers by spend (3-way JOIN)")
    print("=" * 70)

    cursor.execute("""
        SELECT
            c.customer_id,
            c.name,
            c.tier,
            c.city,
            COUNT(o.order_id) as total_orders,
            ROUND(SUM(o.total_amount), 2) as total_spend
        FROM orders o
        INNER JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.tier, c.city
        ORDER BY total_spend DESC
        LIMIT 10
    """)
    print_results(cursor, "top customers")

    # ================================================================
    # QUERY 7: Load metadata history
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 7: Load metadata history (audit trail)")
    print("=" * 70)

    cursor.execute("""
        SELECT
            source_file,
            target_table,
            rows_loaded,
            rows_rejected,
            load_status,
            started_at,
            completed_at
        FROM load_metadata
        ORDER BY started_at DESC
        LIMIT 10
    """)
    print_results(cursor, "load history")

    # ================================================================
    # QUERY 8: Data freshness check
    # ================================================================
    print("\n" + "=" * 70)
    print("QUERY 8: Data freshness — latest data per table")
    print("=" * 70)

    cursor.execute("""
        SELECT 'orders' as entity,
               MAX(order_date) as latest_date,
               COUNT(*) as total_rows
        FROM orders
        UNION ALL
        SELECT 'customers',
               MAX(signup_date),
               COUNT(*)
        FROM customers
        UNION ALL
        SELECT 'products',
               NULL,
               COUNT(*)
        FROM products
    """)
    print_results(cursor, "data freshness")

    # ================================================================
    # PERFORMANCE COMPARISON: MariaDB vs Athena
    # ================================================================
    print("\n" + "=" * 70)
    print("📊 PERFORMANCE COMPARISON: MariaDB vs Athena")
    print("=" * 70)
    print("""
   ┌──────────────────────────────┬──────────────┬──────────────┐
   │ Query                        │ MariaDB      │ Athena       │
   ├──────────────────────────────┼──────────────┼──────────────┤
   │ SELECT COUNT(*)              │ ~5ms         │ ~2500ms      │
   │ GROUP BY status              │ ~15ms        │ ~3000ms      │
   │ JOIN orders + products       │ ~50ms        │ ~4500ms      │
   │ Top 10 customers (3-way)    │ ~80ms        │ ~5000ms      │
   ├──────────────────────────────┼──────────────┼──────────────┤
   │ Cost per query               │ $0.00        │ $0.055       │
   │ Setup cost                   │ EC2 running  │ $0/no infra  │
   │ Concurrent users             │ ~100         │ ~20          │
   │ Best for                     │ App queries  │ Ad-hoc SQL   │
   └──────────────────────────────┴──────────────┴──────────────┘

   CONCLUSION:
   → MariaDB: fast, cheap for repeated queries, app-facing
   → Athena: flexible, zero infra, pay-per-scan, ad-hoc
   → Use BOTH — each for what it's best at
    """)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    verify_data()