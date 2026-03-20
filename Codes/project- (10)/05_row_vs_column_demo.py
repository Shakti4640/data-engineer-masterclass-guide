# file: 05_row_vs_column_demo.py
# Purpose: Demonstrate WHY columnar storage beats row storage for analytics
# Loads sample data and runs comparison queries
# This is the KEY insight that justifies Redshift's existence

import psycopg2
import time
import random
from datetime import datetime, timedelta

REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "etl_loader",
    "password":[REDACTED:PASSWORD]5!"
}

NUM_ORDERS = 100000         # 100K for demo; real world: millions
NUM_ITEMS_PER_ORDER = 3     # Average items per order


def load_sample_data(cursor, conn):
    """
    Load sample data into Redshift fact tables
    
    NOTE: In production, use COPY from S3 (Project 21)
    Multi-row INSERT is used here for demonstration ONLY
    COPY is 10-100x faster for large loads
    """
    print(f"\n📦 Loading {NUM_ORDERS:,} sample orders...")
    
    # Check if data already loaded
    cursor.execute("SELECT COUNT(*) FROM analytics.fact_orders")
    existing = cursor.fetchone()[0]
    if existing > 0:
        print(f"   ℹ️  Already have {existing:,} orders — skipping load")
        return
    
    statuses = ["pending", "shipped", "completed", "cancelled", "refunded"]
    status_weights = [10, 15, 55, 12, 8]
    
    # Batch insert orders
    batch_size = 5000
    total_loaded = 0
    
    for batch_start in range(0, NUM_ORDERS, batch_size):
        batch_end = min(batch_start + batch_size, NUM_ORDERS)
        
        values_orders = []
        values_items = []
        
        for i in range(batch_start, batch_end):
            order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 545))
            date_str = order_date.strftime("%Y%m%d")
            order_id = f"ORD-{date_str}-{i:06d}"
            customer_id = f"CUST-{random.randint(1, 500):06d}"
            status = random.choices(statuses, weights=status_weights)[0]
            
            num_items = random.randint(1, 5)
            order_total = 0
            
            for j in range(num_items):
                product_id = f"PROD-{random.randint(1, 200):05d}"
                qty = random.randint(1, 5)
                price = round(random.uniform(9.99, 299.99), 2)
                line_total = round(qty * price, 2)
                order_total += line_total
                
                values_items.append(
                    f"({batch_start * 5 + i * 5 + j + 1}, "
                    f"'{order_id}', '{product_id}', '{customer_id}', "
                    f"{qty}, {price}, {line_total}, '{order_date.strftime('%Y-%m-%d')}')"
                )
            
            values_orders.append(
                f"('{order_id}', '{customer_id}', {round(order_total, 2)}, "
                f"'{status}', '{order_date.strftime('%Y-%m-%d')}', {num_items}, "
                f"GETDATE(), GETDATE())"
            )
        
        # Insert orders batch
        orders_sql = f"""
            INSERT INTO analytics.fact_orders 
                (order_id, customer_id, total_amount, status, order_date, 
                 item_count, created_at, updated_at)
            VALUES {', '.join(values_orders)}
        """
        cursor.execute(orders_sql)
        
        # Insert items in sub-batches (SQL has length limits)
        item_sub_batch = 2000
        for k in range(0, len(values_items), item_sub_batch):
            sub = values_items[k:k + item_sub_batch]
            items_sql = f"""
                INSERT INTO analytics.fact_order_items 
                    (item_id, order_id, product_id, customer_id,
                     quantity, unit_price, line_total, order_date)
                VALUES {', '.join(sub)}
            """
            cursor.execute(items_sql)
        
        conn.commit()
        total_loaded += (batch_end - batch_start)
        print(f"   ... {total_loaded:,}/{NUM_ORDERS:,} orders loaded")
    
    # Also load dimension data
    print(f"\n📦 Loading dimension tables...")
    
    # Customers
    cust_values = []
    for i in range(1, 501):
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Seattle"]
        states = ["NY", "CA", "IL", "TX", "WA"]
        tiers = ["bronze", "silver", "gold", "platinum"]
        idx = i % len(cities)
        signup = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        
        cust_values.append(
            f"('CUST-{i:06d}', 'Customer {i}', 'cust{i}@quickcart.com', "
            f"'{cities[idx]}', '{states[idx]}', "
            f"'{random.choice(tiers)}', '{signup.strftime('%Y-%m-%d')}', "
            f"GETDATE(), GETDATE())"
        )
    
    cursor.execute(f"""
        INSERT INTO analytics.dim_customers 
            (customer_id, name, email, city, state, tier, signup_date, created_at, updated_at)
        VALUES {', '.join(cust_values)}
    """)
    print(f"   ✅ 500 customers loaded")
    
    # Products
    prod_values = []
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports", "Beauty", "Toys", "Auto"]
    for i in range(1, 201):
        cat = categories[i % len(categories)]
        prod_values.append(
            f"('PROD-{i:05d}', '{cat} Item {i}', '{cat}', "
            f"{round(random.uniform(4.99, 999.99), 2)}, "
            f"{random.randint(0, 5000)}, {random.choice(['true', 'false'])}, "
            f"GETDATE(), GETDATE())"
        )
    
    cursor.execute(f"""
        INSERT INTO analytics.dim_products 
            (product_id, name, category, price, stock_quantity, is_active, created_at, updated_at)
        VALUES {', '.join(prod_values)}
    """)
    print(f"   ✅ 200 products loaded")
    
    conn.commit()
    
    # Run ANALYZE to update statistics
    print(f"\n📊 Running ANALYZE to update statistics...")
    cursor.execute("ANALYZE analytics.fact_orders")
    cursor.execute("ANALYZE analytics.fact_order_items")
    cursor.execute("ANALYZE analytics.dim_customers")
    cursor.execute("ANALYZE analytics.dim_products")
    conn.commit()
    print(f"   ✅ Statistics updated")


def run_analytics_queries(cursor):
    """
    Run typical analytics queries that MariaDB struggled with
    Show Redshift's columnar + MPP advantage
    """
    
    queries = [
        {
            "label": "QUERY 1: Monthly Revenue Trend (date aggregation)",
            "sql": """
                SELECT 
                    d.year,
                    d.month_name,
                    d.month,
                    COUNT(DISTINCT f.order_id) AS num_orders,
                    ROUND(SUM(f.total_amount), 2) AS revenue,
                    ROUND(AVG(f.total_amount), 2) AS avg_order_value
                FROM analytics.fact_orders f
                JOIN analytics.dim_dates d ON f.order_date = d.date_key
                WHERE f.status = 'completed'
                GROUP BY d.year, d.month_name, d.month
                ORDER BY d.year, d.month
            """,
            "insight": "Date dimension JOIN + aggregation — columnar reads only needed columns"
        },
        {
            "label": "QUERY 2: Revenue by Category (3-table JOIN)",
            "sql": """
                SELECT 
                    p.category,
                    COUNT(DISTINCT fi.order_id) AS num_orders,
                    SUM(fi.quantity) AS units_sold,
                    ROUND(SUM(fi.line_total), 2) AS revenue,
                    ROUND(AVG(fi.line_total), 2) AS avg_line_value
                FROM analytics.fact_order_items fi
                JOIN analytics.dim_products p ON fi.product_id = p.product_id
                WHERE fi.order_date >= '2025-01-01'
                GROUP BY p.category
                ORDER BY revenue DESC
            """,
            "insight": "dim_products is DISTSTYLE ALL — JOIN is always local, zero shuffle"
        },
        {
            "label": "QUERY 3: Customer Lifetime Value by Tier (customer aggregation)",
            "sql": """
                SELECT 
                    c.tier,
                    COUNT(DISTINCT c.customer_id) AS customers,
                    COUNT(DISTINCT f.order_id) AS total_orders,
                    ROUND(SUM(f.total_amount), 2) AS total_revenue,
                    ROUND(AVG(f.total_amount), 2) AS avg_order_value,
                    ROUND(SUM(f.total_amount) / COUNT(DISTINCT c.customer_id), 2) 
                        AS revenue_per_customer
                FROM analytics.dim_customers c
                JOIN analytics.fact_orders f ON c.customer_id = f.customer_id
                WHERE f.status IN ('completed', 'shipped')
                GROUP BY c.tier
                ORDER BY total_revenue DESC
            """,
            "insight": "DISTKEY(customer_id) on fact_orders — JOIN with dim_customers is co-located"
        },
        {
            "label": "QUERY 4: Top 10 Customers by Spend (ranking)",
            "sql": """
                SELECT 
                    c.customer_id,
                    c.name,
                    c.tier,
                    c.city,
                    agg.order_count,
                    agg.total_spent,
                    agg.avg_order_value,
                    RANK() OVER (ORDER BY agg.total_spent DESC) AS spend_rank
                FROM analytics.dim_customers c
                JOIN (
                    SELECT 
                        customer_id,
                        COUNT(*) AS order_count,
                        ROUND(SUM(total_amount), 2) AS total_spent,
                        ROUND(AVG(total_amount), 2) AS avg_order_value
                    FROM analytics.fact_orders
                    WHERE status = 'completed'
                    GROUP BY customer_id
                ) agg ON c.customer_id = agg.customer_id
                ORDER BY agg.total_spent DESC
                LIMIT 10
            """,
            "insight": "Subquery aggregation + window function — MPP processes each slice in parallel"
        },
        {
            "label": "QUERY 5: Daily Order Volume with Running Total (window function)",
            "sql": """
                SELECT 
                    order_date,
                    daily_orders,
                    daily_revenue,
                    SUM(daily_orders) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) 
                        AS cumulative_orders,
                    ROUND(SUM(daily_revenue) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING), 2) 
                        AS cumulative_revenue
                FROM (
                    SELECT 
                        order_date,
                        COUNT(*) AS daily_orders,
                        SUM(total_amount) AS daily_revenue
                    FROM analytics.fact_orders
                    GROUP BY order_date
                ) daily
                ORDER BY order_date
                LIMIT 30
            """,
            "insight": "Window function on pre-aggregated data — sort key on order_date makes this fast"
        },
        {
            "label": "QUERY 6: EXPLAIN PLAN — Show distribution and sort key usage",
            "sql": """
                EXPLAIN
                SELECT 
                    c.tier,
                    COUNT(*) AS orders,
                    SUM(f.total_amount) AS revenue
                FROM analytics.fact_orders f
                JOIN analytics.dim_customers c ON f.customer_id = c.customer_id
                WHERE f.order_date BETWEEN '2025-01-01' AND '2025-06-30'
                  AND f.status = 'completed'
                GROUP BY c.tier
            """,
            "insight": "EXPLAIN shows: DS_DIST_ALL_NONE (no redistribution) because dim is DISTSTYLE ALL"
        }
    ]
    
    for q in queries:
        print(f"\n{'='*70}")
        print(f"📊 {q['label']}")
        print(f"💡 {q['insight']}")
        print(f"{'='*70}")
        
        start = time.time()
        cursor.execute(q["sql"])
        results = cursor.fetchall()
        elapsed_ms = round((time.time() - start) * 1000, 2)
        
        columns = [desc[0] for desc in cursor.description]
        
        header = " | ".join(f"{col:<20}" for col in columns)
        print(f"\n{header}")
        print("-" * len(header))
        
        for row in results[:15]:
            row_str = " | ".join(f"{str(val):<20}" for val in row)
            print(row_str)
        
        if len(results) > 15:
            print(f"... and {len(results) - 15} more rows")
        
        print(f"\n⏱️  {len(results)} rows in {elapsed_ms} ms")


def show_table_stats(cursor):
    """Show storage and distribution statistics"""
    
    print(f"\n{'='*70}")
    print(f"📊 TABLE STATISTICS — STORAGE & DISTRIBUTION")
    print(f"{'='*70}")
    
    cursor.execute("""
        SELECT 
            "table" AS table_name,
            diststyle,
            sortkey1,
            tbl_rows,
            size AS size_mb,
            pct_used,
            skew_rows,
            skew_sortkey1
        FROM svv_table_info
        WHERE "schema" = 'analytics'
        ORDER BY tbl_rows DESC
    """)
    
    columns = [desc[0] for desc in cursor.description]
    print(f"\n{' | '.join(f'{col:<18}' for col in columns)}")
    print("-" * 160)
    
    for row in cursor.fetchall():
        print(f"{' | '.join(f'{str(val):<18}' for val in row)}")
    
    print(f"""
KEY METRICS EXPLAINED:
→ diststyle: how data is spread across nodes (KEY/ALL/EVEN)
→ sortkey1: first column in sort key (enables zone map elimination)
→ tbl_rows: actual row count
→ size_mb: storage on disk (after compression)
→ skew_rows: ratio of max/min rows per slice (1.0 = perfect, >2.0 = problem)
→ skew_sortkey1: percentage of unsorted data (lower = better, run VACUUM)
    """)


def run_demo():
    """Main execution"""
    print("=" * 70)
    print("🚀 REDSHIFT ANALYTICS DEMONSTRATION")
    print("   Proving columnar + MPP beats row-oriented for analytics")
    print("=" * 70)
    
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Load data
    load_sample_data(cursor, conn)
    
    # Show table stats
    show_table_stats(cursor)
    
    # Run analytics queries
    run_analytics_queries(cursor)
    
    # Summary
    print(f"""
{'='*70}
💡 MariaDB vs REDSHIFT — THE COMPARISON
{'='*70}

┌────────────────────────┬───────────────┬────────────────┐
│ Query Type             │ MariaDB       │ Redshift       │
│                        │ (Project 8-9) │ (This Project) │
├────────────────────────┼───────────────┼────────────────┤
│ Single row lookup      │ < 1 ms ✅     │ ~50 ms ❌      │
│ by primary key         │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ INSERT single row      │ < 1 ms ✅     │ ~50 ms ❌      │
│                        │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ SUM/COUNT across       │ 8-14 sec ❌   │ < 500 ms ✅    │
│ 1M rows                │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ JOIN + GROUP BY        │ 12-30 sec ❌  │ < 1 sec ✅     │
│ across 3 tables        │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ Window functions       │ 15-45 sec ❌  │ < 2 sec ✅     │
│ (running totals)       │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ Concurrent analytics   │ Blocks OLTP ❌│ Isolated ✅    │
│ queries                │               │                │
├────────────────────────┼───────────────┼────────────────┤
│ ACID transactions      │ Full ✅       │ Limited ❌     │
├────────────────────────┼───────────────┼────────────────┤
│ Cost                   │ $0 (EC2) ✅   │ ~$183/mo ❌    │
└────────────────────────┴───────────────┴────────────────┘

CONCLUSION:
→ MariaDB for OLTP (app transactions)
→ Redshift for OLAP (analytics aggregations)
→ NEVER use one for the other's job
→ ETL pipeline connects them (Projects 21, 32, 65)
    """)
    
    conn.close()
    print("🔌 Connection closed")


if __name__ == "__main__":
    run_demo()