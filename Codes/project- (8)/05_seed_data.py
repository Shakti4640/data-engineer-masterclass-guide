# file: 05_seed_data.py
# Purpose: Insert realistic sample data into MariaDB
# Run on EC2: python3 05_seed_data.py

import pymysql
import random
from datetime import datetime, timedelta

# --- CONNECTION CONFIG ---
# Using app_user (not root!) — practice least privilege from Day 1
DB_CONFIG = {
    "host": "127.0.0.1",       # TCP connection (not Unix socket)
    "port": 3306,
    "user": "app_user",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart_db",
    "charset": "utf8mb4",
    "autocommit": False         # We control transactions explicitly
}

# --- DATA GENERATORS ---
CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("Seattle", "WA"), ("Denver", "CO"), ("Boston", "MA")
]
TIERS = ["bronze", "silver", "gold", "platinum"]
CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen",
    "Books", "Sports", "Toys", "Beauty", "Automotive"
]
STATUSES = ["pending", "shipped", "completed", "cancelled", "refunded"]


def seed_customers(cursor, count=500):
    """Insert customer records"""
    print(f"👥 Seeding {count} customers...")
    
    sql = """
        INSERT INTO customers 
            (customer_id, name, email, city, state, tier, signup_date)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s)
    """
    
    rows = []
    for i in range(1, count + 1):
        city, state = random.choice(CITIES)
        signup = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        rows.append((
            f"CUST-{i:06d}",
            f"Customer {i}",
            f"customer_{i}@quickcart.com",
            city,
            state,
            random.choice(TIERS),
            signup.strftime("%Y-%m-%d")
        ))
    
    cursor.executemany(sql, rows)
    print(f"   ✅ {count} customers inserted")


def seed_products(cursor, count=200):
    """Insert product records"""
    print(f"📦 Seeding {count} products...")
    
    sql = """
        INSERT INTO products 
            (product_id, name, category, price, stock_quantity, is_active)
        VALUES 
            (%s, %s, %s, %s, %s, %s)
    """
    
    rows = []
    for i in range(1, count + 1):
        category = random.choice(CATEGORIES)
        rows.append((
            f"PROD-{i:05d}",
            f"{category} Item {i}",
            category,
            round(random.uniform(4.99, 999.99), 2),
            random.randint(0, 5000),
            random.choice([True, True, True, False])  # 75% active
        ))
    
    cursor.executemany(sql, rows)
    print(f"   ✅ {count} products inserted")


def seed_orders(cursor, num_orders=2000, num_customers=500, num_products=200):
    """Insert orders and order_items"""
    print(f"🛒 Seeding {num_orders} orders with items...")
    
    order_sql = """
        INSERT INTO orders 
            (order_id, customer_id, total_amount, status, order_date)
        VALUES 
            (%s, %s, %s, %s, %s)
    """
    
    item_sql = """
        INSERT INTO order_items 
            (order_id, product_id, quantity, unit_price)
        VALUES 
            (%s, %s, %s, %s)
    """
    
    total_items = 0
    
    for i in range(1, num_orders + 1):
        order_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 180))
        date_str = order_date.strftime("%Y%m%d")
        order_id = f"ORD-{date_str}-{i:06d}"
        customer_id = f"CUST-{random.randint(1, num_customers):06d}"
        
        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        # Pick unique products for this order
        product_indices = random.sample(range(1, num_products + 1), num_items)
        
        order_total = 0
        item_rows = []
        
        for prod_idx in product_indices:
            product_id = f"PROD-{prod_idx:05d}"
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(9.99, 299.99), 2)
            line_total = round(quantity * unit_price, 2)
            order_total += line_total
            
            item_rows.append((order_id, product_id, quantity, unit_price))
        
        # Insert order header
        cursor.execute(order_sql, (
            order_id,
            customer_id,
            round(order_total, 2),
            random.choice(STATUSES),
            order_date.strftime("%Y-%m-%d")
        ))
        
        # Insert order items
        cursor.executemany(item_sql, item_rows)
        total_items += len(item_rows)
        
        # Progress indicator
        if i % 500 == 0:
            print(f"   ... {i}/{num_orders} orders inserted")
    
    print(f"   ✅ {num_orders} orders + {total_items} order items inserted")


def verify_data(cursor):
    """Run verification queries"""
    print("\n🔍 VERIFICATION:")
    print("-" * 50)
    
    queries = [
        ("Total customers", "SELECT COUNT(*) FROM customers"),
        ("Total products", "SELECT COUNT(*) FROM products"),
        ("Total orders", "SELECT COUNT(*) FROM orders"),
        ("Total order items", "SELECT COUNT(*) FROM order_items"),
        ("Revenue (total)", "SELECT ROUND(SUM(total_amount), 2) FROM orders"),
        ("Avg order value", "SELECT ROUND(AVG(total_amount), 2) FROM orders"),
        ("Orders by status", """
            SELECT status, COUNT(*) as cnt 
            FROM orders 
            GROUP BY status 
            ORDER BY cnt DESC
        """),
        ("Customers by tier", """
            SELECT tier, COUNT(*) as cnt 
            FROM customers 
            GROUP BY tier 
            ORDER BY cnt DESC
        """),
        ("Top 5 categories by items sold", """
            SELECT p.category, SUM(oi.quantity) as units_sold
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            GROUP BY p.category
            ORDER BY units_sold DESC
            LIMIT 5
        """)
    ]
    
    for label, sql in queries:
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"\n  📊 {label}:")
        for row in results:
            print(f"     {row}")


def run_seed():
    """Main execution"""
    print("=" * 60)
    print("🚀 SEEDING QUICKCART DATABASE")
    print("=" * 60)
    
    connection = None
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Check if data already exists
        cursor.execute("SELECT COUNT(*) FROM customers")
        existing = cursor.fetchone()[0]
        
        if existing > 0:
            print(f"\n⚠️  Database already has {existing} customers")
            print("   Truncating tables for fresh seed...")
            # Must disable FK checks to truncate in order
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            cursor.execute("TRUNCATE TABLE order_items")
            cursor.execute("TRUNCATE TABLE orders")
            cursor.execute("TRUNCATE TABLE products")
            cursor.execute("TRUNCATE TABLE customers")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            connection.commit()
            print("   ✅ Tables truncated")
        
        # Seed in dependency order: dimensions first, then facts
        seed_customers(cursor, count=500)
        seed_products(cursor, count=200)
        seed_orders(cursor, num_orders=2000, num_customers=500, num_products=200)
        
        # COMMIT all inserts as one transaction
        connection.commit()
        print("\n✅ All data committed to database")
        
        # Verify
        verify_data(cursor)
        
        # Show table sizes
        print("\n📏 TABLE SIZES:")
        cursor.execute("""
            SELECT 
                TABLE_NAME,
                TABLE_ROWS,
                ROUND(DATA_LENGTH / 1024, 1) AS data_kb,
                ROUND(INDEX_LENGTH / 1024, 1) AS index_kb
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'quickcart_db'
            ORDER BY TABLE_NAME
        """)
        for row in cursor.fetchall():
            print(f"   {row[0]:<20} rows={row[1]:<8} data={row[2]}KB  idx={row[3]}KB")
        
    except pymysql.Error as e:
        print(f"\n❌ Database error: {e}")
        if connection:
            connection.rollback()
            print("   Transaction rolled back")
        raise
        
    finally:
        if connection:
            connection.close()
            print("\n🔌 Connection closed")
    
    print("\n" + "=" * 60)
    print("✅ SEED COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_seed()