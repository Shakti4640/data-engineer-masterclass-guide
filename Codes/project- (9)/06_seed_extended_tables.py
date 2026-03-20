# file: 06_seed_extended_tables.py
# Purpose: Populate new tables created in V001 migration
# Depends on: Project 8 seed data (customers, products, orders)

import pymysql
import random
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "app_user",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart_db",
    "charset": "utf8mb4",
    "autocommit": False
}

STREETS = [
    "123 Main St", "456 Oak Ave", "789 Pine Rd", "321 Elm Blvd",
    "654 Maple Dr", "987 Cedar Ln", "147 Birch Way", "258 Spruce Ct",
    "369 Walnut Pl", "741 Cherry St", "852 Ash Ave", "963 Willow Rd"
]
CITIES_STATES = [
    ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"),
    ("Chicago", "IL", "60601"), ("Houston", "TX", "77001"),
    ("Phoenix", "AZ", "85001"), ("Seattle", "WA", "98101"),
    ("Denver", "CO", "80201"), ("Boston", "MA", "02101")
]


def seed_addresses(cursor, conn):
    """1-3 addresses per customer"""
    print("🏠 Seeding customer addresses...")
    
    cursor.execute("SELECT customer_id FROM customers")
    customers = [row[0] for row in cursor.fetchall()]
    
    sql = """
        INSERT INTO customer_addresses 
            (customer_id, address_type, street_line1, street_line2,
             city, state, zip_code, country, is_default)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    rows = []
    for cust_id in customers:
        num_addresses = random.randint(1, 3)
        for j in range(num_addresses):
            city, state, zipcode = random.choice(CITIES_STATES)
            addr_type = ["billing", "shipping", "both"][min(j, 2)]
            is_default = (j == 0)       # First address is default
            
            rows.append((
                cust_id,
                addr_type,
                random.choice(STREETS),
                f"Apt {random.randint(1, 999)}" if random.random() > 0.5 else None,
                city,
                state,
                zipcode,
                "US",
                is_default
            ))
    
    # Bulk insert using batch method from Project 9 benchmark
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()
    
    print(f"   ✅ {len(rows)} addresses inserted for {len(customers)} customers")


def seed_reviews(cursor, conn):
    """Random reviews — not every customer reviews every product"""
    print("⭐ Seeding product reviews...")
    
    cursor.execute("SELECT customer_id FROM customers")
    customers = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT product_id FROM products")
    products = [row[0] for row in cursor.fetchall()]
    
    sql = """
        INSERT INTO product_reviews 
            (product_id, customer_id, rating, review_title, 
             review_body, is_verified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    titles_by_rating = {
        1: ["Terrible", "Waste of money", "Broken on arrival"],
        2: ["Disappointing", "Not as described", "Below average"],
        3: ["It's okay", "Average product", "Does the job"],
        4: ["Good quality", "Happy with purchase", "Recommended"],
        5: ["Excellent!", "Best purchase ever", "Perfect product"]
    }
    
    # Generate unique (product, customer) combinations
    used_combos = set()
    rows = []
    target = min(3000, len(customers) * 2)      # ~6 reviews per customer avg
    
    attempts = 0
    while len(rows) < target and attempts < target * 3:
        attempts += 1
        prod = random.choice(products)
        cust = random.choice(customers)
        combo = (prod, cust)
        
        if combo in used_combos:
            continue
        used_combos.add(combo)
        
        # Skew toward positive reviews (realistic)
        rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0]
        title = random.choice(titles_by_rating[rating])
        
        rows.append((
            prod,
            cust,
            rating,
            title,
            f"Detailed review about this product. Rating: {rating}/5. "
            f"{'Would recommend.' if rating >= 4 else 'Would not recommend.'}",
            random.choice([True, True, True, False])    # 75% verified
        ))
    
    # Bulk insert
    batch_size = 1000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()
    
    print(f"   ✅ {len(rows)} reviews inserted")


def seed_status_history(cursor, conn):
    """Create realistic status history for existing orders"""
    print("📜 Seeding order status history...")
    
    cursor.execute("SELECT order_id, status, order_date FROM orders")
    orders = cursor.fetchall()
    
    # Status transition paths
    status_paths = {
        "pending":   [("pending",)],
        "shipped":   [("pending",), ("shipped",)],
        "completed": [("pending",), ("shipped",), ("completed",)],
        "cancelled": [("pending",), ("cancelled",)],
        "refunded":  [("pending",), ("shipped",), ("completed",), ("refunded",)]
    }
    
    sql = """
        INSERT INTO order_status_history 
            (order_id, old_status, new_status, changed_by, changed_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    rows = []
    for order_id, final_status, order_date in orders:
        path = status_paths.get(final_status, [("pending",)])
        
        # Build history entries with timestamps
        base_time = datetime.combine(order_date, datetime.min.time())
        prev_status = None
        
        for idx, (status,) in enumerate(path):
            changed_at = base_time + timedelta(hours=idx * random.randint(6, 48))
            changer = random.choice(["api_server", "warehouse", "delivery", "cs_agent"])
            
            rows.append((
                order_id,
                prev_status,
                status,
                changer,
                changed_at
            ))
            prev_status = status
    
    # Bulk insert
    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()
    
    print(f"   ✅ {len(rows)} status history records for {len(orders)} orders")


def verify_extended_data(cursor):
    """Verify all extended tables"""
    print(f"\n🔍 VERIFICATION:")
    print("-" * 60)
    
    queries = [
        ("Addresses per customer (avg)",
         "SELECT ROUND(AVG(cnt), 1) FROM (SELECT COUNT(*) cnt FROM customer_addresses GROUP BY customer_id) t"),
        
        ("Reviews by rating",
         "SELECT rating, COUNT(*) cnt FROM product_reviews GROUP BY rating ORDER BY rating"),
        
        ("Avg product rating",
         "SELECT ROUND(AVG(rating), 2) FROM product_reviews"),
        
        ("Status history records",
         "SELECT COUNT(*) FROM order_status_history"),
        
        ("Orders with full history",
         """SELECT osh.order_id, 
                   GROUP_CONCAT(osh.new_status ORDER BY osh.changed_at SEPARATOR ' → ') as journey
            FROM order_status_history osh
            GROUP BY osh.order_id
            LIMIT 5"""),
        
        ("Table sizes (all)",
         """SELECT TABLE_NAME, TABLE_ROWS, 
                   ROUND((DATA_LENGTH + INDEX_LENGTH)/1024/1024, 2) as total_mb
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'quickcart_db'
            ORDER BY total_mb DESC""")
    ]
    
    for label, sql in queries:
        cursor.execute(sql)
        results = cursor.fetchall()
        print(f"\n  📊 {label}:")
        for row in results:
            print(f"     {row}")


def run_extended_seed():
    """Main execution"""
    print("=" * 60)
    print("🚀 SEEDING EXTENDED TABLES")
    print("=" * 60)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Check if already seeded
    cursor.execute("SELECT COUNT(*) FROM customer_addresses")
    if cursor.fetchone()[0] > 0:
        print("\n⚠️  Extended tables already have data — truncating...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.execute("TRUNCATE TABLE customer_addresses")
        cursor.execute("TRUNCATE TABLE product_reviews")
        cursor.execute("TRUNCATE TABLE order_status_history")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        conn.commit()
    
    seed_addresses(cursor, conn)
    seed_reviews(cursor, conn)
    seed_status_history(cursor, conn)
    
    verify_extended_data(cursor)
    
    conn.close()
    print("\n🔌 Connection closed")
    print("\n✅ EXTENDED SEED COMPLETE")


if __name__ == "__main__":
    run_extended_seed()