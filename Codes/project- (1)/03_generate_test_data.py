# file: 03_generate_test_data.py
# Purpose: Create realistic test CSVs so upload script can be tested

import csv
import os
import random
from datetime import datetime

LOCAL_EXPORT_DIR = "/tmp/exports"
TODAY = datetime.now().strftime("%Y%m%d")


def ensure_directory():
    os.makedirs(LOCAL_EXPORT_DIR, exist_ok=True)
    print(f"📁 Directory ready: {LOCAL_EXPORT_DIR}")


def generate_orders(num_rows=1000):
    filepath = os.path.join(LOCAL_EXPORT_DIR, f"orders_{TODAY}.csv")
    statuses = ["completed", "pending", "shipped", "cancelled", "refunded"]
    
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "total_amount",
            "status", "order_date"
        ])
        for i in range(1, num_rows + 1):
            qty = random.randint(1, 10)
            price = round(random.uniform(9.99, 499.99), 2)
            writer.writerow([
                f"ORD-{TODAY}-{i:06d}",
                f"CUST-{random.randint(1, 50000):06d}",
                f"PROD-{random.randint(1, 5000):05d}",
                qty,
                price,
                round(qty * price, 2),
                random.choice(statuses),
                datetime.now().strftime("%Y-%m-%d")
            ])
    
    size_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
    print(f"✅ Generated: orders_{TODAY}.csv ({num_rows} rows, {size_mb} MB)")


def generate_customers(num_rows=500):
    filepath = os.path.join(LOCAL_EXPORT_DIR, f"customers_{TODAY}.csv")
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston",
        "Phoenix", "Philadelphia", "San Antonio", "San Diego"
    ]
    tiers = ["bronze", "silver", "gold", "platinum"]
    
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "name", "email",
            "city", "state", "tier", "signup_date"
        ])
        for i in range(1, num_rows + 1):
            city = random.choice(cities)
            writer.writerow([
                f"CUST-{i:06d}",
                f"Customer_{i}",
                f"customer_{i}@example.com",
                city,
                "US",
                random.choice(tiers),
                f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            ])
    
    size_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
    print(f"✅ Generated: customers_{TODAY}.csv ({num_rows} rows, {size_mb} MB)")


def generate_products(num_rows=200):
    filepath = os.path.join(LOCAL_EXPORT_DIR, f"products_{TODAY}.csv")
    categories = [
        "Electronics", "Clothing", "Home & Kitchen",
        "Books", "Sports", "Toys", "Beauty", "Automotive"
    ]
    
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "product_id", "name", "category",
            "price", "stock_quantity", "is_active"
        ])
        for i in range(1, num_rows + 1):
            writer.writerow([
                f"PROD-{i:05d}",
                f"Product_{i}",
                random.choice(categories),
                round(random.uniform(4.99, 999.99), 2),
                random.randint(0, 5000),
                random.choice(["true", "false"])
            ])
    
    size_mb = round(os.path.getsize(filepath) / (1024 * 1024), 2)
    print(f"✅ Generated: products_{TODAY}.csv ({num_rows} rows, {size_mb} MB)")


if __name__ == "__main__":
    ensure_directory()
    generate_orders(1000)
    generate_customers(500)
    generate_products(200)
    print(f"\n🎯 All test files ready in {LOCAL_EXPORT_DIR}")
    print(f"   Now run: python 02_upload_daily_csvs.py")