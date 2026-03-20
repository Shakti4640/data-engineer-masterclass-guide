# file: 04_bulk_load_benchmark.py
# Purpose: Compare insert methods — prove bulk is 100x faster
# Solves: "Loading 500K rows takes 4 hours" problem

import pymysql
import time
import random
from datetime import datetime, date, timedelta

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "app_user",
    "password"[REDACTED:PASSWORD]25!",
    "database": "quickcart_db",
    "charset": "utf8mb4",
    "autocommit": False
}

# Number of rows for each test
TEST_ROWS = 10000       # Use 10K for demo; scale to 500K for real benchmark


def generate_review_data(count):
    """Generate sample review data"""
    rows = []
    used_combos = set()
    
    for _ in range(count):
        while True:
            prod = f"PROD-{random.randint(1, 200):05d}"
            cust = f"CUST-{random.randint(1, 500):06d}"
            combo = (prod, cust)
            if combo not in used_combos:
                used_combos.add(combo)
                break
        
        rows.append((
            prod,
            cust,
            random.randint(1, 5),
            f"Review title {random.randint(1, 10000)}",
            f"This is a review body with some text about the product quality and value.",
            random.choice([True, False])
        ))
    
    return rows


def cleanup(cursor, conn):
    """Remove test data"""
    cursor.execute("DELETE FROM product_reviews WHERE review_title LIKE 'Review title%'")
    conn.commit()


def method_1_row_by_row_with_commit(cursor, conn, data):
    """
    ❌ WORST: Individual INSERT + COMMIT per row
    Real-world equivalent: naive loop with autocommit=True
    """
    cleanup(cursor, conn)
    
    sql = """
        INSERT INTO product_reviews 
            (product_id, customer_id, rating, review_title, review_body, is_verified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    start = time.time()
    
    for row in data:
        cursor.execute(sql, row)
        conn.commit()           # Commit per row — redo log flush per row
    
    elapsed = time.time() - start
    rate = len(data) / elapsed
    
    print(f"   Method 1 (row-by-row + commit each): {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return elapsed


def method_2_row_by_row_single_commit(cursor, conn, data):
    """
    🟡 BETTER: Individual INSERT but single COMMIT at end
    Eliminates per-row redo log flush
    """
    cleanup(cursor, conn)
    
    sql = """
        INSERT INTO product_reviews 
            (product_id, customer_id, rating, review_title, review_body, is_verified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    start = time.time()
    
    for row in data:
        cursor.execute(sql, row)
    conn.commit()               # Single commit at end
    
    elapsed = time.time() - start
    rate = len(data) / elapsed
    
    print(f"   Method 2 (row-by-row + single commit): {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return elapsed


def method_3_executemany(cursor, conn, data):
    """
    🟢 GOOD: executemany — pymysql batches the INSERTs
    """
    cleanup(cursor, conn)
    
    sql = """
        INSERT INTO product_reviews 
            (product_id, customer_id, rating, review_title, review_body, is_verified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    start = time.time()
    
    cursor.executemany(sql, data)
    conn.commit()
    
    elapsed = time.time() - start
    rate = len(data) / elapsed
    
    print(f"   Method 3 (executemany + single commit): {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return elapsed


def method_4_multi_row_insert(cursor, conn, data, batch_size=1000):
    """
    ✅ BEST (without LOAD DATA): Multi-row INSERT VALUES
    INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c'), ...
    Single SQL statement per batch — minimal parser overhead
    """
    cleanup(cursor, conn)
    
    start = time.time()
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        
        # Build multi-row VALUES clause
        placeholders = ", ".join(
            ["(%s, %s, %s, %s, %s, %s)"] * len(batch)
        )
        
        sql = f"""
            INSERT INTO product_reviews 
                (product_id, customer_id, rating, review_title, review_body, is_verified)
            VALUES {placeholders}
        """
        
        # Flatten batch into single tuple
        flat_values = []
        for row in batch:
            flat_values.extend(row)
        
        cursor.execute(sql, flat_values)
        
        # Commit every batch — balances memory vs durability
        if (i // batch_size) % 10 == 0:
            conn.commit()
    
    conn.commit()   # Final commit
    
    elapsed = time.time() - start
    rate = len(data) / elapsed
    
    print(f"   Method 4 (multi-row INSERT, batch={batch_size}): {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return elapsed


def method_5_batched_commit(cursor, conn, data, batch_size=5000):
    """
    ✅ PRACTICAL: executemany with periodic commits
    Best for very large loads (500K+) — prevents massive undo log
    """
    cleanup(cursor, conn)
    
    sql = """
        INSERT INTO product_reviews 
            (product_id, customer_id, rating, review_title, review_body, is_verified)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    start = time.time()
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(sql, batch)
        conn.commit()           # Commit every batch_size rows
    
    elapsed = time.time() - start
    rate = len(data) / elapsed
    
    print(f"   Method 5 (executemany + commit every {batch_size}): {elapsed:.2f}s ({rate:.0f} rows/sec)")
    return elapsed


def run_benchmark():
    """Run all methods and compare"""
    print("=" * 60)
    print(f"🏎️  BULK INSERT BENCHMARK — {TEST_ROWS:,} rows")
    print("=" * 60)
    
    # Generate test data once
    print(f"\n📊 Generating {TEST_ROWS:,} test rows...")
    data = generate_review_data(TEST_ROWS)
    print(f"   ✅ Data ready")
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    print(f"\n⏱️  Running benchmarks:\n")
    
    # Run each method
    t1 = method_1_row_by_row_with_commit(cursor, conn, data)
    t2 = method_2_row_by_row_single_commit(cursor, conn, data)
    t3 = method_3_executemany(cursor, conn, data)
    t4 = method_4_multi_row_insert(cursor, conn, data)
    t5 = method_5_batched_commit(cursor, conn, data)
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 RESULTS SUMMARY ({TEST_ROWS:,} rows)")
    print(f"{'='*60}")
    print(f"{'Method':<45} {'Time':>8} {'Speedup':>8}")
    print(f"{'-'*60}")
    print(f"{'1. Row-by-row + commit each (WORST)':<45} {t1:>7.2f}s {'1.0x':>8}")
    print(f"{'2. Row-by-row + single commit':<45} {t2:>7.2f}s {f'{t1/t2:.1f}x':>8}")
    print(f"{'3. executemany + single commit':<45} {t3:>7.2f}s {f'{t1/t3:.1f}x':>8}")
    print(f"{'4. Multi-row INSERT (batch=1000)':<45} {t4:>7.2f}s {f'{t1/t4:.1f}x':>8}")
    print(f"{'5. executemany + periodic commit (PRACTICAL)':<45} {t5:>7.2f}s {f'{t1/t5:.1f}x':>8}")
    
    print(f"""
💡 KEY INSIGHT:
   → Method 1 is what most beginners write
   → Method 4/5 is what production systems use
   → Difference: {t1/min(t4,t5):.0f}x faster
   → For 500K rows: Method 1 = ~{(t1/TEST_ROWS)*500000/3600:.1f} hours, Method 4 = ~{(t4/TEST_ROWS)*500000:.0f} seconds
    """)
    
    # Cleanup
    cleanup(cursor, conn)
    conn.close()


if __name__ == "__main__":
    run_benchmark()