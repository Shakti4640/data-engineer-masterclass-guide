# file: 03_transaction_patterns.py
# Purpose: Demonstrate correct transaction handling for multi-table operations
# Solves: "Order header created but items missing" problem

import pymysql
import random
from datetime import datetime, date

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "app_user",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart_db",
    "charset": "utf8mb4",
    "autocommit": False         # CRITICAL: we manage transactions ourselves
}


def create_order_WRONG_way(cursor, conn):
    """
    ❌ THE WRONG WAY — no transaction control
    If second INSERT fails, order exists without items
    """
    print("\n❌ WRONG WAY — no explicit transaction")
    print("-" * 50)
    
    order_id = f"ORD-WRONG-{random.randint(10000,99999)}"
    
    try:
        # Insert order header
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, total_amount, status, order_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, "CUST-000001", 99.99, "pending", date.today()))
        
        conn.commit()    # ← MISTAKE: committing BEFORE items inserted
        print(f"   Order {order_id} committed")
        
        # Simulate failure — maybe product doesn't exist
        cursor.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
        """, (order_id, "PROD-NONEXISTENT", 1, 99.99))
        
        conn.commit()
        
    except pymysql.Error as e:
        print(f"   ❌ Item insert failed: {e}")
        print(f"   ⚠️  BUT order {order_id} already committed!")
        print(f"   ⚠️  Customer sees empty order — DATA INCONSISTENCY")
        conn.rollback()
    
    # Verify the damage
    cursor.execute("SELECT order_id, total_amount FROM orders WHERE order_id = %s", (order_id,))
    orphan = cursor.fetchone()
    if orphan:
        print(f"   💀 Orphan order exists: {orphan}")
        # Cleanup
        cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        conn.commit()


def create_order_RIGHT_way(cursor, conn):
    """
    ✅ THE RIGHT WAY — proper transaction
    All inserts succeed together or fail together
    """
    print("\n✅ RIGHT WAY — explicit transaction with rollback")
    print("-" * 50)
    
    order_id = f"ORD-RIGHT-{random.randint(10000,99999)}"
    
    try:
        # All operations in ONE transaction (autocommit=False)
        
        # Insert order header
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, total_amount, status, order_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, "CUST-000001", 149.97, "pending", date.today()))
        print(f"   Order header inserted (not yet committed)")
        
        # Insert order items
        items = [
            (order_id, "PROD-00001", 2, 29.99),
            (order_id, "PROD-00002", 1, 89.99),
        ]
        cursor.executemany("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
        """, items)
        print(f"   {len(items)} items inserted (not yet committed)")
        
        # Insert initial status history
        cursor.execute("""
            INSERT INTO order_status_history 
                (order_id, old_status, new_status, changed_by)
            VALUES (%s, NULL, 'pending', 'api_server')
        """, (order_id,))
        print(f"   Status history recorded (not yet committed)")
        
        # ALL GOOD — commit everything atomically
        conn.commit()
        print(f"   ✅ COMMITTED — all 4 inserts are now permanent")
        
        # Verify
        cursor.execute("""
            SELECT o.order_id, o.total_amount, COUNT(oi.item_id) as item_count
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_id = %s
            GROUP BY o.order_id, o.total_amount
        """, (order_id,))
        result = cursor.fetchone()
        print(f"   Verified: {result}")
        
        return order_id
        
    except pymysql.Error as e:
        conn.rollback()     # ← UNDO everything
        print(f"   ❌ Error: {e}")
        print(f"   🔄 ROLLED BACK — database unchanged")
        return None


def create_order_FAILED_transaction(cursor, conn):
    """
    ✅ RIGHT WAY with intentional failure — proves rollback works
    """
    print("\n✅ RIGHT WAY — transaction with intentional failure")
    print("-" * 50)
    
    order_id = f"ORD-FAIL-{random.randint(10000,99999)}"
    
    try:
        cursor.execute("""
            INSERT INTO orders (order_id, customer_id, total_amount, status, order_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (order_id, "CUST-000001", 99.99, "pending", date.today()))
        print(f"   Order header inserted (in transaction)")
        
        # This will FAIL — PROD-NONEXISTENT doesn't exist (FK constraint)
        cursor.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price)
            VALUES (%s, %s, %s, %s)
        """, (order_id, "PROD-NONEXISTENT", 1, 99.99))
        
        conn.commit()
        
    except pymysql.Error as e:
        conn.rollback()
        print(f"   ❌ Item insert failed: {e}")
        print(f"   🔄 ROLLED BACK — order header also removed")
    
    # Verify: order should NOT exist
    cursor.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
    result = cursor.fetchone()
    if result is None:
        print(f"   ✅ Confirmed: order {order_id} does NOT exist — clean state")
    else:
        print(f"   ⚠️  Unexpected: order exists: {result}")


def update_order_status_transactional(cursor, conn, order_id, new_status, changed_by="system"):
    """
    ✅ Status update pattern: update order + insert history in one transaction
    """
    print(f"\n🔄 Updating order {order_id} to '{new_status}'")
    
    try:
        # Get current status
        cursor.execute(
            "SELECT status FROM orders WHERE order_id = %s FOR UPDATE",
            (order_id,)
        )
        # FOR UPDATE: locks this row until transaction ends
        # Prevents another thread from changing status simultaneously
        
        row = cursor.fetchone()
        if not row:
            print(f"   ❌ Order not found")
            return False
        
        old_status = row[0]
        
        if old_status == new_status:
            print(f"   ℹ️  Already '{new_status}' — no change")
            conn.rollback()     # Release lock
            return True
        
        # Update order status
        cursor.execute("""
            UPDATE orders SET status = %s WHERE order_id = %s
        """, (new_status, order_id))
        
        # Insert history record
        cursor.execute("""
            INSERT INTO order_status_history 
                (order_id, old_status, new_status, changed_by)
            VALUES (%s, %s, %s, %s)
        """, (order_id, old_status, new_status, changed_by))
        
        conn.commit()
        print(f"   ✅ {old_status} → {new_status} (committed)")
        return True
        
    except pymysql.Error as e:
        conn.rollback()
        print(f"   ❌ Failed: {e}")
        return False


def run_transaction_demos():
    """Execute all transaction demonstrations"""
    print("=" * 60)
    print("🔄 TRANSACTION PATTERNS DEMONSTRATION")
    print("=" * 60)
    
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Demo 1: Wrong way
    create_order_WRONG_way(cursor, conn)
    
    # Demo 2: Right way — success
    order_id = create_order_RIGHT_way(cursor, conn)
    
    # Demo 3: Right way — failure (proves rollback)
    create_order_FAILED_transaction(cursor, conn)
    
    # Demo 4: Status update pattern
    if order_id:
        update_order_status_transactional(cursor, conn, order_id, "shipped", "warehouse_api")
        update_order_status_transactional(cursor, conn, order_id, "completed", "delivery_api")
        
        # Show full history
        print(f"\n📜 Full status history for {order_id}:")
        cursor.execute("""
            SELECT old_status, new_status, changed_by, changed_at
            FROM order_status_history
            WHERE order_id = %s
            ORDER BY changed_at
        """, (order_id,))
        for row in cursor.fetchall():
            print(f"   {row[0] or 'NULL':>12} → {row[1]:<12} by {row[2]:<15} at {row[3]}")
    
    conn.close()
    print("\n🔌 Connection closed")


if __name__ == "__main__":
    run_transaction_demos()