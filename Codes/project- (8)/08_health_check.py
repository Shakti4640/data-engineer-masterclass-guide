# file: 08_health_check.py
# Purpose: Verify MariaDB is healthy — run after install or anytime

import pymysql
import sys

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "app_user",
    "password"[REDACTED:PASSWORD]25!",
    "database": "quickcart_db",
    "charset": "utf8mb4"
}


def check_health():
    """Run comprehensive health checks"""
    
    print("=" * 60)
    print("🏥 MariaDB HEALTH CHECK")
    print("=" * 60)
    
    checks_passed = 0
    checks_failed = 0
    
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # --- CHECK 1: Connection ---
        cursor.execute("SELECT 1")
        print("\n✅ CHECK 1: Connection successful")
        checks_passed += 1
        
        # --- CHECK 2: Version ---
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"✅ CHECK 2: MariaDB version = {version}")
        checks_passed += 1
        
        # --- CHECK 3: Engine ---
        cursor.execute("""
            SELECT ENGINE, COUNT(*) 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'quickcart_db' 
            GROUP BY ENGINE
        """)
        engines = cursor.fetchall()
        all_innodb = all(row[0] == "InnoDB" for row in engines)
        if all_innodb:
            print(f"✅ CHECK 3: All tables use InnoDB engine")
            checks_passed += 1
        else:
            print(f"❌ CHECK 3: Non-InnoDB tables found: {engines}")
            checks_failed += 1
        
        # --- CHECK 4: Buffer Pool ---
        cursor.execute("SHOW VARIABLES LIKE 'innodb_buffer_pool_size'")
        pool_bytes = int(cursor.fetchone()[1])
        pool_gb = round(pool_bytes / (1024**3), 1)
        if pool_gb >= 2.0:
            print(f"✅ CHECK 4: Buffer pool = {pool_gb} GB (adequate)")
            checks_passed += 1
        else:
            print(f"⚠️  CHECK 4: Buffer pool = {pool_gb} GB (consider increasing)")
            checks_failed += 1
        
        # --- CHECK 5: Buffer Pool Hit Ratio ---
        cursor.execute("""
            SELECT 
                VARIABLE_NAME, VARIABLE_VALUE 
            FROM information_schema.GLOBAL_STATUS 
            WHERE VARIABLE_NAME IN (
                'Innodb_buffer_pool_read_requests',
                'Innodb_buffer_pool_reads'
            )
        """)
        stats = dict(cursor.fetchall())
        read_requests = int(stats.get("Innodb_buffer_pool_read_requests", 0))
        disk_reads = int(stats.get("Innodb_buffer_pool_reads", 0))
        
        if read_requests > 0:
            hit_ratio = round((1 - disk_reads / read_requests) * 100, 2)
            if hit_ratio >= 99.0:
                print(f"✅ CHECK 5: Buffer pool hit ratio = {hit_ratio}% (excellent)")
                checks_passed += 1
            elif hit_ratio >= 95.0:
                print(f"⚠️  CHECK 5: Buffer pool hit ratio = {hit_ratio}% (acceptable)")
                checks_passed += 1
            else:
                print(f"❌ CHECK 5: Buffer pool hit ratio = {hit_ratio}% (needs tuning)")
                checks_failed += 1
        else:
            print(f"ℹ️  CHECK 5: Buffer pool hit ratio — not enough data yet")
            checks_passed += 1
        
        # --- CHECK 6: Connections ---
        cursor.execute("SHOW VARIABLES LIKE 'max_connections'")
        max_conn = int(cursor.fetchone()[1])
        cursor.execute("SHOW STATUS LIKE 'Threads_connected'")
        current_conn = int(cursor.fetchone()[1])
        usage_pct = round((current_conn / max_conn) * 100, 1)
        
        if usage_pct < 80:
            print(f"✅ CHECK 6: Connections = {current_conn}/{max_conn} ({usage_pct}% used)")
            checks_passed += 1
        else:
            print(f"❌ CHECK 6: Connections = {current_conn}/{max_conn} ({usage_pct}% — HIGH)")
            checks_failed += 1
        
        # --- CHECK 7: Binary Logging ---
        cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        log_bin = cursor.fetchone()[1]
        if log_bin == "ON":
            print(f"✅ CHECK 7: Binary logging = ON (recovery enabled)")
            checks_passed += 1
        else:
            print(f"⚠️  CHECK 7: Binary logging = OFF (no point-in-time recovery)")
            checks_failed += 1
        
        # --- CHECK 8: Slow Query Log ---
        cursor.execute("SHOW VARIABLES LIKE 'slow_query_log'")
        slow_log = cursor.fetchone()[1]
        if slow_log == "ON":
            print(f"✅ CHECK 8: Slow query log = ON")
            checks_passed += 1
        else:
            print(f"⚠️  CHECK 8: Slow query log = OFF (enable for debugging)")
            checks_failed += 1
        
        # --- CHECK 9: Table Row Counts ---
        print(f"\n📊 TABLE STATISTICS:")
        cursor.execute("""
            SELECT 
                TABLE_NAME,
                TABLE_ROWS,
                ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS total_mb
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'quickcart_db'
            ORDER BY TABLE_NAME
        """)
        for row in cursor.fetchall():
            print(f"   {row[0]:<20} {row[1]:>8} rows    {row[2]:>8} MB")
        checks_passed += 1
        
        # --- CHECK 10: Uptime ---
        cursor.execute("SHOW STATUS LIKE 'Uptime'")
        uptime_seconds = int(cursor.fetchone()[1])
        uptime_hours = round(uptime_seconds / 3600, 1)
        uptime_days = round(uptime_seconds / 86400, 1)
        print(f"\n⏰ Uptime: {uptime_days} days ({uptime_hours} hours)")
        checks_passed += 1
        
        conn.close()
        
    except pymysql.Error as e:
        print(f"\n❌ CONNECTION FAILED: {e}")
        checks_failed += 1
    
    # --- SUMMARY ---
    print("\n" + "=" * 60)
    total = checks_passed + checks_failed
    print(f"📋 HEALTH CHECK SUMMARY: {checks_passed}/{total} passed")
    if checks_failed == 0:
        print("🟢 STATUS: HEALTHY")
    elif checks_failed <= 2:
        print("🟡 STATUS: NEEDS ATTENTION")
    else:
        print("🔴 STATUS: UNHEALTHY — ACTION REQUIRED")
    print("=" * 60)
    
    return 0 if checks_failed == 0 else 1


if __name__ == "__main__":
    exit_code = check_health()
    sys.exit(exit_code)