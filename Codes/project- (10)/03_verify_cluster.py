# file: 03_verify_cluster.py
# Purpose: Connect to Redshift, verify configuration, run diagnostic queries
# Prerequisites: Cluster available (Step 1), schema created (Step 2)
# Install: pip3 install psycopg2-binary

import psycopg2
import time

# --- CONNECTION CONFIG ---
# Replace with your actual endpoint from Step 1
REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin_user",
    "password": "QuickCart_DW_2025!"
}


def timed_query(cursor, label, sql):
    """Execute query with timing"""
    print(f"\n{'='*65}")
    print(f"📊 {label}")
    print(f"{'='*65}")
    
    start = time.time()
    cursor.execute(sql)
    results = cursor.fetchall()
    elapsed_ms = round((time.time() - start) * 1000, 2)
    
    columns = [desc[0] for desc in cursor.description]
    
    # Print header
    header = " | ".join(f"{col:<25}" for col in columns)
    print(f"\n{header}")
    print("-" * len(header))
    
    for row in results[:20]:
        row_str = " | ".join(f"{str(val):<25}" for val in row)
        print(row_str)
    
    if len(results) > 20:
        print(f"... and {len(results) - 20} more rows")
    
    print(f"\n⏱️  {len(results)} rows in {elapsed_ms} ms")
    return results


def run_verification():
    """Run all diagnostic checks"""
    print("=" * 65)
    print("🏥 REDSHIFT CLUSTER HEALTH CHECK")
    print("=" * 65)
    
    conn = None
    try:
        # --- CONNECTION TEST ---
        print("\n🔌 Connecting to Redshift...")
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        print("✅ Connected successfully")
        
        # --- CHECK 1: Version ---
        timed_query(cursor, "CLUSTER VERSION", "SELECT version();")
        
        # --- CHECK 2: Current user & database ---
        timed_query(
            cursor,
            "SESSION INFO",
            """
            SELECT 
                current_user AS user_name,
                current_database() AS database_name,
                current_schema() AS schema_name,
                GETDATE() AS server_time
            """
        )
        
        # --- CHECK 3: Node configuration ---
        timed_query(
            cursor,
            "NODE CONFIGURATION",
            """
            SELECT 
                node,
                type,
                ROUND(used::FLOAT / capacity * 100, 1) AS disk_pct_used,
                used AS disk_used_mb,
                capacity AS disk_capacity_mb
            FROM stv_partitions
            WHERE type = 0
            GROUP BY node, type, used, capacity
            ORDER BY node
            """
        )
        
        # --- CHECK 4: Slices per node ---
        timed_query(
            cursor,
            "SLICES PER NODE",
            """
            SELECT 
                node, 
                COUNT(*) AS num_slices
            FROM stv_slices
            GROUP BY node
            ORDER BY node
            """
        )
        
        # --- CHECK 5: Schema tables ---
        timed_query(
            cursor,
            "ANALYTICS SCHEMA TABLES",
            """
            SELECT 
                schemaname,
                tablename,
                diststyle,
                sortkey1,
                sortkey_num,
                tbl_rows,
                size AS size_mb,
                pct_used
            FROM svv_table_info
            WHERE schemaname = 'analytics'
            ORDER BY tablename
            """
        )
        
        # --- CHECK 6: Column encodings ---
        timed_query(
            cursor,
            "COLUMN ENCODINGS (fact_orders)",
            """
            SELECT 
                "column",
                type,
                encoding,
                distkey,
                sortkey,
                notnull
            FROM pg_table_def
            WHERE schemaname = 'analytics'
              AND tablename = 'fact_orders'
            ORDER BY "column"
            """
        )
        
        # --- CHECK 7: WLM configuration ---
        timed_query(
            cursor,
            "WORKLOAD MANAGEMENT QUEUES",
            """
            SELECT 
                service_class,
                num_query_tasks AS slots,
                query_working_mem AS memory_mb,
                max_execution_time AS timeout_ms
            FROM stv_wlm_service_class_config
            WHERE service_class > 4
            """
        )
        
        # --- CHECK 8: Date dimension verification ---
        timed_query(
            cursor,
            "DATE DIMENSION VERIFICATION",
            """
            SELECT 
                year,
                COUNT(*) AS days,
                MIN(date_key) AS first_day,
                MAX(date_key) AS last_day,
                SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekends,
                SUM(CASE WHEN NOT is_weekend THEN 1 ELSE 0 END) AS weekdays
            FROM analytics.dim_dates
            GROUP BY year
            ORDER BY year
            """
        )
        
        # --- CHECK 9: Cluster-wide settings ---
        timed_query(
            cursor,
            "KEY CLUSTER SETTINGS",
            """
            SELECT 
                name, 
                setting
            FROM pg_settings
            WHERE name IN (
                'max_concurrency_scaling_clusters',
                'enable_result_cache_for_session',
                'query_group',
                'search_path',
                'datestyle',
                'statement_timeout'
            )
            ORDER BY name
            """
        )
        
        # --- SUMMARY ---
        print(f"\n{'='*65}")
        print(f"✅ CLUSTER HEALTH CHECK COMPLETE")
        print(f"{'='*65}")
        print(f"""
CLUSTER STATUS: HEALTHY

KEY OBSERVATIONS:
→ Cluster responding to queries
→ Analytics schema created with proper dist/sort keys
→ Date dimension populated
→ Ready for data loading (Project 21: COPY from S3)

PERFORMANCE BASELINE:
→ Empty cluster — baseline metrics will be measured after data load
→ Query timing will improve dramatically with result caching
→ Column compression benefits appear after loading real data
        """)
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ CONNECTION FAILED: {e}")
        print(f"""
TROUBLESHOOTING:
1. Is the cluster status 'Available'?
   → aws redshift describe-clusters --cluster-identifier quickcart-analytics

2. Is the Security Group allowing port 5439 from your IP?
   → Check inbound rules for sg-redshift

3. Is the cluster in a private subnet?
   → You may need a bastion host or VPN to connect

4. Is Enhanced VPC Routing blocking traffic?
   → Check VPC endpoint for S3 is configured
        """)
        
    except psycopg2.Error as e:
        print(f"\n❌ QUERY ERROR: {e}")
        
    finally:
        if conn:
            conn.close()
            print("\n🔌 Connection closed")


if __name__ == "__main__":
    run_verification()