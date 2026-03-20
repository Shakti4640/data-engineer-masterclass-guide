# file: 39_03_maintenance_schedule.py
# Purpose: Generate SQL for ongoing table maintenance
# Schedule: Add to Glue Workflow after nightly load (Project 35)
# Or: Run as a Redshift Scheduled Query

import boto3
from datetime import datetime

def generate_maintenance_sql():
    """Generate daily/weekly maintenance SQL"""

    print("=" * 70)
    print("🔧 REDSHIFT MAINTENANCE SQL")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- DAILY MAINTENANCE (run after nightly load completes)
-- Schedule: 3:00 AM UTC (after pipeline finishes at ~2:50 AM)
-- Duration: ~10-20 minutes
-- ═══════════════════════════════════════════════════════════════

-- Step 1: VACUUM DELETE ONLY (remove ghost rows — fast)
-- Removes deleted rows without re-sorting
-- Much faster than VACUUM FULL
VACUUM DELETE ONLY analytics.fact_orders;
VACUUM DELETE ONLY analytics.dim_customers;
VACUUM DELETE ONLY analytics.dim_products;

-- Step 2: ANALYZE (refresh statistics for optimizer)
-- Essential after DELETE+INSERT merge pattern
ANALYZE analytics.fact_orders;
ANALYZE analytics.dim_customers;
ANALYZE analytics.dim_products;

-- Step 3: Check maintenance results
SELECT
    "schema",
    "table",
    tbl_rows,
    tbl_rows - estimated_visible_rows AS ghost_rows,
    unsorted AS unsorted_pct,
    stats_off AS stats_staleness
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY "table";


-- ═══════════════════════════════════════════════════════════════
-- WEEKLY MAINTENANCE (Sunday 4:00 AM)
-- Includes SORT to maintain zone map effectiveness
-- Duration: ~30-60 minutes
-- ═══════════════════════════════════════════════════════════════

-- VACUUM SORT ONLY: re-sorts unsorted region
-- New rows from nightly INSERT land in unsorted region
-- After 7 days: ~7% unsorted → zone maps less effective
VACUUM SORT ONLY analytics.fact_orders;
VACUUM SORT ONLY analytics.dim_customers;

-- Full ANALYZE with column-level statistics
ANALYZE VERBOSE analytics.fact_orders;
ANALYZE VERBOSE analytics.dim_customers;
ANALYZE VERBOSE analytics.dim_products;


-- ═══════════════════════════════════════════════════════════════
-- MONTHLY MAINTENANCE (First Sunday of month, 3:00 AM)
-- Full VACUUM: delete + sort + reclaim
-- Duration: ~60-120 minutes
-- ═══════════════════════════════════════════════════════════════

VACUUM FULL analytics.fact_orders;
VACUUM FULL analytics.dim_customers;
VACUUM FULL analytics.dim_products;

ANALYZE analytics.fact_orders;
ANALYZE analytics.dim_customers;
ANALYZE analytics.dim_products;


-- ═══════════════════════════════════════════════════════════════
-- MONITORING: Check if maintenance is needed
-- Run anytime to check table health
-- ═══════════════════════════════════════════════════════════════

SELECT
    "schema",
    "table",
    diststyle,
    sortkey1,
    tbl_rows,
    estimated_visible_rows,
    CASE WHEN tbl_rows > 0
         THEN ROUND((tbl_rows - estimated_visible_rows)::FLOAT / tbl_rows * 100, 1)
         ELSE 0
    END AS bloat_pct,
    unsorted AS unsorted_pct,
    stats_off AS stats_stale_pct,
    size AS size_mb,
    -- Recommendations
    CASE WHEN (tbl_rows - estimated_visible_rows)::FLOAT / NULLIF(tbl_rows, 0) > 0.05
         THEN '⚠️ VACUUM DELETE needed'
         ELSE '✅ Clean' END AS vacuum_status,
    CASE WHEN unsorted > 10
         THEN '⚠️ VACUUM SORT needed'
         ELSE '✅ Sorted' END AS sort_status,
    CASE WHEN stats_off > 5
         THEN '⚠️ ANALYZE needed'
         ELSE '✅ Fresh' END AS stats_status
FROM svv_table_info
WHERE "schema" = 'analytics'
ORDER BY tbl_rows DESC;
    """)
    print("=" * 70)


def generate_glue_maintenance_script():
    """
    Generate a Glue Python Shell script for automated maintenance
    Add to Glue Workflow (Project 35) as final step after CTAS (Project 36)
    """
    print(f"""
# ═══════════════════════════════════════════════════════════════
# GLUE PYTHON SHELL SCRIPT FOR REDSHIFT MAINTENANCE
# Upload to: s3://quickcart-datalake-prod/scripts/glue/redshift_maintenance.py
# Add as final step in Glue Workflow (Project 35)
# ═══════════════════════════════════════════════════════════════

import sys
import boto3
import time
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Use Redshift Data API (no JDBC connection needed)
redshift_data = boto3.client('redshift-data', region_name='us-east-2')

CLUSTER_ID = 'quickcart-dwh'
DATABASE = 'quickcart_warehouse'
DB_USER = 'admin'

def run_sql(sql, description):
    print(f"Running: {{description}}")
    response = redshift_data.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=sql
    )
    stmt_id = response['Id']
    
    while True:
        status = redshift_data.describe_statement(Id=stmt_id)
        state = status['Status']
        if state in ('FINISHED', 'FAILED', 'ABORTED'):
            break
        time.sleep(5)
    
    if state == 'FINISHED':
        print(f"  ✅ {{description}}: completed")
    else:
        error = status.get('Error', 'Unknown')
        print(f"  ❌ {{description}}: {{error}}")

# Daily maintenance
run_sql('VACUUM DELETE ONLY analytics.fact_orders', 'VACUUM DELETE fact_orders')
run_sql('VACUUM DELETE ONLY analytics.dim_customers', 'VACUUM DELETE dim_customers')
run_sql('ANALYZE analytics.fact_orders', 'ANALYZE fact_orders')
run_sql('ANALYZE analytics.dim_customers', 'ANALYZE dim_customers')
run_sql('ANALYZE analytics.dim_products', 'ANALYZE dim_products')

print('✅ Daily maintenance complete')
    """)


if __name__ == "__main__":
    import sys
    action = sys.argv[1] if len(sys.argv) > 1 else "maintenance"

    if action == "maintenance":
        generate_maintenance_sql()
    elif action == "glue-script":
        generate_glue_maintenance_script()
    else:
        print("Usage: python 39_03_... [maintenance|glue-script]")