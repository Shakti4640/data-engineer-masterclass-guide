# file: 38_02_spectrum_monitor.py
# Purpose: Monitor Spectrum usage and costs
# Run from: Scheduled Lambda or cron for daily reporting
# NOTE: Requires Redshift Data API or psycopg2 for actual execution

import boto3
from datetime import datetime

AWS_REGION = "us-east-2"

# Redshift Data API (serverless way to query Redshift)
redshift_data = boto3.client("redshift-data", region_name=AWS_REGION)

CLUSTER_ID = "quickcart-dwh"
DATABASE = "quickcart_warehouse"
DB_USER = "admin"


def run_redshift_query(sql, description=""):
    """Execute query via Redshift Data API and return results"""
    if description:
        print(f"\n   📊 {description}")

    response = redshift_data.execute_statement(
        ClusterIdentifier=CLUSTER_ID,
        Database=DATABASE,
        DbUser=DB_USER,
        Sql=sql
    )

    statement_id = response["Id"]

    # Wait for completion
    import time
    while True:
        status = redshift_data.describe_statement(Id=statement_id)
        state = status["Status"]
        if state in ("FINISHED", "FAILED", "ABORTED"):
            break
        time.sleep(2)

    if state == "FINISHED":
        result = redshift_data.get_statement_result(Id=statement_id)
        return result
    else:
        error = status.get("Error", "Unknown error")
        print(f"   ❌ Query failed: {error}")
        return None


def check_spectrum_usage():
    """Check daily Spectrum scan volume and estimated costs"""
    print("=" * 70)
    print("📊 REDSHIFT SPECTRUM USAGE REPORT")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Daily scan volume
    result = run_redshift_query("""
        SELECT 
            DATE_TRUNC('day', starttime) AS scan_date,
            COUNT(*) AS spectrum_queries,
            ROUND(SUM(s3_scanned_bytes) / (1024.0*1024*1024), 2) AS gb_scanned,
            ROUND(SUM(s3_scanned_bytes) / (1024.0*1024*1024*1024) * 5, 4) AS estimated_cost_usd,
            ROUND(AVG(s3_scanned_bytes) / (1024.0*1024), 1) AS avg_mb_per_query,
            SUM(s3_scanned_rows) AS total_rows_scanned
        FROM svl_s3query_summary
        WHERE starttime >= CURRENT_DATE - 7
        GROUP BY DATE_TRUNC('day', starttime)
        ORDER BY scan_date DESC
    """, "Daily Spectrum scan volume (last 7 days)")

    if result:
        columns = [col["name"] for col in result["ColumnMetadata"]]
        print(f"\n   {' | '.join(f'{c:>18s}' for c in columns)}")
        print(f"   {'-' * (20 * len(columns))}")

        for row in result["Records"]:
            values = [str(field.get("stringValue", field.get("longValue", field.get("doubleValue", "N/A")))) for field in row]
            print(f"   {' | '.join(f'{v:>18s}' for v in values)}")

    # Top schemas by scan volume
    result = run_redshift_query("""
        SELECT 
            SPLIT_PART(querytxt, '.', 1) AS schema_accessed,
            COUNT(*) AS query_count,
            ROUND(SUM(s3_scanned_bytes) / (1024.0*1024*1024), 2) AS gb_scanned,
            ROUND(SUM(s3_scanned_bytes) / (1024.0*1024*1024*1024) * 5, 4) AS cost_usd
        FROM svl_s3query_summary s
        JOIN stl_query q ON s.query = q.query
        WHERE s.starttime >= CURRENT_DATE - 7
        GROUP BY SPLIT_PART(querytxt, '.', 1)
        ORDER BY gb_scanned DESC
        LIMIT 10
    """, "Top schemas by Spectrum scan volume")

    # Check for expensive queries
    result = run_redshift_query("""
        SELECT 
            q.query,
            q.userid,
            ROUND(s.s3_scanned_bytes / (1024.0*1024*1024), 2) AS gb_scanned,
            ROUND(s.s3_scanned_bytes / (1024.0*1024*1024*1024) * 5, 4) AS cost_usd,
            s.s3_scanned_rows,
            s.files AS files_scanned,
            SUBSTRING(q.querytxt, 1, 100) AS query_preview
        FROM svl_s3query_summary s
        JOIN stl_query q ON s.query = q.query
        WHERE s.starttime >= CURRENT_DATE - 1
          AND s.s3_scanned_bytes > 1073741824  -- > 1 GB
        ORDER BY s.s3_scanned_bytes DESC
        LIMIT 10
    """, "Expensive Spectrum queries (> 1 GB scanned, last 24h)")

    # External table inventory
    run_redshift_query("""
        SELECT schemaname, tablename, location
        FROM svv_external_tables
        ORDER BY schemaname, tablename
    """, "External table inventory")

    print("\n" + "=" * 70)
    print("📊 REPORT COMPLETE")
    print("=" * 70)


def print_optimization_recommendations():
    """Print Spectrum optimization tips based on usage patterns"""
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SPECTRUM OPTIMIZATION RECOMMENDATIONS                              ║
╚══════════════════════════════════════════════════════════════════════╝

  1. HIGH SCAN VOLUME ON ext_silver:
     → Analysts scanning full silver tables via Spectrum
     → Solution: Create gold CTAS tables for frequent queries (Project 36)
     → Point Spectrum at gold instead of silver

  2. REPEATED QUERIES ON SAME EXTERNAL TABLE:
     → Same Spectrum query runs 20+ times/day
     → Solution: Create Redshift Materialized View (Project 63)
     → Or: Load as local table if accessed > 50 times/day

  3. MANY SMALL FILES IN EXTERNAL TABLE:
     → Spectrum overhead increases with file count
     → 1000 small files slower than 10 large files
     → Solution: Compact files (Project 62)

  4. CSV/JSON EXTERNAL TABLES:
     → Full file scan (no column pruning)
     → 10x more data scanned than Parquet
     → Solution: Convert to Parquet (Project 25)

  5. NO PARTITION PRUNING:
     → Check SVL_S3QUERY_SUMMARY → files column
     → If files = total files in table → no pruning happening
     → Solution: Ensure partition columns in WHERE clause
    """)


if __name__ == "__main__":
    import sys

    action = sys.argv[1] if len(sys.argv) > 1 else "report"

    if action == "report":
        check_spectrum_usage()
    elif action == "recommendations":
        print_optimization_recommendations()
    else:
        print("Usage: python 38_02_... [report|recommendations]")

    print("\n🏁 DONE")