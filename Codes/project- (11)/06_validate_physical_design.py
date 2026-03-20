# file: 06_validate_physical_design.py
# Purpose: PROVE distribution, sort keys, and encodings are optimal
# This is what the CTO asked for: "numbers, not assumptions"

import psycopg2
import time

REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin_user",
    "password": "[REDACTED:PASSWORD]"
}


def validate_distribution(cursor):
    """
    Check data distribution across slices
    Skew > 2.0 = serious problem (one slice has 2x more data than another)
    """
    print(f"\n{'='*70}")
    print(f"📊 DISTRIBUTION VALIDATION — Is data evenly spread?")
    print(f"{'='*70}")
    
    cursor.execute("""
        SELECT 
            "schema",
            "table",
            diststyle,
            skew_rows,
            CASE 
                WHEN skew_rows <= 1.2 THEN '🟢 EXCELLENT'
                WHEN skew_rows <= 1.5 THEN '🟡 ACCEPTABLE'
                WHEN skew_rows <= 2.0 THEN '🟠 CONCERNING'
                ELSE '🔴 CRITICAL SKEW'
            END AS skew_assessment,
            tbl_rows,
            size AS size_mb
        FROM svv_table_info
        WHERE "schema" IN ('analytics', 'agg')
        ORDER BY skew_rows DESC
    """)
    
    columns = [desc[0] for desc in cursor.description]
    print(f"\n{'Schema':<12} {'Table':<25} {'DistStyle':<12} {'Skew':<6} {'Assessment':<20} {'Rows':<10} {'MB':<6}")
    print("-" * 95)
    
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))
        print(f"{str(row_dict['schema']):<12} "
              f"{str(row_dict['table']):<25} "
              f"{str(row_dict['diststyle']):<12} "
              f"{str(row_dict['skew_rows']):<6} "
              f"{str(row_dict['skew_assessment']):<20} "
              f"{str(row_dict['tbl_rows']):<10} "
              f"{str(row_dict['size_mb']):<6}")
    
    print(f"""
INTERPRETATION:
→ DISTSTYLE ALL tables: skew = 1.0 always (full copy on each node)
→ DISTKEY tables: skew depends on key cardinality and distribution
→ customer_id as DISTKEY: should be even if customers have similar order counts
→ If skew > 2.0: consider changing DISTKEY or using EVEN distribution
    """)


def validate_sort_keys(cursor):
    """
    Check sort key effectiveness
    High 'unsorted' percentage = zone maps are ineffective
    """
    print(f"\n{'='*70}")
    print(f"📊 SORT KEY VALIDATION — Are zone maps working?")
    print(f"{'='*70}")
    
    cursor.execute("""
        SELECT 
            "schema",
            "table",
            sortkey1,
            sortkey_num,
            unsorted,
            CASE 
                WHEN unsorted <= 5 THEN '🟢 EXCELLENT'
                WHEN unsorted <= 20 THEN '🟡 ACCEPTABLE'
                WHEN unsorted <= 40 THEN '🟠 VACUUM NEEDED'
                ELSE '🔴 SEVERELY UNSORTED'
            END AS sort_assessment,
            vacuum_sort_benefit
        FROM svv_table_info
        WHERE "schema" IN ('analytics', 'agg')
          AND sortkey1 IS NOT NULL
        ORDER BY unsorted DESC
    """)
    
    columns = [desc[0] for desc in cursor.description]
    print(f"\n{'Table':<25} {'SortKey':<15} {'#Keys':<6} {'Unsorted%':<10} {'Assessment':<20} {'VacuumBenefit'}")
    print("-" * 100)
    
    for row in cursor.fetchall():
        rd = dict(zip(columns, row))
        print(f"{str(rd['table']):<25} "
              f"{str(rd['sortkey1']):<15} "
              f"{str(rd['sortkey_num']):<6} "
              f"{str(rd['unsorted']):<10} "
              f"{str(rd['sort_assessment']):<20} "
              f"{str(rd['vacuum_sort_benefit'])}")
    
    print(f"""
INTERPRETATION:
→ unsorted < 5%: zone maps are highly effective — blocks are well-ordered
→ unsorted 5-20%: acceptable, some new data loaded out of order
→ unsorted > 20%: run VACUUM SORT ONLY <table>; to re-sort
→ After VACUUM SORT: zone maps become tight → queries skip more blocks
    """)


def validate_encodings(cursor):
    """
    Check if current encodings match recommended encodings
    ANALYZE COMPRESSION compares current vs optimal
    """
    print(f"\n{'='*70}")
    print(f"📊 ENCODING VALIDATION — Is compression optimal?")
    print(f"{'='*70}")
    
    tables_to_check = [
        "analytics.fact_orders",
        "analytics.fact_order_items",
        "analytics.dim_customers_scd2"
    ]
    
    for table in tables_to_check:
        print(f"\n   📦 {table}:")
        
        try:
            cursor.execute(f"ANALYZE COMPRESSION {table}")
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            print(f"   {'Column':<25} {'Current':<15} {'Recommended':<15} {'Est.Reduction%':<15} {'Match?'}")
            print(f"   {'-'*75}")
            
            for row in results:
                rd = dict(zip(columns, row))
                col_name = rd.get("Column", rd.get("column", "?"))
                current_enc = rd.get("Current_Encoding", rd.get("current_encoding", "?"))
                recommended = rd.get("Recommended_Encoding", rd.get("recommended_encoding", "?"))
                reduction = rd.get("Est_Reduction_Pct", rd.get("est_reduction_pct", "?"))
                
                match = "✅" if str(current_enc).lower() == str(recommended).lower() else "⚠️  SUBOPTIMAL"
                
                print(f"   {str(col_name):<25} "
                      f"{str(current_enc):<15} "
                      f"{str(recommended):<15} "
                      f"{str(reduction):<15} "
                      f"{match}")
                      
        except psycopg2.Error as e:
            print(f"   ⚠️  Cannot analyze: {e}")
    
    print(f"""
INTERPRETATION:
→ If Current = Recommended: encoding is optimal ✅
→ If different: consider DEEP COPY to apply recommended encoding
   Deep copy: CREATE new_table (with encoding) → INSERT SELECT → rename
→ Est_Reduction_Pct: how much smaller data could be with optimal encoding
→ For small tables: encoding difference is negligible
→ For billion-row tables: even 5% better compression saves terabytes
    """)


def validate_query_performance(cursor):
    """
    Run key queries and show EXPLAIN plans
    Prove distribution and sort keys are actually being used
    """
    print(f"\n{'='*70}")
    print(f"📊 QUERY PERFORMANCE VALIDATION")
    print(f"{'='*70}")
    
    test_queries = [
        {
            "name": "Date range filter (should use sort key zone maps)",
            "sql": """
                EXPLAIN
                SELECT status, COUNT(*), SUM(total_amount)
                FROM analytics.fact_orders
                WHERE order_date BETWEEN '2025-01-01' AND '2025-03-31'
                GROUP BY status
            """,
            "look_for": ["Filter", "sort", "zone"]
        },
        {
            "name": "Customer JOIN (should show DS_DIST_ALL_NONE — no shuffle)",
            "sql": """
                EXPLAIN
                SELECT c.tier, SUM(f.total_amount) 
                FROM analytics.fact_orders f
                JOIN analytics.dim_customers_scd2 c 
                    ON f.customer_sk = c.surrogate_key
                WHERE c.is_current = TRUE
                GROUP BY c.tier
            """,
            "look_for": ["DS_DIST_ALL_NONE", "DS_DIST_BOTH"]
        },
        {
            "name": "Wide fact (should have NO JOINs — all data in one table)",
            "sql": """
                EXPLAIN
                SELECT customer_tier, order_month_name,
                       COUNT(*), SUM(total_amount)
                FROM analytics.fact_orders_wide
                WHERE order_date BETWEEN '2025-01-01' AND '2025-06-30'
                GROUP BY customer_tier, order_month_name
            """,
            "look_for": ["Seq Scan", "no join"]
        },
        {
            "name": "Aggregate table (should be tiny scan — pre-computed)",
            "sql": """
                EXPLAIN
                SELECT date_key, SUM(total_revenue)
                FROM agg.agg_daily_revenue
                WHERE date_key BETWEEN '2025-01-01' AND '2025-06-30'
                GROUP BY date_key
            """,
            "look_for": ["Seq Scan", "small rows"]
        }
    ]
    
    for tq in test_queries:
        print(f"\n   📊 {tq['name']}")
        print(f"   {'-'*60}")
        
        cursor.execute(tq["sql"])
        plan_rows = cursor.fetchall()
        
        for row in plan_rows:
            plan_line = str(row[0]).strip()
            # Highlight important keywords
            highlighted = plan_line
            for keyword in ["DS_DIST_ALL_NONE", "DS_DIST_BOTH", "DS_BCAST", 
                           "Filter:", "Sort Key", "Hash Join"]:
                if keyword in plan_line:
                    highlighted = f"→ {plan_line}  ◄◄◄"
                    break
            print(f"   {highlighted}")
    
    print(f"""
EXPLAIN PLAN KEY TERMS:
→ DS_DIST_ALL_NONE: dimension is DISTSTYLE ALL — NO redistribution needed ✅
→ DS_DIST_BOTH: both tables have same DISTKEY — co-located join ✅
→ DS_BCAST_INNER: inner table broadcast to all nodes — redistribution ⚠️
→ DS_DIST_ALL_INNER: inner table redistributed — expensive ❌
→ Filter: zone map elimination is happening ✅
→ Hash Join vs Merge Join: both are fine for different scenarios
    """)


def run_full_validation():
    """Run all validations"""
    print("=" * 70)
    print("🔍 COMPLETE PHYSICAL DESIGN VALIDATION")
    print("   Proving distribution, sort keys, and encodings are optimal")
    print("=" * 70)
    
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    validate_distribution(cursor)
    validate_sort_keys(cursor)
    validate_encodings(cursor)
    validate_query_performance(cursor)
    
    print(f"\n{'='*70}")
    print(f"✅ VALIDATION COMPLETE")
    print(f"{'='*70}")
    print(f"""
ACTIONS IF ISSUES FOUND:
─────────────────────────
1. High skew (> 2.0):
   → Change DISTKEY to higher cardinality column
   → Or use DISTSTYLE EVEN

2. High unsorted (> 20%):
   → VACUUM SORT ONLY analytics.fact_orders;

3. Suboptimal encoding:
   → Deep copy: CREATE new_table → INSERT SELECT → rename

4. DS_BCAST in EXPLAIN:
   → Consider changing DISTKEY or DISTSTYLE ALL for smaller table
    """)
    
    conn.close()
    print("🔌 Connection closed")


if __name__ == "__main__":
    run_full_validation()