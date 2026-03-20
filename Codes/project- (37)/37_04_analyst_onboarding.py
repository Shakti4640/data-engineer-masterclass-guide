# file: 37_04_analyst_onboarding.py
# Purpose: Generate documentation for analyst onboarding
# Auto-discovers views and generates usage guide

import boto3
import json

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"

glue = boto3.client("glue", region_name=AWS_REGION)
athena = boto3.client("athena", region_name=AWS_REGION)

VIEWS_DB = "reporting_views"
GOLD_DB = "gold_quickcart"
SILVER_DB = "silver_quickcart"


def generate_onboarding_guide():
    """
    Auto-generate analyst onboarding documentation
    
    Discovers all views, named queries, and workgroups
    Produces a guide analysts can follow on Day 1
    """
    print("=" * 70)
    print("📖 ANALYST ONBOARDING GUIDE — AUTO-GENERATED")
    print("=" * 70)

    # ── Section 1: Available Databases ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 1: DATABASE OVERVIEW                                       ║
╚══════════════════════════════════════════════════════════════════════╝

  You have access to 3 databases in Athena:

  ┌────────────────────────┬────────────────────────────────────────────┐
  │ Database               │ Purpose                                    │
  ├────────────────────────┼────────────────────────────────────────────┤
  │ reporting_views        │ START HERE — standardized views            │
  │                        │ Pre-joined, filtered, consistent metrics   │
  │                        │ Fastest to query, easiest to understand    │
  ├────────────────────────┼────────────────────────────────────────────┤
  │ gold_quickcart         │ Pre-computed summary tables                │
  │                        │ Tiny data, instant queries, nearly free    │
  │                        │ Refreshed nightly at ~2:50 AM              │
  ├────────────────────────┼────────────────────────────────────────────┤
  │ silver_quickcart       │ Full detailed data — use with caution      │
  │                        │ Large tables (15+ GB), costs $0.075/query  │
  │                        │ Use when views/gold don't have what you    │
  │                        │ need                                       │
  └────────────────────────┴────────────────────────────────────────────┘

  RULE OF THUMB:
  → Start with reporting_views (views)
  → If not enough detail → try gold_quickcart (summaries)
  → Only go to silver_quickcart when absolutely necessary
    """)

    # ── Section 2: Available Views ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 2: AVAILABLE VIEWS                                         ║
╚══════════════════════════════════════════════════════════════════════╝
    """)

    try:
        response = glue.get_tables(DatabaseName=VIEWS_DB)
        tables = response.get("TableList", [])

        gold_backed = []
        silver_backed = []

        for table in tables:
            name = table.get("Name", "")
            table_type = table.get("TableType", "")
            view_text = table.get("ViewOriginalText", "")

            if table_type != "VIRTUAL_VIEW":
                continue

            # Determine backing layer
            if GOLD_DB in view_text:
                gold_backed.append(name)
                cost_label = "💚 FREE (gold-backed, < $0.001/query)"
            else:
                silver_backed.append(name)
                cost_label = "🟡 MODERATE (silver-backed, ~$0.075/query)"

            # Get columns
            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            col_names = [c["Name"] for c in columns[:8]]
            col_str = ", ".join(col_names)
            if len(columns) > 8:
                col_str += f", ... (+{len(columns) - 8} more)"

            print(f"  📊 {name}")
            print(f"     Cost: {cost_label}")
            print(f"     Columns: {col_str}")
            print(f"     Example: SELECT * FROM {VIEWS_DB}.{name} LIMIT 10")
            print()

        print(f"  Summary: {len(gold_backed)} gold-backed (free), "
              f"{len(silver_backed)} silver-backed (paid)")

    except Exception as e:
        print(f"  Error listing views: {e}")

    # ── Section 3: Cost Guide ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 3: COST GUIDE — READ THIS CAREFULLY                       ║
╚══════════════════════════════════════════════════════════════════════╝

  Athena charges $5 per TB of data scanned.

  ┌────────────────────────────────────────┬──────────────┬───────────┐
  │ What You Query                         │ Data Scanned │ Cost      │
  ├────────────────────────────────────────┼──────────────┼───────────┤
  │ v_revenue_summary (gold-backed)        │ 3 KB         │ $0.00005  │
  │ v_customer_segments (gold-backed)      │ 1 KB         │ $0.00005  │
  │ v_product_leaderboard (gold-backed)    │ 50 KB        │ $0.00005  │
  │ v_conversion_trend (gold-backed)       │ 5 KB         │ $0.00005  │
  │ v_completed_order_details (silver)     │ 15 GB        │ $0.075    │
  │ v_all_order_details (silver)           │ 15 GB        │ $0.075    │
  │ v_customer_360 (silver)                │ 2 GB         │ $0.010    │
  └────────────────────────────────────────┴──────────────┴───────────┘

  COST-SAVING TIPS:
  1. Use gold-backed views (v_revenue_summary, etc.) — virtually free
  2. Always add WHERE year = '2025' — reduces scan by partition pruning
  3. Use LIMIT when exploring data — doesn't reduce scan but limits output
  4. Never do SELECT * on silver tables without a WHERE clause
  5. If you run the same query 5+ times/day, ask Data Engineering
     to create a gold table for it
    """)

    # ── Section 4: Workgroup Assignment ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 4: YOUR WORKGROUP                                          ║
╚══════════════════════════════════════════════════════════════════════╝

  You are assigned to a workgroup that controls your query costs.
  
  To set your workgroup in Athena Console:
  → Top-right dropdown → Select your workgroup
  
  Workgroup settings (enforced — you cannot override):
  → Query scan limit: set per workgroup
  → Result location: auto-configured
  → If your query exceeds the scan limit: it will be CANCELLED
  → This protects you from accidental expensive queries
  
  If you need a higher limit for a specific analysis:
  → Contact Data Engineering with your query
  → We'll either increase your limit or create a gold table
    """)

    # ── Section 5: Quick Start Queries ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 5: QUICK START — COPY AND RUN THESE                       ║
╚══════════════════════════════════════════════════════════════════════╝

  -- Query 1: What's our revenue by category this year? (FREE)
  SELECT category, SUM(total_revenue) AS revenue, SUM(order_count) AS orders
  FROM reporting_views.v_revenue_summary
  WHERE year = '2025'
  GROUP BY category
  ORDER BY revenue DESC;

  -- Query 2: What are our customer segments? (FREE)
  SELECT segment, tier, customer_count, avg_lifetime_value, revenue_share_pct
  FROM reporting_views.v_customer_segments
  ORDER BY total_segment_revenue DESC;

  -- Query 3: Top 10 products? (FREE)
  SELECT product_name, category, total_revenue, unique_buyers
  FROM reporting_views.v_product_leaderboard
  WHERE rank_in_category <= 3
  ORDER BY total_revenue DESC
  LIMIT 10;

  -- Query 4: Conversion trend last 7 days? (FREE)
  SELECT date, completion_rate, total_revenue, revenue_change
  FROM reporting_views.v_conversion_trend
  ORDER BY date DESC
  LIMIT 7;

  -- Query 5: Detailed orders for specific category (COSTS ~$0.075)
  SELECT order_id, customer_name, product_name, revenue, order_date
  FROM reporting_views.v_completed_order_details
  WHERE product_category = 'Electronics'
    AND year = '2025' AND month = '01'
  ORDER BY revenue DESC
  LIMIT 100;
    """)

    # ── Section 6: Named Queries ──
    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║  SECTION 6: SAVED QUERY LIBRARY                                     ║
╚══════════════════════════════════════════════════════════════════════╝

  We maintain a library of common analysis queries.
  
  To access:
  → Athena Console → "Saved queries" tab
  → Browse by category (Revenue, Customers, Operations, DQ)
  → Click to open → modify parameters → run

  Available categories:
  → Revenue: Monthly by Category, Top N Products, YoY Comparison
  → Customers: Segment Distribution, Health Dashboard, At-Risk
  → Operations: Conversion Trend, Daily Summary by City
  → DQ: NULL Check, Row Count Comparison
    """)

    print("=" * 70)
    print("📖 END OF ONBOARDING GUIDE")
    print("=" * 70)


def export_guide_to_s3():
    """Export the onboarding guide as a text file to S3 for sharing"""
    import io
    import sys

    # Capture print output
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()

    generate_onboarding_guide()

    sys.stdout = old_stdout
    guide_content = buffer.getvalue()

    # Upload to S3
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="documentation/analyst_onboarding_guide.txt",
        Body=guide_content,
        ContentType="text/plain"
    )

    print(f"✅ Guide exported to: s3://{S3_BUCKET}/documentation/analyst_onboarding_guide.txt")
    print(f"   Share this URL with new analysts")


if __name__ == "__main__":
    import sys

    action = sys.argv[1] if len(sys.argv) > 1 else "print"

    if action == "print":
        generate_onboarding_guide()
    elif action == "export":
        export_guide_to_s3()
    else:
        print("Usage: python 37_04_... [print|export]")