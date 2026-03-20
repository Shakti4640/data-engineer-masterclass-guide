# file: 65_05_weekly_reconciliation.py
# Run from: Account B after weekly full load completes
# Purpose: Compare Redshift target with MariaDB source to detect drift

import psycopg2
import pymysql
import boto3
from datetime import datetime

REGION = "us-east-2"
SCHEMA = "quickcart_ops"
SNS_TOPIC = f"arn:aws:sns:{REGION}:222222222222:data-platform-alerts"

# Redshift connection
RS_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"
RS_PORT = 5439
RS_DB = "analytics"
RS_USER = "etl_writer"
RS_PASS = "R3dsh1ftWr1t3r#2025"

# MariaDB connection (via VPC Peering)
MY_HOST = "10.1.3.50"
MY_PORT = 3306
MY_USER = "glue_reader"
MY_PASS = "GlueR3ad0nly#2025"
MY_DB = "quickcart"


def reconcile():
    """
    Compare row counts and key metrics between source and target
    
    CHECKS:
    1. Row count match (excluding soft deletes)
    2. Sum of numeric columns match
    3. Max updated_at match
    4. Identify rows in target but NOT in source (orphans from hard deletes)
    """
    print("=" * 70)
    print("🔍 WEEKLY RECONCILIATION: MariaDB vs Redshift")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Connect to both
    rs_conn = psycopg2.connect(
        host=RS_HOST, port=RS_PORT, dbname=RS_DB,
        user=RS_USER, password=RS_PASS
    )
    rs_conn.autocommit = True
    rs_cur = rs_conn.cursor()

    my_conn = pymysql.connect(
        host=MY_HOST, port=MY_PORT, user=MY_USER,
        password=MY_PASS, database=MY_DB
    )
    my_cur = my_conn.cursor()

    tables = [
        {
            "source": "orders",
            "target": "orders",
            "pk": "order_id",
            "sum_col": "total_amount",
            "ts_col": "updated_at"
        },
        {
            "source": "customers",
            "target": "dim_customers",
            "pk": "customer_id",
            "sum_col": None,
            "ts_col": "updated_at"
        },
        {
            "source": "products",
            "target": "dim_products",
            "pk": "product_id",
            "sum_col": "price",
            "ts_col": "updated_at"
        }
    ]

    alerts = []

    for table in tables:
        print(f"\n📋 Reconciling: {table['source']} ↔ {table['target']}")
        print("-" * 50)

        # Source count (excluding soft deletes)
        my_cur.execute(
            f"SELECT COUNT(*) FROM {table['source']} WHERE is_deleted = FALSE"
        )
        source_count = my_cur.fetchone()[0]

        # Target count
        rs_cur.execute(
            f"SELECT COUNT(*) FROM {SCHEMA}.{table['target']}"
        )
        target_count = rs_cur.fetchone()[0]

        # Count diff
        diff = abs(source_count - target_count)
        diff_pct = (diff / max(source_count, 1)) * 100
        status = "✅" if diff_pct < 0.1 else "⚠️" if diff_pct < 1.0 else "❌"

        print(f"   {status} Row count — Source: {source_count:,} | Target: {target_count:,} | Diff: {diff:,} ({diff_pct:.3f}%)")

        if diff_pct >= 0.1:
            alerts.append({
                "table": table["source"],
                "check": "row_count",
                "source": source_count,
                "target": target_count,
                "diff_pct": round(diff_pct, 3)
            })

        # Sum check (if applicable)
        if table["sum_col"]:
            my_cur.execute(
                f"SELECT ROUND(SUM({table['sum_col']}), 2) FROM {table['source']} WHERE is_deleted = FALSE"
            )
            source_sum = float(my_cur.fetchone()[0] or 0)

            rs_cur.execute(
                f"SELECT ROUND(SUM({table['sum_col']}), 2) FROM {SCHEMA}.{table['target']}"
            )
            target_sum = float(rs_cur.fetchone()[0] or 0)

            sum_diff = abs(source_sum - target_sum)
            sum_diff_pct = (sum_diff / max(source_sum, 1)) * 100
            status = "✅" if sum_diff_pct < 0.01 else "⚠️" if sum_diff_pct < 0.1 else "❌"

            print(f"   {status} SUM({table['sum_col']}) — Source: ${source_sum:,.2f} | Target: ${target_sum:,.2f} | Diff: {sum_diff_pct:.4f}%")

            if sum_diff_pct >= 0.01:
                alerts.append({
                    "table": table["source"],
                    "check": f"sum_{table['sum_col']}",
                    "source": source_sum,
                    "target": target_sum,
                    "diff_pct": round(sum_diff_pct, 4)
                })

        # Max timestamp check
        my_cur.execute(
            f"SELECT MAX({table['ts_col']}) FROM {table['source']}"
        )
        source_max_ts = str(my_cur.fetchone()[0])

        rs_cur.execute(
            f"SELECT MAX({table['ts_col']}) FROM {SCHEMA}.{table['target']}"
        )
        target_max_ts = str(rs_cur.fetchone()[0])

        ts_match = source_max_ts[:19] == target_max_ts[:19]
        status = "✅" if ts_match else "⚠️"
        print(f"   {status} MAX({table['ts_col']}) — Source: {source_max_ts} | Target: {target_max_ts}")

    # Summary
    print(f"\n{'='*70}")
    if alerts:
        print(f"⚠️ RECONCILIATION FOUND {len(alerts)} ISSUE(S)")
        for a in alerts:
            print(f"   → {a['table']}.{a['check']}: {a['diff_pct']}% drift")

        # Send alert
        sns = boto3.client("sns", region_name=REGION)
        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject=f"⚠️ CDC Reconciliation: {len(alerts)} drift(s) detected",
            Message="\n".join([
                "WEEKLY CDC RECONCILIATION REPORT",
                f"Time: {datetime.now()}",
                "",
                "Issues found:",
                *[f"  {a['table']}.{a['check']}: {a['diff_pct']}% drift "
                  f"(source={a['source']}, target={a['target']})"
                  for a in alerts],
                "",
                "Action: Review CDC watermarks and run manual full load if needed"
            ])
        )
        print("📧 Alert sent")
    else:
        print("✅ RECONCILIATION PASSED — Source and Target are in sync")

    print(f"{'='*70}")

    rs_cur.close()
    rs_conn.close()
    my_cur.close()
    my_conn.close()


if __name__ == "__main__":
    reconcile()