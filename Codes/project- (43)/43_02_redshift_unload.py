# file: 43_02_redshift_unload.py
# Purpose: UNLOAD aggregated results from Redshift to S3
# Builds on Project 22 (UNLOAD basics)

import boto3
import psycopg2
import time
from datetime import datetime

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
REDSHIFT_IAM_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-redshift-s3-role"

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def unload_customer_metrics():
    """
    UNLOAD customer_metrics aggregation to S3 as Parquet.
    
    This query computes:
    → Lifetime value (sum of all orders)
    → Churn risk (simplified: days since last order / 365)
    → Customer segment (based on lifetime value tiers)
    → Total orders and average order value
    """
    today = datetime.now().strftime("%Y-%m-%d")
    s3_prefix = f"s3://{BUCKET_NAME}/reverse_etl/customer_metrics/dt={today}/"
    computed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    aggregation_query = f"""
        SELECT
            c.customer_id,
            COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
            -- Simplified churn risk: longer since last order = higher risk
            CASE
                WHEN MAX(o.order_date) IS NULL THEN 0.9500
                WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE) > 180 THEN 0.8000
                WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE) > 90  THEN 0.5000
                WHEN DATEDIFF(day, MAX(o.order_date), CURRENT_DATE) > 30  THEN 0.2000
                ELSE 0.0500
            END AS churn_risk,
            CASE
                WHEN COALESCE(SUM(o.total_amount), 0) > 5000 THEN 'platinum'
                WHEN COALESCE(SUM(o.total_amount), 0) > 1000 THEN 'gold'
                WHEN COALESCE(SUM(o.total_amount), 0) > 200  THEN 'silver'
                ELSE 'bronze'
            END AS segment,
            COUNT(o.order_id) AS total_orders,
            COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
            '{computed_at}'::timestamp AS computed_at
        FROM public.customers c
        LEFT JOIN public.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
    """

    unload_sql = f"""
        UNLOAD ('{aggregation_query.replace(chr(39), chr(39)+chr(39))}')
        TO '{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT PARQUET
        ALLOWOVERWRITE
        PARALLEL OFF;
    """

    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    print(f"📤 UNLOAD customer_metrics → {s3_prefix}")
    start = time.time()
    cursor.execute(unload_sql)
    duration = time.time() - start

    cursor.close()
    conn.close()

    print(f"   ✅ UNLOAD complete in {duration:.1f}s")

    # Verify files in S3
    s3_client = boto3.client("s3", region_name=REGION)
    prefix = f"reverse_etl/customer_metrics/dt={today}/"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    files = response.get("Contents", [])
    total_size = sum(f["Size"] for f in files)

    print(f"   📦 Files: {len(files)}, Total size: {round(total_size/1024, 1)} KB")

    return s3_prefix, len(files)


def unload_customer_recommendations():
    """
    UNLOAD product recommendations to S3.
    
    Simplified recommendation logic:
    → For each customer, rank products by purchase frequency in same category
    → Top 5 products per customer
    → Score based on normalized purchase frequency
    
    In production: this would be an ML model output table.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    s3_prefix = f"s3://{BUCKET_NAME}/reverse_etl/customer_recommendations/dt={today}/"
    computed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    recommendation_query = f"""
        WITH customer_category_prefs AS (
            SELECT
                o.customer_id,
                p.category,
                COUNT(*) AS purchase_count
            FROM public.orders o
            JOIN public.products p ON o.product_id = p.product_id
            WHERE o.status != 'cancelled'
            GROUP BY o.customer_id, p.category
        ),
        category_products AS (
            SELECT
                p.product_id,
                p.category,
                AVG(p.price) AS avg_price
            FROM public.products p
            WHERE p.is_active = true
            GROUP BY p.product_id, p.category
        ),
        scored AS (
            SELECT
                ccp.customer_id,
                cp.product_id,
                -- Score: normalized purchase frequency in category
                CAST(ccp.purchase_count AS FLOAT) /
                    NULLIF(SUM(ccp.purchase_count) OVER (PARTITION BY ccp.customer_id), 0)
                    AS score,
                ROW_NUMBER() OVER (
                    PARTITION BY ccp.customer_id
                    ORDER BY ccp.purchase_count DESC, cp.avg_price DESC
                ) AS rank
            FROM customer_category_prefs ccp
            JOIN category_products cp ON ccp.category = cp.category
            -- Exclude products already purchased
            WHERE NOT EXISTS (
                SELECT 1 FROM public.orders o2
                WHERE o2.customer_id = ccp.customer_id
                AND o2.product_id = cp.product_id
            )
        )
        SELECT
            customer_id,
            product_id,
            ROUND(score, 4) AS score,
            rank,
            '{computed_at}'::timestamp AS computed_at
        FROM scored
        WHERE rank <= 5
    """

    unload_sql = f"""
        UNLOAD ('{recommendation_query.replace(chr(39), chr(39)+chr(39))}')
        TO '{s3_prefix}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT PARQUET
        ALLOWOVERWRITE;
    """
    # Note: PARALLEL ON (default) for recommendations — larger dataset

    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    print(f"\n📤 UNLOAD customer_recommendations → {s3_prefix}")
    start = time.time()
    cursor.execute(unload_sql)
    duration = time.time() - start

    cursor.close()
    conn.close()

    print(f"   ✅ UNLOAD complete in {duration:.1f}s")

    # Verify
    s3_client = boto3.client("s3", region_name=REGION)
    prefix = f"reverse_etl/customer_recommendations/dt={today}/"
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    files = response.get("Contents", [])
    total_size = sum(f["Size"] for f in files)

    print(f"   📦 Files: {len(files)}, Total size: {round(total_size/1024, 1)} KB")

    return s3_prefix, len(files)


def run_all_unloads():
    """Run all reverse ETL UNLOAD operations"""
    print("=" * 70)
    print(f"📤 REDSHIFT UNLOAD — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    results = {}

    try:
        prefix, count = unload_customer_metrics()
        results["customer_metrics"] = {"s3_prefix": prefix, "files": count, "status": "SUCCESS"}
    except Exception as e:
        print(f"   ❌ customer_metrics UNLOAD failed: {e}")
        results["customer_metrics"] = {"status": "FAILED", "error": str(e)}

    try:
        prefix, count = unload_customer_recommendations()
        results["customer_recommendations"] = {"s3_prefix": prefix, "files": count, "status": "SUCCESS"}
    except Exception as e:
        print(f"   ❌ customer_recommendations UNLOAD failed: {e}")
        results["customer_recommendations"] = {"status": "FAILED", "error": str(e)}

    return results


if __name__ == "__main__":
    run_all_unloads()