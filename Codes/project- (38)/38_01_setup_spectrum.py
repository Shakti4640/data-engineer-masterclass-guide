# file: 38_01_setup_spectrum.py
# Purpose: Configure Redshift Spectrum — IAM, external schemas
# Prerequisites: Redshift cluster exists, Glue Catalog has tables

import boto3
import json
import time

AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

S3_BUCKET = "quickcart-datalake-prod"
REDSHIFT_CLUSTER = "quickcart-dwh"
SPECTRUM_ROLE_NAME = "RedshiftSpectrumRole"

iam = boto3.client("iam")
redshift = boto3.client("redshift", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create IAM Role for Spectrum
# ═══════════════════════════════════════════════════════════════════
def create_spectrum_role():
    """
    Spectrum needs an IAM Role with:
    1. S3 read access (to scan Parquet files)
    2. Glue Catalog read access (to read table metadata)
    3. Trust policy allowing Redshift to assume the role
    
    CAN WE REUSE RedshiftS3ReadRole from Project 32?
    → Yes, but we need to ADD Glue Catalog permissions
    → Spectrum needs: glue:GetDatabase, glue:GetTable, glue:GetPartitions
    → Project 32 role only has S3 permissions
    → Best practice: create dedicated Spectrum role OR extend existing
    """
    print("\n🔐 Creating Spectrum IAM Role...")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "redshift.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    spectrum_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "S3ReadAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:GetBucketAcl"
                ],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET}",
                    f"arn:aws:s3:::{S3_BUCKET}/*"
                ]
            },
            {
                "Sid": "GlueCatalogReadAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition"
                ],
                "Resource": [
                    f"arn:aws:iam::{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{AWS_REGION}:{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{AWS_REGION}:{ACCOUNT_ID}:database/*",
                    f"arn:aws:glue:{AWS_REGION}:{ACCOUNT_ID}:table/*"
                ]
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=SPECTRUM_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Redshift Spectrum to read S3 and Glue Catalog"
        )
        print(f"   ✅ Role created: {SPECTRUM_ROLE_NAME}")
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"   ℹ️  Role already exists: {SPECTRUM_ROLE_NAME}")

    iam.put_role_policy(
        RoleName=SPECTRUM_ROLE_NAME,
        PolicyName="SpectrumS3GluePolicy",
        PolicyDocument=json.dumps(spectrum_policy)
    )
    print(f"   ✅ Policy attached")

    time.sleep(10)  # IAM propagation

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{SPECTRUM_ROLE_NAME}"
    return role_arn


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Attach Role to Redshift Cluster
# ═══════════════════════════════════════════════════════════════════
def attach_role_to_cluster(role_arn):
    """Attach Spectrum IAM Role to Redshift cluster"""
    print(f"\n🔗 Attaching role to Redshift cluster...")

    try:
        redshift.modify_cluster_iam_roles(
            ClusterIdentifier=REDSHIFT_CLUSTER,
            AddIamRoles=[role_arn]
        )
        print(f"   ✅ Role attached to {REDSHIFT_CLUSTER}")
        print(f"   ⏳ Takes 1-2 minutes to propagate")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Role already attached")
        else:
            print(f"   ⚠️  {e}")

    return role_arn


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Generate Redshift SQL for External Schemas
# ═══════════════════════════════════════════════════════════════════
def generate_spectrum_sql(role_arn):
    """
    Generate SQL to create external schemas in Redshift
    
    MUST run these ON Redshift (via Query Editor or psql)
    Cannot be executed via boto3 — Redshift DDL requires JDBC/ODBC
    """
    print(f"\n📋 SQL TO RUN ON REDSHIFT:")
    print("=" * 70)
    print(f"""
-- ═══════════════════════════════════════════════════════════════
-- STEP 1: Create External Schemas (link to Glue Catalog)
-- ═══════════════════════════════════════════════════════════════

-- External schema for GOLD layer (tiny tables — virtually free)
CREATE EXTERNAL SCHEMA IF NOT EXISTS ext_gold
FROM DATA CATALOG
DATABASE 'gold_quickcart'
IAM_ROLE '{role_arn}'
REGION '{AWS_REGION}';

-- External schema for SILVER layer (larger tables — use with caution)
CREATE EXTERNAL SCHEMA IF NOT EXISTS ext_silver
FROM DATA CATALOG
DATABASE 'silver_quickcart'
IAM_ROLE '{role_arn}'
REGION '{AWS_REGION}';

-- External schema for BRONZE layer (raw data — archive access)
CREATE EXTERNAL SCHEMA IF NOT EXISTS ext_bronze
FROM DATA CATALOG
DATABASE 'bronze_mariadb'
IAM_ROLE '{role_arn}'
REGION '{AWS_REGION}';


-- ═══════════════════════════════════════════════════════════════
-- STEP 2: Verify External Tables Are Visible
-- ═══════════════════════════════════════════════════════════════

-- List all external schemas
SELECT schemaname, databasename, esoptions
FROM svv_external_schemas;

-- List all external tables
SELECT schemaname, tablename, location
FROM svv_external_tables
ORDER BY schemaname, tablename;

-- List external table columns
SELECT schemaname, tablename, columnname, external_type
FROM svv_external_columns
WHERE schemaname = 'ext_gold'
ORDER BY tablename, columnnum;


-- ═══════════════════════════════════════════════════════════════
-- STEP 3: Test Queries (Gold — virtually free)
-- ═══════════════════════════════════════════════════════════════

-- Test gold revenue summary (~3 KB scan)
SELECT * FROM ext_gold.revenue_by_category_month
WHERE year = '2025'
ORDER BY total_revenue DESC
LIMIT 10;

-- Test gold customer segments (~1 KB scan)
SELECT * FROM ext_gold.customer_segments_summary
ORDER BY total_segment_revenue DESC;

-- Test gold product performance (~50 KB scan)
SELECT product_name, product_category, total_revenue
FROM ext_gold.product_performance
ORDER BY total_revenue DESC
LIMIT 10;


-- ═══════════════════════════════════════════════════════════════
-- STEP 4: Mixed Queries (Local + Spectrum)
-- ═══════════════════════════════════════════════════════════════

-- Join LOCAL orders with EXTERNAL customer RFM
SELECT 
    o.order_id,
    o.customer_id,
    o.total_amount,
    o.status,
    o.order_date,
    r.rfm_segment,
    r.monetary AS customer_lifetime_value,
    r.frequency AS customer_order_count,
    r.recency_days AS days_since_last_order
FROM analytics.fact_orders o
LEFT JOIN ext_silver.customer_rfm r
    ON o.customer_id = r.customer_id
WHERE o.order_date >= CURRENT_DATE - 7
ORDER BY o.total_amount DESC
LIMIT 20;

-- Join LOCAL orders with EXTERNAL product performance
SELECT
    o.order_id,
    o.order_date,
    o.total_amount,
    p.product_name,
    p.product_category,
    p.total_revenue AS product_total_revenue,
    p.unique_buyers AS product_unique_buyers
FROM analytics.fact_orders o
LEFT JOIN ext_gold.product_performance p
    ON o.product_id = p.product_id
WHERE o.order_date >= CURRENT_DATE - 7
ORDER BY p.total_revenue DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════════════
-- STEP 5: Create Local Views for Tableau
-- (Tableau queries views — doesn't know about Spectrum)
-- ═══════════════════════════════════════════════════════════════

-- View: Orders enriched with customer RFM segment
CREATE OR REPLACE VIEW analytics.v_orders_with_rfm AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.status,
    o.order_date,
    r.rfm_segment,
    r.monetary AS customer_ltv,
    r.frequency AS customer_orders,
    r.recency_days
FROM analytics.fact_orders o
LEFT JOIN ext_silver.customer_rfm r
    ON o.customer_id = r.customer_id;

-- View: Revenue summary (backed by gold — virtually free)
CREATE OR REPLACE VIEW analytics.v_revenue_summary AS
SELECT *
FROM ext_gold.revenue_by_category_month;

-- View: Customer segments (backed by gold)
CREATE OR REPLACE VIEW analytics.v_customer_segments AS
SELECT *
FROM ext_gold.customer_segments_summary;

-- View: Product leaderboard (backed by gold)
CREATE OR REPLACE VIEW analytics.v_product_leaderboard AS
SELECT *
FROM ext_gold.product_performance;

-- View: Historical + Current orders (UNION pattern)
CREATE OR REPLACE VIEW analytics.v_all_orders_history AS
-- Hot data: local Redshift (last 90 days, fast)
SELECT order_id, customer_id, product_id, quantity,
       unit_price, total_amount, status, order_date,
       'local' AS data_source
FROM analytics.fact_orders
WHERE order_date >= CURRENT_DATE - 90

UNION ALL

-- Cold data: S3 via Spectrum (older than 90 days, slower)
SELECT order_id, customer_id, product_id, quantity,
       unit_price, total_amount, status, 
       CAST(order_date AS DATE) AS order_date,
       'spectrum' AS data_source
FROM ext_silver.clean_orders
WHERE CAST(year AS INT) < EXTRACT(YEAR FROM CURRENT_DATE)
   OR (CAST(year AS INT) = EXTRACT(YEAR FROM CURRENT_DATE)
       AND CAST(month AS INT) < EXTRACT(MONTH FROM CURRENT_DATE) - 3);


-- ═══════════════════════════════════════════════════════════════
-- STEP 6: Grant Permissions
-- ═══════════════════════════════════════════════════════════════

-- Grant Spectrum access to analyst groups
GRANT USAGE ON SCHEMA ext_gold TO GROUP analyst_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ext_gold TO GROUP analyst_group;

-- Grant view access
GRANT SELECT ON analytics.v_orders_with_rfm TO GROUP analyst_group;
GRANT SELECT ON analytics.v_revenue_summary TO GROUP analyst_group;
GRANT SELECT ON analytics.v_customer_segments TO GROUP analyst_group;
GRANT SELECT ON analytics.v_product_leaderboard TO GROUP analyst_group;
GRANT SELECT ON analytics.v_all_orders_history TO GROUP analyst_group;

-- Restrict silver access (expensive scans) to senior analysts only
GRANT USAGE ON SCHEMA ext_silver TO GROUP senior_analyst_group;
GRANT SELECT ON ALL TABLES IN SCHEMA ext_silver TO GROUP senior_analyst_group;


-- ═══════════════════════════════════════════════════════════════
-- STEP 7: Monitor Spectrum Usage
-- ═══════════════════════════════════════════════════════════════

-- Check Spectrum scan statistics
SELECT query, segment,
       s3_scanned_rows, s3_scanned_bytes,
       s3query_returned_rows, s3query_returned_bytes,
       files, avg_request_parallelism
FROM svl_s3query_summary
ORDER BY query DESC
LIMIT 20;

-- Check Spectrum costs (bytes scanned)
SELECT DATE_TRUNC('day', starttime) AS day,
       SUM(s3_scanned_bytes) / (1024.0*1024*1024) AS gb_scanned,
       SUM(s3_scanned_bytes) / (1024.0*1024*1024*1024) * 5 AS estimated_cost
FROM svl_s3query_summary
GROUP BY DATE_TRUNC('day', starttime)
ORDER BY day DESC
LIMIT 30;
""")
    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 SETTING UP REDSHIFT SPECTRUM")
    print("=" * 60)

    role_arn = create_spectrum_role()
    attach_role_to_cluster(role_arn)
    generate_spectrum_sql(role_arn)

    print("\n" + "=" * 60)
    print("✅ SPECTRUM INFRASTRUCTURE READY")
    print("   Next: Run the SQL above on Redshift Query Editor")
    print("=" * 60)