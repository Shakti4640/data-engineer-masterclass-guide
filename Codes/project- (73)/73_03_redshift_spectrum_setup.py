# file: 73_03_redshift_spectrum_setup.py
# Configure Redshift to query S3 Gold layer via Spectrum
# Bridge between warehouse (hot data) and lake (warm/cold data)

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Redshift connection config
# Using Redshift Serverless (Project 67)
WORKGROUP_NAME = "quickcart-analytics"
DATABASE_NAME = "quickcart"

# Spectrum IAM role (must have Glue Catalog + S3 read access)
SPECTRUM_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftSpectrumRole"


def create_spectrum_iam_role(iam_client):
    """
    IAM role for Redshift Spectrum
    
    Spectrum needs:
    → glue:GetTable, glue:GetDatabase, etc. (read catalog)
    → s3:GetObject, s3:ListBucket (read S3 data)
    → Lake Formation will layer on top of these IAM permissions
    
    NOTE: When Lake Formation is enabled, Spectrum ALSO needs
    lakeformation:GetDataAccess to receive vended credentials
    """
    role_name = "RedshiftSpectrumRole"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "redshift.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GlueCatalogRead",
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
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/*",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/*/*"
                ]
            },
            {
                "Sid": "S3GoldRead",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource": [
                    "arn:aws:s3:::orders-data-prod",
                    "arn:aws:s3:::orders-data-prod/gold/*",
                    "arn:aws:s3:::customers-data-prod",
                    "arn:aws:s3:::customers-data-prod/gold/*",
                    "arn:aws:s3:::products-data-prod",
                    "arn:aws:s3:::products-data-prod/gold/*",
                    "arn:aws:s3:::analytics-data-prod",
                    "arn:aws:s3:::analytics-data-prod/derived/*"
                ]
            },
            {
                "Sid": "[REDACTED:AWS_ACCESS_KEY]s",
                "Effect": "Allow",
                "Action": "lakeformation:GetDataAccess",
                "Resource": "*"
                # Lake Formation uses this to vend temporary S3 credentials
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Redshift Spectrum role — reads Glue Catalog + S3 Gold layer"
        )
        print(f"✅ IAM role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM role exists: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="spectrum-data-lake-access",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"   ✅ Permissions attached: Glue + S3 Gold + Lake Formation")

    return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"


def generate_redshift_sql():
    """
    Generate SQL statements to set up Spectrum in Redshift
    
    These must be executed IN Redshift (via psql, Query Editor, or Data API)
    → Cannot be done via boto3 — Redshift SQL is needed
    
    ARCHITECTURE:
    → External schemas point to Glue Catalog databases
    → External tables are automatically discovered from Glue Catalog
    → No need to CREATE EXTERNAL TABLE — Glue Catalog IS the DDL
    → Internal schemas hold materialized/hot data
    """
    spectrum_role = SPECTRUM_ROLE_ARN

    sql_statements = []

    # --- EXTERNAL SCHEMAS (S3 via Spectrum) ---
    external_schemas = [
        ("lake_orders_gold", "orders_gold",
         "S3 Gold orders data via Spectrum — daily_orders, clickstream_events"),
        ("lake_customers_gold", "customers_gold",
         "S3 Gold customer data via Spectrum — customer_segments"),
        ("lake_products_gold", "products_gold",
         "S3 Gold product data via Spectrum — product_catalog"),
        ("lake_analytics", "analytics_derived",
         "S3 derived analytics datasets via Spectrum")
    ]

    for schema_name, glue_db, comment in external_schemas:
        sql = f"""
-- ═══════════════════════════════════════════════════════════
-- External Schema: {schema_name} → Glue Catalog: {glue_db}
-- {comment}
-- ═══════════════════════════════════════════════════════════
DROP SCHEMA IF EXISTS {schema_name};

CREATE EXTERNAL SCHEMA {schema_name}
FROM DATA CATALOG
DATABASE '{glue_db}'
IAM_ROLE '{spectrum_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Verify: list all tables discovered from Glue Catalog
SELECT schemaname, tablename, location
FROM svv_external_tables
WHERE schemaname = '{schema_name}';
"""
        sql_statements.append(sql)

    # --- INTERNAL SCHEMAS (Redshift native — hot data) ---
    internal_sql = f"""
-- ═══════════════════════════════════════════════════════════
-- Internal Schemas: Hot data stored IN Redshift
-- ═══════════════════════════════════════════════════════════

-- Staging: temporary COPY landing zone
CREATE SCHEMA IF NOT EXISTS staging;

-- Mart: materialized aggregations for BI dashboards
CREATE SCHEMA IF NOT EXISTS mart_sales;
CREATE SCHEMA IF NOT EXISTS mart_customers;

-- ═══════════════════════════════════════════════════════════
-- HOT TABLE: Recent orders (last 90 days) — loaded via COPY
-- For sub-second dashboard queries
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mart_sales.recent_orders (
    order_id        VARCHAR(50)     ENCODE zstd,
    customer_id     VARCHAR(50)     ENCODE zstd,
    order_total     DECIMAL(10,2)   ENCODE az64,
    item_count      INTEGER         ENCODE az64,
    status          VARCHAR(20)     ENCODE zstd,
    payment_method  VARCHAR(30)     ENCODE zstd,
    shipping_city   VARCHAR(100)    ENCODE zstd,
    shipping_country VARCHAR(2)     ENCODE zstd,
    order_date      DATE            ENCODE az64,
    shipped_date    DATE            ENCODE az64
)
DISTKEY(order_date)
SORTKEY(order_date, status);

-- ═══════════════════════════════════════════════════════════
-- MATERIALIZED VIEW: Sales dashboard (auto-refresh)
-- Combines HOT Redshift data + WARM S3 data via Spectrum
-- ═══════════════════════════════════════════════════════════
CREATE MATERIALIZED VIEW mart_sales.daily_sales_summary
AUTO REFRESH YES
AS
SELECT
    order_date,
    status,
    COUNT(*) as order_count,
    SUM(order_total) as total_revenue,
    AVG(order_total) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM mart_sales.recent_orders
GROUP BY order_date, status;

-- ═══════════════════════════════════════════════════════════
-- SPECTRUM + INTERNAL JOIN: The power of the Lakehouse
-- Query that spans BOTH Redshift internal AND S3 external
-- ═══════════════════════════════════════════════════════════

-- Example: Recent orders (Redshift) enriched with clickstream (S3)
-- This query is WHY we built the entire platform
SELECT
    ro.order_id,
    ro.order_total,
    ro.status,
    ro.order_date,
    click_summary.total_clicks,
    click_summary.first_click_time,
    click_summary.last_click_time,
    click_summary.devices_used
FROM mart_sales.recent_orders ro
LEFT JOIN (
    -- This subquery runs on Spectrum (S3 Gold layer)
    SELECT
        user_id,
        COUNT(*) as total_clicks,
        MIN(event_ts) as first_click_time,
        MAX(event_ts) as last_click_time,
        COUNT(DISTINCT device_type) as devices_used
    FROM lake_orders_gold.clickstream_events
    WHERE year = '2025' AND month = '01'
    GROUP BY user_id
) click_summary
    ON ro.customer_id = click_summary.user_id
WHERE ro.order_date >= '2025-01-01'
ORDER BY ro.order_total DESC
LIMIT 100;

-- ═══════════════════════════════════════════════════════════
-- FULL PLATFORM QUERY: 3 sources in one SQL statement
-- Internal (Redshift) + S3 Orders Gold + S3 Customers Gold
-- ═══════════════════════════════════════════════════════════
SELECT
    cs.tier as customer_tier,
    COUNT(DISTINCT ro.order_id) as orders,
    SUM(ro.order_total) as revenue,
    AVG(ro.order_total) as avg_order_value,
    SUM(clicks.click_count) as total_clicks,
    ROUND(
        COUNT(DISTINCT ro.order_id)::FLOAT /
        NULLIF(SUM(clicks.click_count), 0) * 100,
        2
    ) as conversion_rate_pct
FROM mart_sales.recent_orders ro                     -- Redshift internal
JOIN lake_customers_gold.customer_segments cs          -- S3 via Spectrum
    ON ro.customer_id = cs.customer_id
LEFT JOIN (
    SELECT user_id, COUNT(*) as click_count
    FROM lake_orders_gold.clickstream_events            -- S3 via Spectrum
    WHERE year = '2025' AND month = '01'
    GROUP BY user_id
) clicks ON ro.customer_id = clicks.user_id
WHERE ro.order_date >= '2025-01-01'
GROUP BY cs.tier
ORDER BY revenue DESC;
"""
    sql_statements.append(internal_sql)

    return sql_statements


def save_sql_scripts(sql_statements):
    """Save generated SQL to files for execution in Redshift"""
    import os

    output_dir = "/tmp/redshift_setup"
    os.makedirs(output_dir, exist_ok=True)

    # Combined file
    combined_path = os.path.join(output_dir, "73_redshift_full_setup.sql")
    with open(combined_path, "w") as f:
        for i, sql in enumerate(sql_statements):
            f.write(f"-- ========== SECTION {i+1} ==========\n")
            f.write(sql)
            f.write("\n\n")

    print(f"\n💾 SQL scripts saved to: {output_dir}")
    print(f"   Combined: {combined_path}")
    print(f"\n   Execute in Redshift Query Editor or via:")
    print(f"   psql -h workgroup.region.redshift-serverless.amazonaws.com \\")
    print(f"        -U admin -d quickcart -f {combined_path}")


def execute_via_data_api(sql_statements):
    """
    Execute SQL via Redshift Data API (no direct connection needed)
    
    Data API:
    → Async SQL execution
    → No VPC/network config needed
    → IAM-authenticated
    → Great for automation
    """
    rs_data = boto3.client("redshift-data", region_name=REGION)

    print("\n🔄 Executing SQL via Redshift Data API:")

    for i, sql in enumerate(sql_statements):
        # Split multi-statement SQL into individual statements
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]

        for j, stmt in enumerate(statements):
            if len(stmt) < 10:  # Skip empty/trivial statements
                continue

            # Truncate for display
            display = stmt[:80].replace("\n", " ")
            print(f"\n   [{i+1}.{j+1}] {display}...")

            try:
                response = rs_data.execute_statement(
                    WorkgroupName=WORKGROUP_NAME,
                    Database=DATABASE_NAME,
                    Sql=stmt + ";"
                )
                stmt_id = response["Id"]

                # Wait for completion
                while True:
                    status_response = rs_data.describe_statement(Id=stmt_id)
                    status = status_response["Status"]
                    if status in ("FINISHED", "FAILED", "ABORTED"):
                        break
                    time.sleep(1)

                if status == "FINISHED":
                    has_results = status_response.get("HasResultSet", False)
                    if has_results:
                        results = rs_data.get_statement_result(Id=stmt_id)
                        rows = results.get("Records", [])
                        print(f"       ✅ Success ({len(rows)} rows)")
                    else:
                        print(f"       ✅ Success")
                else:
                    error = status_response.get("Error", "Unknown error")
                    print(f"       ❌ {status}: {error}")

            except Exception as e:
                print(f"       ⚠️  {e}")


def main():
    print("=" * 70)
    print("🌉 REDSHIFT SPECTRUM BRIDGE: Warehouse ↔ Data Lake")
    print("=" * 70)

    iam_client = boto3.client("iam", region_name=REGION)

    # Step 1: Spectrum IAM Role
    print("\n--- Step 1: Spectrum IAM Role ---")
    create_spectrum_iam_role(iam_client)

    # Step 2: Generate SQL
    print("\n--- Step 2: Generate Redshift SQL ---")
    sql_statements = generate_redshift_sql()
    save_sql_scripts(sql_statements)

    # Step 3: Execute (optional — requires Redshift Serverless running)
    print("\n--- Step 3: Execute via Data API ---")
    try:
        execute_via_data_api(sql_statements)
    except Exception as e:
        print(f"   ⚠️  Data API execution skipped: {e}")
        print(f"   → Execute SQL manually from saved scripts")

    print("\n" + "=" * 70)
    print("✅ SPECTRUM BRIDGE COMPLETE")
    print("   Redshift can now query S3 Gold data seamlessly")
    print("=" * 70)


if __name__ == "__main__":
    main()