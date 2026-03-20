# file: 58_01_producer_create_datashare.py
# Purpose: Create datashare on Account A's Redshift cluster
# Generates SQL to run on PRODUCER cluster

import boto3
import json

REGION = "us-east-2"
ACCOUNT_A_ID = [REDACTED:BANK_ACCOUNT_NUMBER]1"
ACCOUNT_B_ID = [REDACTED:BANK_ACCOUNT_NUMBER]2"

DATASHARE_NAME = "quickcart_analytics_share"
SHARED_SCHEMA = "analytics"
PRODUCER_CLUSTER = "quickcart-prod"


def generate_producer_sql():
    """
    Generate SQL commands to run on PRODUCER Redshift cluster
    
    These commands:
    1. Create the datashare
    2. Add schema and tables to the datashare
    3. Grant usage to consumer namespace
    
    MUST be run by a superuser or datashare owner on the producer cluster
    """
    print("=" * 70)
    print("📝 PRODUCER SQL — Run on Account A's Redshift Cluster")
    print(f"   Cluster: {PRODUCER_CLUSTER}")
    print(f"   Datashare: {DATASHARE_NAME}")
    print("=" * 70)

    sql_commands = f"""
-- ═══════════════════════════════════════════════════════════════
-- STEP 1: Create the datashare
-- ═══════════════════════════════════════════════════════════════
CREATE DATASHARE {DATASHARE_NAME}
SET PUBLICACCESSIBLE = FALSE;
-- PUBLICACCESSIBLE = FALSE: only authorized consumers can access
-- Set TRUE only if consumer is a publicly accessible cluster

-- Verify creation
SHOW DATASHARES LIKE '{DATASHARE_NAME}';


-- ═══════════════════════════════════════════════════════════════
-- STEP 2: Create dedicated schema for shared objects
-- (if not already exists)
-- ═══════════════════════════════════════════════════════════════
CREATE SCHEMA IF NOT EXISTS {SHARED_SCHEMA};

-- Move/create tables you want to share into this schema
-- Example: create a star schema table for BI
CREATE TABLE IF NOT EXISTS {SHARED_SCHEMA}.orders_star AS
SELECT
    o.order_id,
    o.order_date,
    o.total_amount,
    o.status,
    c.customer_id,
    c.name AS customer_name,
    c.city AS customer_city,
    c.state AS customer_state,
    c.tier AS customer_tier,
    p.product_id,
    p.name AS product_name,
    p.category AS product_category,
    p.price AS product_price
FROM public.orders o
LEFT JOIN public.customers c ON o.customer_id = c.customer_id
LEFT JOIN public.products p ON o.product_id = p.product_id;

-- Create daily rollup
CREATE TABLE IF NOT EXISTS {SHARED_SCHEMA}.daily_rollup AS
SELECT
    order_date,
    COUNT(*) AS order_count,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value
FROM public.orders
GROUP BY order_date;

-- Create materialized view (auto-refreshed)
CREATE MATERIALIZED VIEW IF NOT EXISTS {SHARED_SCHEMA}.mv_hourly_revenue AS
SELECT
    DATE_TRUNC('hour', order_date) AS hour,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue
FROM public.orders
GROUP BY 1;

-- Create customer 360 view
CREATE OR REPLACE VIEW {SHARED_SCHEMA}.customer_360 AS
SELECT
    c.customer_id,
    c.name,
    c.city,
    c.state,
    c.tier,
    COUNT(o.order_id) AS total_orders,
    SUM(o.total_amount) AS lifetime_value,
    MAX(o.order_date) AS last_order_date,
    MIN(o.order_date) AS first_order_date
FROM public.customers c
LEFT JOIN public.orders o ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3, 4, 5;


-- ═══════════════════════════════════════════════════════════════
-- STEP 3: Add schema and all its tables to the datashare
-- ═══════════════════════════════════════════════════════════════
ALTER DATASHARE {DATASHARE_NAME} ADD SCHEMA {SHARED_SCHEMA};
ALTER DATASHARE {DATASHARE_NAME} ADD ALL TABLES IN SCHEMA {SHARED_SCHEMA};

-- If you want to add specific tables instead of ALL:
-- ALTER DATASHARE {DATASHARE_NAME} ADD TABLE {SHARED_SCHEMA}.orders_star;
-- ALTER DATASHARE {DATASHARE_NAME} ADD TABLE {SHARED_SCHEMA}.daily_rollup;

-- Verify objects in datashare
SELECT * FROM svv_datashare_objects
WHERE share_name = '{DATASHARE_NAME}';


-- ═══════════════════════════════════════════════════════════════
-- STEP 4: Grant usage to consumer account
-- For cross-account: grant to ACCOUNT (not namespace)
-- ═══════════════════════════════════════════════════════════════
GRANT USAGE ON DATASHARE {DATASHARE_NAME} TO ACCOUNT '{ACCOUNT_B_ID}';

-- Verify grants
SELECT * FROM svv_datashare_consumers
WHERE share_name = '{DATASHARE_NAME}';


-- ═══════════════════════════════════════════════════════════════
-- STEP 5: Get producer namespace (consumer needs this)
-- ═══════════════════════════════════════════════════════════════
SELECT current_namespace;
-- Save this UUID — consumer needs it to create database


-- ═══════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ═══════════════════════════════════════════════════════════════

-- List all datashares on this cluster
SELECT share_name, share_type, is_publicaccessible,
       share_acl, producer_account, producer_namespace
FROM svv_datashares;

-- List all objects in the datashare
SELECT share_name, object_type, object_name
FROM svv_datashare_objects
WHERE share_name = '{DATASHARE_NAME}'
ORDER BY object_type, object_name;

-- List all consumers
SELECT share_name, consumer_account, consumer_namespace, share_date
FROM svv_datashare_consumers
WHERE share_name = '{DATASHARE_NAME}';
"""

    print(sql_commands)

    # Save SQL to file
    with open("/tmp/producer_datashare.sql", "w") as f:
        f.write(sql_commands)
    print(f"\n💾 SQL saved to /tmp/producer_datashare.sql")

    return sql_commands


def authorize_datashare_cross_account():
    """
    AWS API-level authorization for cross-account data sharing
    
    This is the SECOND level of authorization:
    Level 1: Redshift SQL GRANT (done in SQL above)
    Level 2: AWS API authorization (done here)
    
    Must be called from PRODUCER's account
    """
    redshift_client = boto3.client("redshift", region_name=REGION)

    print(f"\n{'=' * 70}")
    print(f"🔐 AWS API — CROSS-ACCOUNT DATASHARE AUTHORIZATION")
    print(f"{'=' * 70}")

    # Get datashare ARN
    # Format: arn:aws:redshift:region:account:datashare:namespace/share-name
    # In practice, get this from describe_data_shares

    try:
        response = redshift_client.describe_data_shares(
            DataShareArn=f"arn:aws:redshift:{REGION}:{ACCOUNT_A_ID}:datashare:*/{DATASHARE_NAME}"
        )

        if response.get("DataShares"):
            datashare = response["DataShares"][0]
            datashare_arn = datashare["DataShareArn"]
            print(f"  Datashare ARN: {datashare_arn}")
            print(f"  Status: {datashare.get('Status', 'N/A')}")
            print(f"  Producer: {datashare.get('ProducerArn', 'N/A')}")

            # Authorize consumer account
            auth_response = redshift_client.authorize_data_share_consumer(
                DataShareArn=datashare_arn,
                ConsumerIdentifier=ACCOUNT_B_ID
            )
            print(f"\n  ✅ Account B ({ACCOUNT_B_ID}) authorized as consumer")
            print(f"  Authorization status: {auth_response.get('Status', 'N/A')}")

        else:
            print(f"  ⚠️  No datashare found with name: {DATASHARE_NAME}")
            print(f"  → Run the SQL commands first to create the datashare")
            print(f"  → Then re-run this script for API authorization")

    except Exception as e:
        print(f"  ⚠️  API call result: {e}")
        print(f"\n  📌 MANUAL ALTERNATIVE:")
        print(f"  → Redshift Console → Datashares → {DATASHARE_NAME}")
        print(f"  → Actions → Authorize → Enter Account B ID: {ACCOUNT_B_ID}")
        print(f"  → Click Authorize")

    # Save config for consumer
    config = {
        "datashare_name": DATASHARE_NAME,
        "producer_account_id": ACCOUNT_A_ID,
        "consumer_account_id": ACCOUNT_B_ID,
        "producer_cluster": PRODUCER_CLUSTER,
        "shared_schema": SHARED_SCHEMA,
        "shared_tables": [
            "orders_star", "daily_rollup", "mv_hourly_revenue",
            "customer_360"
        ]
    }
    with open("/tmp/datashare_config.json", "w") as f:
        json.dump(config, f, indent=2)
    print(f"\n💾 Config saved to /tmp/datashare_config.json")

    return config


if __name__ == "__main__":
    generate_producer_sql()
    authorize_datashare_cross_account()