# file: 58_02_consumer_create_database.py
# Purpose: Consumer (Account B) associates datashare and creates local database
# Generates SQL to run on CONSUMER cluster

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/datashare_config.json", "r") as f:
        return json.load(f)


def associate_datashare_consumer():
    """
    AWS API-level: consumer associates (accepts) the datashare
    
    This is the consumer's side of the handshake:
    Producer authorized → Consumer associates → Connection established
    """
    config = load_config()
    redshift_client = boto3.client("redshift", region_name=REGION)

    print("=" * 70)
    print("🔐 CONSUMER — ASSOCIATING DATASHARE")
    print(f"   Consumer Account: {config['consumer_account_id']}")
    print(f"   Producer Account: {config['producer_account_id']}")
    print("=" * 70)

    try:
        # List available datashares for this consumer
        response = redshift_client.describe_data_shares_for_consumer(
            ConsumerArn=f"arn:aws:redshift:{REGION}:{config['consumer_account_id']}:namespace:*"
        )

        if response.get("DataShares"):
            for ds in response["DataShares"]:
                print(f"\n  📦 Available datashare:")
                print(f"     ARN: {ds['DataShareArn']}")
                print(f"     Producer: {ds.get('ProducerArn', 'N/A')}")
                print(f"     Status: {ds.get('Status', 'N/A')}")

                # Associate the datashare
                if config["datashare_name"] in ds["DataShareArn"]:
                    assoc_response = redshift_client.associate_data_share_consumer(
                        DataShareArn=ds["DataShareArn"],
                        ConsumerArn=f"arn:aws:redshift:{REGION}:{config['consumer_account_id']}:namespace:*",
                        AssociateEntireAccount=True
                    )
                    print(f"\n  ✅ Datashare associated!")
                    print(f"     Status: {assoc_response.get('Status', 'N/A')}")
        else:
            print(f"\n  ⚠️  No datashares available")
            print(f"  → Ensure producer has authorized Account B")
            print(f"  → Check: aws redshift describe-data-shares-for-consumer")

    except Exception as e:
        print(f"\n  ⚠️  API result: {e}")
        print(f"\n  📌 MANUAL ALTERNATIVE:")
        print(f"  → Redshift Console (Account B) → Datashares → From other accounts")
        print(f"  → Find: {config['datashare_name']}")
        print(f"  → Click Associate")


def generate_consumer_sql():
    """
    Generate SQL for consumer to create database from datashare
    
    Run this on Account B's Redshift cluster/serverless
    """
    config = load_config()

    # Producer namespace must be obtained from producer cluster
    # SELECT current_namespace; on producer
    producer_namespace = "<PRODUCER-NAMESPACE-UUID>"  # Replace with actual

    print(f"\n{'=' * 70}")
    print(f"📝 CONSUMER SQL — Run on Account B's Redshift")
    print(f"{'=' * 70}")

    sql_commands = f"""
-- ═══════════════════════════════════════════════════════════════
-- CONSUMER SETUP — Account B's Redshift
-- ═══════════════════════════════════════════════════════════════

-- STEP 1: Check available datashares
SHOW DATASHARES;

-- Look for inbound datashares
SELECT share_name, share_type, producer_account, producer_namespace
FROM svv_datashares
WHERE share_type = 'INBOUND';

-- STEP 2: Get producer namespace from the listing above
-- Replace {producer_namespace} with actual UUID from SHOW DATASHARES output


-- ═══════════════════════════════════════════════════════════════
-- STEP 3: Create local database from datashare
-- This makes shared tables queryable in Account B
-- ═══════════════════════════════════════════════════════════════
CREATE DATABASE acct_a_shared
FROM DATASHARE '{config["datashare_name"]}'
OF ACCOUNT '{config["producer_account_id"]}'
NAMESPACE '{producer_namespace}';

-- NOTE: Replace '{producer_namespace}' with actual UUID!
-- Get it from: SHOW DATASHARES; (producer_namespace column)


-- ═══════════════════════════════════════════════════════════════
-- STEP 4: Verify the shared database
-- ═══════════════════════════════════════════════════════════════

-- List databases (should include acct_a_shared)
SELECT datname FROM pg_database;

-- List schemas in shared database
SELECT nspname FROM pg_namespace
WHERE nspname NOT LIKE 'pg_%';

-- List tables in shared schema
SELECT tablename
FROM pg_tables
WHERE schemaname = '{config["shared_schema"]}'
ORDER BY tablename;

-- Alternative: use SVV views
SELECT * FROM svv_all_tables
WHERE database_name = 'acct_a_shared';


-- ═══════════════════════════════════════════════════════════════
-- STEP 5: Query shared tables (the payoff!)
-- ═══════════════════════════════════════════════════════════════

-- Simple query on shared table
SELECT *
FROM acct_a_shared.{config["shared_schema"]}.orders_star
LIMIT 10;

-- Aggregation on shared table
SELECT order_date,
       COUNT(*) AS orders,
       SUM(total_amount) AS revenue,
       AVG(total_amount) AS avg_order
FROM acct_a_shared.{config["shared_schema"]}.daily_rollup
ORDER BY order_date DESC
LIMIT 30;

-- Materialized view (sees latest refresh from producer)
SELECT *
FROM acct_a_shared.{config["shared_schema"]}.mv_hourly_revenue
WHERE hour >= DATEADD(day, -1, GETDATE())
ORDER BY hour DESC;

-- Customer 360 view
SELECT *
FROM acct_a_shared.{config["shared_schema"]}.customer_360
WHERE lifetime_value > 1000
ORDER BY lifetime_value DESC
LIMIT 20;


-- ═══════════════════════════════════════════════════════════════
-- STEP 6: JOIN shared tables with LOCAL tables
-- This is the POWER of data sharing — combine both
-- ═══════════════════════════════════════════════════════════════

-- Create a local forecast table in Account B
CREATE TABLE IF NOT EXISTS public.revenue_forecast (
    forecast_date DATE,
    forecast_revenue DECIMAL(12,2),
    confidence DECIMAL(5,2)
);

INSERT INTO public.revenue_forecast VALUES
('2025-01-15', 125000.00, 0.85),
('2025-01-16', 130000.00, 0.82),
('2025-01-17', 128000.00, 0.88);

-- JOIN: Account A's actuals + Account B's forecasts
SELECT
    a.order_date,
    a.total_revenue AS actual_revenue,
    f.forecast_revenue,
    (a.total_revenue - f.forecast_revenue) AS variance,
    ROUND((a.total_revenue / f.forecast_revenue - 1) * 100, 1) AS variance_pct
FROM acct_a_shared.{config["shared_schema"]}.daily_rollup a
JOIN public.revenue_forecast f
ON a.order_date = f.forecast_date
ORDER BY a.order_date;

-- This query:
-- → Reads actuals from Account A (via data sharing, zero-copy)
-- → Reads forecasts from Account B (local table)
-- → Joins them using Account B's compute
-- → Account A's cluster is NOT involved in this query execution


-- ═══════════════════════════════════════════════════════════════
-- STEP 7: Grant access to Account B's users
-- (datashare database is accessible to superusers by default)
-- ═══════════════════════════════════════════════════════════════
GRANT USAGE ON DATABASE acct_a_shared TO bi_team_role;
GRANT SELECT ON ALL TABLES IN SCHEMA acct_a_shared.{config["shared_schema"]} TO bi_team_role;


-- ═══════════════════════════════════════════════════════════════
-- MONITORING: Track datashare usage
-- ═══════════════════════════════════════════════════════════════
SELECT * FROM svl_datashare_usage_consumer
ORDER BY request_time DESC
LIMIT 20;

-- Verify: consumer queries do NOT appear in producer's STL_QUERY
-- (compute isolation confirmed)
"""

    print(sql_commands)

    with open("/tmp/consumer_datashare.sql", "w") as f:
        f.write(sql_commands)
    print(f"\n💾 SQL saved to /tmp/consumer_datashare.sql")

    return sql_commands


if __name__ == "__main__":
    associate_datashare_consumer()
    generate_consumer_sql()