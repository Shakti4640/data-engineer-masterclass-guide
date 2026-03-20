# file: 58_04_cleanup.py
# Purpose: Generate cleanup SQL for both producer and consumer

import json


def load_config():
    with open("/tmp/datashare_config.json", "r") as f:
        return json.load(f)


def generate_cleanup_sql():
    """Generate cleanup SQL for both producer and consumer"""
    config = load_config()

    print("=" * 70)
    print("🧹 CLEANUP SQL — PROJECT 58")
    print("=" * 70)

    # Consumer cleanup
    consumer_sql = f"""
-- ═══════════════════════════════════════════════════════════════
-- CONSUMER CLEANUP — Run on Account B's Redshift FIRST
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Revoke user access to shared database
REVOKE ALL ON DATABASE acct_a_shared FROM bi_team_role;

-- Step 2: Drop the shared database
-- (This only removes the local reference — producer's data is untouched)
DROP DATABASE IF EXISTS acct_a_shared;

-- Step 3: Drop local test tables
DROP TABLE IF EXISTS public.revenue_forecast;

-- Step 4: Verify cleanup
SELECT datname FROM pg_database;
-- acct_a_shared should no longer appear

-- NOTE: This does NOT affect producer's data or datashare
-- Producer's tables remain intact
"""

    print(f"\n📝 CONSUMER CLEANUP SQL (Account B):")
    print(consumer_sql)

    # Producer cleanup
    producer_sql = f"""
-- ═══════════════════════════════════════════════════════════════
-- PRODUCER CLEANUP — Run on Account A's Redshift AFTER consumer cleanup
-- ═══════════════════════════════════════════════════════════════

-- Step 1: Revoke consumer access
REVOKE USAGE ON DATASHARE {config['datashare_name']}
FROM ACCOUNT '{config['consumer_account_id']}';

-- Step 2: Remove objects from datashare
ALTER DATASHARE {config['datashare_name']}
REMOVE ALL TABLES IN SCHEMA {config['shared_schema']};

ALTER DATASHARE {config['datashare_name']}
REMOVE SCHEMA {config['shared_schema']};

-- Step 3: Drop the datashare
DROP DATASHARE {config['datashare_name']};

-- Step 4: Optionally drop shared schema and tables
-- WARNING: Only do this if you don't need the tables anymore
-- DROP TABLE IF EXISTS {config['shared_schema']}.orders_star;
-- DROP TABLE IF EXISTS {config['shared_schema']}.daily_rollup;
-- DROP MATERIALIZED VIEW IF EXISTS {config['shared_schema']}.mv_hourly_revenue;
-- DROP VIEW IF EXISTS {config['shared_schema']}.customer_360;
-- DROP TABLE IF EXISTS {config['shared_schema']}.schema_changes;
-- DROP SCHEMA IF EXISTS {config['shared_schema']};

-- Step 5: Verify cleanup
SHOW DATASHARES;
-- {config['datashare_name']} should no longer appear

SELECT * FROM svv_datashare_consumers;
-- Should be empty for this share
"""

    print(f"\n📝 PRODUCER CLEANUP SQL (Account A):")
    print(producer_sql)

    # API-level cleanup
    print(f"\n{'=' * 70}")
    print(f"📝 AWS API CLEANUP (run from Account A)")
    print(f"{'=' * 70}")

    api_cleanup = f"""
# Deauthorize consumer at AWS API level
import boto3

redshift_client = boto3.client("redshift", region_name="{config.get('region', 'us-east-2')}")

# List datashares to find ARN
response = redshift_client.describe_data_shares()
for ds in response.get("DataShares", []):
    if "{config['datashare_name']}" in ds["DataShareArn"]:
        # Deauthorize consumer
        redshift_client.deauthorize_data_share_consumer(
            DataShareArn=ds["DataShareArn"],
            ConsumerIdentifier="{config['consumer_account_id']}"
        )
        print(f"Deauthorized consumer: {config['consumer_account_id']}")
"""
    print(api_cleanup)

    # Save all SQL
    with open("/tmp/cleanup_consumer_datashare.sql", "w") as f:
        f.write(consumer_sql)
    with open("/tmp/cleanup_producer_datashare.sql", "w") as f:
        f.write(producer_sql)

    print(f"\n💾 Consumer SQL saved to /tmp/cleanup_consumer_datashare.sql")
    print(f"💾 Producer SQL saved to /tmp/cleanup_producer_datashare.sql")

    # Cleanup local config
    import os
    for f_path in ["/tmp/datashare_config.json",
                    "/tmp/producer_datashare.sql",
                    "/tmp/consumer_datashare.sql",
                    "/tmp/manage_datashare.sql"]:
        try:
            os.remove(f_path)
            print(f"✅ Removed: {f_path}")
        except FileNotFoundError:
            pass

    print(f"\n{'=' * 70}")
    print(f"📋 CLEANUP ORDER:")
    print(f"  1. Run CONSUMER cleanup SQL on Account B's Redshift")
    print(f"  2. Run AWS API deauthorization from Account A")
    print(f"  3. Run PRODUCER cleanup SQL on Account A's Redshift")
    print(f"  4. Verify: SHOW DATASHARES; on both clusters → empty")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    generate_cleanup_sql()