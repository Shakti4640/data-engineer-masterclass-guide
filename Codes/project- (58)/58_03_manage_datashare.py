# file: 58_03_manage_datashare.py
# Purpose: Ongoing management of datashares — monitor, modify, revoke

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/datashare_config.json", "r") as f:
        return json.load(f)


def generate_management_sql():
    """
    SQL commands for ongoing datashare management
    Run on PRODUCER cluster
    """
    config = load_config()
    ds_name = config["datashare_name"]
    schema = config["shared_schema"]
    account_b = config["consumer_account_id"]

    print("=" * 70)
    print("📝 DATASHARE MANAGEMENT SQL")
    print("=" * 70)

    sql = f"""
-- ═══════════════════════════════════════════════════════════════
-- MONITORING (run on PRODUCER)
-- ═══════════════════════════════════════════════════════════════

-- 1. List all datashares and their status
SELECT share_name, share_type, is_publicaccessible,
       producer_account, producer_namespace
FROM svv_datashares
ORDER BY share_name;

-- 2. List all objects in each datashare
SELECT share_name, object_type, object_name
FROM svv_datashare_objects
WHERE share_name = '{ds_name}'
ORDER BY object_type, object_name;

-- 3. List all consumers
SELECT share_name, consumer_account, consumer_namespace,
       share_date
FROM svv_datashare_consumers
WHERE share_name = '{ds_name}';

-- 4. Track producer-side usage (who's querying shared data)
SELECT share_name, consumer_account, consumer_namespace,
       object_name, request_type, request_time
FROM svl_datashare_usage_producer
WHERE share_name = '{ds_name}'
ORDER BY request_time DESC
LIMIT 50;


-- ═══════════════════════════════════════════════════════════════
-- ADDING NEW OBJECTS TO DATASHARE
-- (new table automatically available to all consumers)
-- ═══════════════════════════════════════════════════════════════

-- Add a single new table
-- ALTER DATASHARE {ds_name} ADD TABLE {schema}.new_table_name;

-- Add a new view
-- ALTER DATASHARE {ds_name} ADD TABLE {schema}.new_view_name;

-- Add all NEW tables in schema (catches up)
-- ALTER DATASHARE {ds_name} ADD ALL TABLES IN SCHEMA {schema};

-- If you add a new table to the schema, it's NOT auto-shared
-- You must explicitly ADD it to the datashare
-- UNLESS you shared the entire schema (then new tables auto-included)


-- ═══════════════════════════════════════════════════════════════
-- REMOVING OBJECTS FROM DATASHARE
-- (immediately affects consumers — their queries will break!)
-- ═══════════════════════════════════════════════════════════════

-- Remove specific table from share (consumers lose access immediately)
-- ALTER DATASHARE {ds_name} REMOVE TABLE {schema}.table_to_remove;

-- WARNING: no grace period — consumer's next query fails
-- ALWAYS notify consumers BEFORE removing objects


-- ═══════════════════════════════════════════════════════════════
-- REVOKING CONSUMER ACCESS
-- (nuclear option — cuts all access for that consumer)
-- ═══════════════════════════════════════════════════════════════

-- Revoke access from Account B
-- REVOKE USAGE ON DATASHARE {ds_name} FROM ACCOUNT '{account_b}';

-- After revoking:
-- → Consumer's database still exists but all queries fail
-- → Consumer should DROP DATABASE acct_a_shared;
-- → Consumer must re-associate if access is restored


-- ═══════════════════════════════════════════════════════════════
-- DROPPING DATASHARE ENTIRELY
-- (removes the share and all consumer access)
-- ═══════════════════════════════════════════════════════════════

-- DROP DATASHARE {ds_name};
-- WARNING: all consumers lose access immediately
-- Cannot be undone — must recreate from scratch


-- ═══════════════════════════════════════════════════════════════
-- BEST PRACTICE: Schema change notification
-- Before modifying shared tables, notify consumers:
-- ═══════════════════════════════════════════════════════════════

-- 1. Create a "schema_changes" table in the shared schema
CREATE TABLE IF NOT EXISTS {schema}.schema_changes (
    change_id INT IDENTITY(1,1),
    change_date TIMESTAMP DEFAULT GETDATE(),
    change_type VARCHAR(50),    -- 'ADD_TABLE', 'DROP_COLUMN', 'RENAME_TABLE'
    object_name VARCHAR(256),
    description VARCHAR(1000),
    breaking_change BOOLEAN,
    effective_date TIMESTAMP
);

-- 2. Log changes before making them
INSERT INTO {schema}.schema_changes
(change_type, object_name, description, breaking_change, effective_date)
VALUES
('ADD_COLUMN', '{schema}.orders_star',
 'Adding column: discount_amount DECIMAL(10,2)',
 FALSE, DATEADD(day, 7, GETDATE()));

-- 3. Consumers can query this table to see upcoming changes
-- SELECT * FROM acct_a_shared.{schema}.schema_changes
-- WHERE effective_date > GETDATE()
-- ORDER BY effective_date;
"""

    print(sql)

    with open("/tmp/manage_datashare.sql", "w") as f:
        f.write(sql)
    print(f"\n💾 SQL saved to /tmp/manage_datashare.sql")


def describe_datashares_via_api():
    """Use Redshift API to check datashare status"""
    config = load_config()
    redshift_client = boto3.client("redshift", region_name=REGION)

    print(f"\n{'=' * 70}")
    print(f"📊 DATASHARE STATUS VIA AWS API")
    print(f"{'=' * 70}")

    try:
        # Describe all datashares
        response = redshift_client.describe_data_shares()

        for ds in response.get("DataShares", []):
            print(f"\n  📦 Datashare: {ds['DataShareArn']}")
            print(f"     Producer: {ds.get('ProducerArn', 'N/A')}")
            print(f"     Status: {ds.get('Status', 'N/A')}")
            print(f"     Managed By: {ds.get('ManagedBy', 'N/A')}")

            # List associations
            for assoc in ds.get("DataShareAssociations", []):
                print(f"     Consumer: {assoc.get('ConsumerIdentifier', 'N/A')}")
                print(f"     Status: {assoc.get('Status', 'N/A')}")
                print(f"     Created: {assoc.get('CreatedDate', 'N/A')}")

        if not response.get("DataShares"):
            print(f"  ℹ️  No datashares found")
            print(f"  → Create datashare SQL first, then re-check")

    except Exception as e:
        print(f"  ⚠️  API error: {e}")
        print(f"  → Ensure Redshift cluster exists and is running")


if __name__ == "__main__":
    generate_management_sql()
    describe_datashares_via_api()