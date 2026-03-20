# file: 49_05_add_new_entity.py
# Purpose: Demonstrate how easy it is to add a new entity to the ETL pipeline
# BEFORE (Project 42): copy-paste 20 SQL statements + modify each one
# AFTER (this project): INSERT one row into entity_config

import psycopg2

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def add_new_entity(entity_name, pk_column, s3_prefix,
                   target_table=None, target_schema="public",
                   load_order=100):
    """
    Add a new entity to the ETL pipeline.
    
    BEFORE (Project 42):
    → Copy 20 SQL statements from existing entity
    → Modify table names, column names, S3 paths in each
    → Test entire Glue job to verify
    → Risk: typo in one statement breaks everything
    → Time: ~2 hours
    
    AFTER (this project):
    → INSERT one row into etl.entity_config
    → Create the target table DDL
    → Next pipeline run automatically includes it
    → Time: ~10 minutes
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    target_table = target_table or entity_name
    iam_role = "arn:aws:iam::123456789012:role/quickcart-redshift-s3-role"

    print(f"{'='*60}")
    print(f"➕ ADDING NEW ENTITY: {entity_name}")
    print(f"{'='*60}")

    # Step 1: Verify target table exists
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (target_schema, target_table))

    table_exists = cursor.fetchone()[0] > 0

    if not table_exists:
        print(f"   ❌ Target table {target_schema}.{target_table} does not exist")
        print(f"   → Create it first, then re-run this script")
        print(f"   → Example:")
        print(f"      CREATE TABLE {target_schema}.{target_table} (")
        print(f"          {pk_column} VARCHAR(50) PRIMARY KEY,")
        print(f"          ... your columns here ...")
        print(f"      ) DISTKEY({pk_column}) SORTKEY({pk_column});")
        cursor.close()
        conn.close()
        return False

    print(f"   ✅ Target table exists: {target_schema}.{target_table}")

    # Step 2: Add to entity_config
    cursor.execute("""
        INSERT INTO etl.entity_config 
        (entity_name, target_schema, target_table, pk_column, s3_prefix, iam_role, is_active, load_order)
        VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
        ON CONFLICT (entity_name) DO UPDATE SET
            target_schema = EXCLUDED.target_schema,
            target_table = EXCLUDED.target_table,
            pk_column = EXCLUDED.pk_column,
            s3_prefix = EXCLUDED.s3_prefix,
            iam_role = EXCLUDED.iam_role,
            is_active = TRUE,
            load_order = EXCLUDED.load_order,
            updated_at = GETDATE()
    """, (entity_name, target_schema, target_table, pk_column,
          s3_prefix, iam_role, load_order))

    print(f"   ✅ Entity config added: {entity_name}")
    print(f"      PK: {pk_column}")
    print(f"      S3: {s3_prefix}")
    print(f"      Load order: {load_order}")

    # Step 3: Verify
    cursor.execute("""
        SELECT entity_name, target_table, pk_column, s3_prefix, load_order
        FROM etl.entity_config
        WHERE is_active = TRUE
        ORDER BY load_order
    """)

    print(f"\n   📋 All active entities:")
    for row in cursor.fetchall():
        print(f"      {row[4]}. {row[0]} → {row[1]} (PK: {row[2]})")

    print(f"\n   ✅ Next CALL etl.load_daily_data() will automatically include '{entity_name}'")
    print(f"   → No code changes needed!")
    print(f"   → No Glue job modifications!")
    print(f"   → No Step Functions updates!")

    cursor.close()
    conn.close()
    return True


def deactivate_entity(entity_name):
    """
    Temporarily disable an entity from the ETL pipeline.
    Data remains in target table, but no new loads occur.
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE etl.entity_config
        SET is_active = FALSE, updated_at = GETDATE()
        WHERE entity_name = %s
    """, (entity_name,))

    print(f"✅ Entity deactivated: {entity_name}")
    print(f"   → Will be skipped in next pipeline run")
    print(f"   → To reactivate: UPDATE etl.entity_config SET is_active = TRUE WHERE entity_name = '{entity_name}'")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 49_05_add_new_entity.py add order_returns order_return_id s3://bucket/silver/order_returns/")
        print("  python 49_05_add_new_entity.py deactivate order_returns")
        sys.exit(0)

    action = sys.argv[1]

    if action == "add":
        entity = sys.argv[2]
        pk = sys.argv[3]
        s3 = sys.argv[4]
        add_new_entity(entity, pk, s3)

    elif action == "deactivate":
        entity = sys.argv[2]
        deactivate_entity(entity)