# file: 49_01_create_etl_schema.py
# Purpose: Create the ETL schema, audit log table, and base infrastructure in Redshift

import psycopg2
import time

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def create_etl_infrastructure():
    """Create schema, audit tables, and helper objects"""
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    print("=" * 60)
    print("🔧 CREATING REDSHIFT ETL INFRASTRUCTURE")
    print("=" * 60)

    # ━━━ CREATE ETL SCHEMA ━━━
    cursor.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    print("✅ Schema created: etl")

    # ━━━ CREATE RUN LOG TABLE ━━━
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl.run_log (
            run_id          BIGINT IDENTITY(1,1) PRIMARY KEY,
            run_group_id    VARCHAR(50)     NOT NULL
                            COMMENT 'Groups related entity loads in one pipeline run',
            procedure_name  VARCHAR(100)    NOT NULL,
            entity_name     VARCHAR(50),
            process_date    DATE,
            status          VARCHAR(20)     NOT NULL
                            COMMENT 'STARTED, SUCCESS, FAILED, SKIPPED',
            rows_staged     BIGINT          DEFAULT 0,
            rows_deduped    BIGINT          DEFAULT 0,
            rows_merged     BIGINT          DEFAULT 0,
            rows_inserted   BIGINT          DEFAULT 0,
            rows_updated    BIGINT          DEFAULT 0,
            error_message   VARCHAR(4000),
            error_state     VARCHAR(10),
            started_at      TIMESTAMP       DEFAULT GETDATE(),
            completed_at    TIMESTAMP,
            duration_seconds DECIMAL(10,2),
            s3_source_path  VARCHAR(500),
            created_at      TIMESTAMP       DEFAULT GETDATE()
        )
        DISTSTYLE KEY
        DISTKEY(run_group_id)
        SORTKEY(started_at);
    """)
    print("✅ Table created: etl.run_log")

    # ━━━ CREATE ENTITY CONFIG TABLE ━━━
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl.entity_config (
            entity_name     VARCHAR(50)     PRIMARY KEY,
            target_schema   VARCHAR(50)     NOT NULL DEFAULT 'public',
            target_table    VARCHAR(100)    NOT NULL,
            pk_column       VARCHAR(50)     NOT NULL,
            s3_prefix       VARCHAR(500)    NOT NULL,
            iam_role        VARCHAR(200)    NOT NULL,
            is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
            load_order      INT             NOT NULL DEFAULT 100,
            created_at      TIMESTAMP       DEFAULT GETDATE(),
            updated_at      TIMESTAMP       DEFAULT GETDATE()
        )
        DISTSTYLE ALL;
    """)
    print("✅ Table created: etl.entity_config")

    # ━━━ POPULATE ENTITY CONFIG ━━━
    iam_role = "arn:aws:iam::123456789012:role/quickcart-redshift-s3-role"
    bucket = "quickcart-datalake-prod"

    entities = [
        ("orders", "public", "orders", "order_id",
         f"s3://{bucket}/silver/orders/", iam_role, True, 1),
        ("customers", "public", "customers", "customer_id",
         f"s3://{bucket}/silver/customers/", iam_role, True, 2),
        ("products", "public", "products", "product_id",
         f"s3://{bucket}/silver/products/", iam_role, True, 3)
    ]

    for entity in entities:
        cursor.execute("""
            INSERT INTO etl.entity_config 
            (entity_name, target_schema, target_table, pk_column, s3_prefix, iam_role, is_active, load_order)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, entity)

    print("✅ Entity config populated: orders, customers, products")

    # ━━━ VERIFY ━━━
    cursor.execute("SELECT entity_name, target_table, pk_column, is_active FROM etl.entity_config ORDER BY load_order")
    print(f"\n📋 Entity Configuration:")
    for row in cursor.fetchall():
        print(f"   {row[0]:<15} → {row[1]:<15} PK: {row[2]:<15} Active: {row[3]}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_etl_infrastructure()