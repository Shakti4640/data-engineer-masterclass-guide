# file: 65_02_setup_redshift_cdc_tables.py
# Run from: Account B (222222222222)
# Purpose: Create staging tables, watermark table, and merge stored procedure

import psycopg2

REDSHIFT_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "analytics"
REDSHIFT_USER = "etl_writer"
REDSHIFT_PASS = "R3dsh1ftWr1t3r#2025"
SCHEMA = "quickcart_ops"


def get_connection():
    conn = psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB, user=REDSHIFT_USER,
        password=REDSHIFT_PASS
    )
    conn.autocommit = True
    return conn


def create_staging_tables():
    """
    Create staging tables that mirror target structure
    → Used as landing zone for CDC deltas
    → TRUNCATED before each load, filled from S3 COPY
    """
    conn = get_connection()
    cur = conn.cursor()

    print("📦 Creating staging tables...")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.stg_orders (
            order_id        VARCHAR(30),
            customer_id     VARCHAR(30),
            product_id      VARCHAR(20),
            quantity         INTEGER,
            unit_price       DECIMAL(10,2),
            total_amount     DECIMAL(12,2),
            status           VARCHAR(20),
            order_date       DATE,
            created_at       TIMESTAMP,
            updated_at       TIMESTAMP,
            is_deleted       BOOLEAN,
            etl_loaded_at    TIMESTAMP DEFAULT GETDATE(),
            etl_source       VARCHAR(50) DEFAULT 'cdc_mariadb',
            cdc_operation    VARCHAR(10)
        )
        DISTSTYLE KEY DISTKEY(order_id)
        SORTKEY(order_id);
    """)
    print(f"   ✅ {SCHEMA}.stg_orders created")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.stg_customers (
            customer_id     VARCHAR(30),
            name            VARCHAR(200),
            email           VARCHAR(200),
            city            VARCHAR(100),
            state           VARCHAR(50),
            tier            VARCHAR(20),
            signup_date     DATE,
            created_at      TIMESTAMP,
            updated_at      TIMESTAMP,
            is_deleted      BOOLEAN,
            etl_loaded_at   TIMESTAMP DEFAULT GETDATE(),
            etl_source      VARCHAR(50) DEFAULT 'cdc_mariadb',
            cdc_operation   VARCHAR(10)
        )
        DISTSTYLE KEY DISTKEY(customer_id)
        SORTKEY(customer_id);
    """)
    print(f"   ✅ {SCHEMA}.stg_customers created")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.stg_products (
            product_id      VARCHAR(20),
            name            VARCHAR(200),
            category        VARCHAR(100),
            price           DECIMAL(10,2),
            stock_quantity  INTEGER,
            is_active       BOOLEAN,
            created_at      TIMESTAMP,
            updated_at      TIMESTAMP,
            is_deleted      BOOLEAN,
            etl_loaded_at   TIMESTAMP DEFAULT GETDATE(),
            etl_source      VARCHAR(50) DEFAULT 'cdc_mariadb',
            cdc_operation   VARCHAR(10)
        )
        DISTSTYLE KEY DISTKEY(product_id)
        SORTKEY(product_id);
    """)
    print(f"   ✅ {SCHEMA}.stg_products created")

    cur.close()
    conn.close()


def create_watermark_table():
    """
    Watermark table tracks CDC state per source table
    
    FIELDS:
    → table_name: which source table
    → last_extracted_at: timestamp of last successful extraction
    → last_run_id: unique identifier for the last run
    → rows_processed: total delta rows in last run
    → rows_inserted/updated/deleted: breakdown of changes
    → run_started_at/completed_at: timing for monitoring
    """
    conn = get_connection()
    cur = conn.cursor()

    print("\n📦 Creating watermark table...")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.cdc_watermarks (
            table_name          VARCHAR(100) PRIMARY KEY,
            last_extracted_at   TIMESTAMP NOT NULL,
            last_run_id         VARCHAR(100),
            rows_processed      BIGINT DEFAULT 0,
            rows_inserted       BIGINT DEFAULT 0,
            rows_updated        BIGINT DEFAULT 0,
            rows_deleted        BIGINT DEFAULT 0,
            run_started_at      TIMESTAMP,
            run_completed_at    TIMESTAMP,
            status              VARCHAR(20) DEFAULT 'initialized'
        )
        DISTSTYLE ALL;
    """)
    print(f"   ✅ {SCHEMA}.cdc_watermarks created")

    # Initialize watermarks for each table
    # Set to a past date so first run captures all existing data
    tables = ["orders", "customers", "products"]
    for table in tables:
        cur.execute(f"""
            INSERT INTO {SCHEMA}.cdc_watermarks (table_name, last_extracted_at, status)
            SELECT '{table}', '2024-01-01 00:00:00', 'initialized'
            WHERE NOT EXISTS (
                SELECT 1 FROM {SCHEMA}.cdc_watermarks WHERE table_name = '{table}'
            );
        """)
    print(f"   ✅ Watermarks initialized for: {', '.join(tables)}")

    cur.close()
    conn.close()


def create_merge_stored_procedure():
    """
    Stored procedure that executes the MERGE pattern:
    1. Delete matching rows from target
    2. Insert non-deleted rows from staging
    3. Update watermark
    4. All within single transaction
    
    PARAMETERS:
    → p_schema: schema name
    → p_table: target table name (e.g., 'orders')
    → p_staging_table: staging table name (e.g., 'stg_orders')
    → p_primary_key: primary key column (e.g., 'order_id')
    → p_run_id: unique run identifier
    → p_watermark_value: new watermark timestamp
    """
    conn = get_connection()
    cur = conn.cursor()

    print("\n📦 Creating merge stored procedure...")

    cur.execute(f"""
        CREATE OR REPLACE PROCEDURE {SCHEMA}.sp_cdc_merge(
            p_schema VARCHAR(100),
            p_table VARCHAR(100),
            p_staging_table VARCHAR(100),
            p_primary_key VARCHAR(100),
            p_run_id VARCHAR(100),
            p_watermark_value TIMESTAMP
        )
        AS $$
        DECLARE
            v_staging_count BIGINT;
            v_deleted_count BIGINT;
            v_inserted_count BIGINT;
            v_soft_deleted_count BIGINT;
            v_target_table VARCHAR(200);
            v_staging_full VARCHAR(200);
        BEGIN
            v_target_table := p_schema || '.' || p_table;
            v_staging_full := p_schema || '.' || p_staging_table;

            -- Count staging rows
            EXECUTE 'SELECT COUNT(*) FROM ' || v_staging_full INTO v_staging_count;
            
            IF v_staging_count = 0 THEN
                -- No changes — update watermark and exit
                UPDATE quickcart_ops.cdc_watermarks
                SET last_extracted_at = p_watermark_value,
                    last_run_id = p_run_id,
                    rows_processed = 0,
                    rows_inserted = 0,
                    rows_updated = 0,
                    rows_deleted = 0,
                    run_completed_at = GETDATE(),
                    status = 'completed_no_changes'
                WHERE table_name = p_table;
                
                RAISE INFO 'No changes detected for %', p_table;
                RETURN;
            END IF;

            RAISE INFO 'Starting merge for % — % staging rows', p_table, v_staging_count;

            -- Count soft deletes in staging
            EXECUTE 'SELECT COUNT(*) FROM ' || v_staging_full || ' WHERE is_deleted = TRUE'
                INTO v_soft_deleted_count;

            -- STEP 1: DELETE existing rows that appear in staging
            -- This handles both UPDATES (delete old version) and DELETES
            EXECUTE 'DELETE FROM ' || v_target_table ||
                    ' WHERE ' || p_primary_key || ' IN ' ||
                    '(SELECT ' || p_primary_key || ' FROM ' || v_staging_full || ')';
            
            GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
            RAISE INFO 'Deleted % existing rows from target', v_deleted_count;

            -- STEP 2: INSERT non-deleted rows from staging
            -- New rows: inserted fresh
            -- Updated rows: inserted with new values (after old version deleted above)
            -- Soft-deleted rows: NOT inserted (filtered by WHERE)
            EXECUTE 'INSERT INTO ' || v_target_table ||
                    ' SELECT * FROM ' || v_staging_full ||
                    ' WHERE is_deleted = FALSE';
            
            GET DIAGNOSTICS v_inserted_count = ROW_COUNT;
            RAISE INFO 'Inserted % rows into target', v_inserted_count;

            -- STEP 3: Update watermark (INSIDE transaction)
            UPDATE quickcart_ops.cdc_watermarks
            SET last_extracted_at = p_watermark_value,
                last_run_id = p_run_id,
                rows_processed = v_staging_count,
                rows_inserted = v_inserted_count - (v_deleted_count - v_soft_deleted_count),
                rows_updated = v_deleted_count - v_soft_deleted_count,
                rows_deleted = v_soft_deleted_count,
                run_completed_at = GETDATE(),
                status = 'completed'
            WHERE table_name = p_table;

            -- STEP 4: Update table statistics
            EXECUTE 'ANALYZE ' || v_target_table;

            RAISE INFO 'Merge complete for %: % processed, % net inserted, % updated, % deleted',
                p_table, v_staging_count, 
                v_inserted_count - (v_deleted_count - v_soft_deleted_count),
                v_deleted_count - v_soft_deleted_count,
                v_soft_deleted_count;

        EXCEPTION
            WHEN OTHERS THEN
                -- Log failure in watermark table
                UPDATE quickcart_ops.cdc_watermarks
                SET status = 'failed',
                    last_run_id = p_run_id,
                    run_completed_at = GETDATE()
                WHERE table_name = p_table;
                
                RAISE;
        END;
        $$ LANGUAGE plpgsql;
    """)
    print(f"   ✅ Stored procedure created: {SCHEMA}.sp_cdc_merge")

    cur.close()
    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("📦 SETTING UP REDSHIFT CDC INFRASTRUCTURE")
    print("=" * 60)

    create_staging_tables()
    create_watermark_table()
    create_merge_stored_procedure()

    print(f"\n{'='*60}")
    print("✅ Redshift CDC infrastructure ready")
    print(f"{'='*60}")