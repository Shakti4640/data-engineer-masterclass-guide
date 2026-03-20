# file: 42_04_redshift_cdc_merge.py
# Purpose: Load CDC data from S3 staging into Redshift using MERGE
# Builds on Project 21 (Redshift COPY) and Project 39 (optimization)

import boto3
import psycopg2
import time
from datetime import datetime

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}

REDSHIFT_IAM_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-redshift-s3-role"


def get_redshift_connection():
    """Get Redshift connection using psycopg2"""
    return psycopg2.connect(**REDSHIFT_CONFIG)


def ensure_target_table_exists(cursor, entity_name):
    """
    Create target table if it doesn't exist.
    In production, this would already exist from Project 11/21.
    """
    ddl_map = {
        "orders": """
            CREATE TABLE IF NOT EXISTS public.orders (
                order_id        VARCHAR(30)   PRIMARY KEY,
                customer_id     VARCHAR(30)   NOT NULL,
                product_id      VARCHAR(30)   NOT NULL,
                quantity         INTEGER       NOT NULL,
                unit_price       DECIMAL(10,2) NOT NULL,
                total_amount     DECIMAL(12,2) NOT NULL,
                status           VARCHAR(20)   NOT NULL,
                order_date       DATE          NOT NULL,
                created_at       TIMESTAMP     NOT NULL,
                updated_at       TIMESTAMP     NOT NULL
            )
            DISTKEY(order_id)
            SORTKEY(updated_at);
        """,
        "customers": """
            CREATE TABLE IF NOT EXISTS public.customers (
                customer_id     VARCHAR(30)   PRIMARY KEY,
                name            VARCHAR(100)  NOT NULL,
                email           VARCHAR(200),
                city            VARCHAR(50),
                state           VARCHAR(10),
                tier            VARCHAR(20),
                signup_date     DATE,
                created_at      TIMESTAMP     NOT NULL,
                updated_at      TIMESTAMP     NOT NULL
            )
            DISTKEY(customer_id)
            SORTKEY(updated_at);
        """,
        "products": """
            CREATE TABLE IF NOT EXISTS public.products (
                product_id      VARCHAR(30)   PRIMARY KEY,
                name            VARCHAR(200)  NOT NULL,
                category        VARCHAR(50),
                price           DECIMAL(10,2),
                stock_quantity  INTEGER,
                is_active       BOOLEAN,
                created_at      TIMESTAMP     NOT NULL,
                updated_at      TIMESTAMP     NOT NULL
            )
            DISTKEY(product_id)
            SORTKEY(updated_at);
        """
    }

    if entity_name not in ddl_map:
        raise ValueError(f"Unknown entity: {entity_name}")

    cursor.execute(ddl_map[entity_name])
    print(f"   ✅ Target table verified: public.{entity_name}")


def create_staging_table(cursor, entity_name):
    """
    Create staging table as exact clone of target table.
    
    WHY staging table:
    → COPY loads data into staging first (fast, no contention)
    → Validate row counts before merging
    → Deduplicate if overlap window caused duplicates
    → MERGE from staging into target (atomic)
    → DROP staging when done
    
    NAMING: staging_{entity} (e.g., staging_orders)
    
    IMPORTANT: DROP IF EXISTS first — handles leftover from failed previous run
    """
    staging_table = f"staging_{entity_name}"

    cursor.execute(f"DROP TABLE IF EXISTS public.{staging_table}")
    cursor.execute(f"""
        CREATE TABLE public.{staging_table}
        (LIKE public.{entity_name})
    """)
    print(f"   ✅ Staging table created: public.{staging_table}")
    return staging_table


def copy_cdc_to_staging(cursor, staging_table, s3_path):
    """
    COPY CDC CSV from S3 into Redshift staging table.
    
    Same COPY mechanics as Project 21, but:
    → Much smaller file (1.5 MB vs 480 MB)
    → Loads in seconds instead of minutes
    → IGNOREHEADER 1 because our CDC CSV has a header row
    """
    copy_sql = f"""
        COPY public.{staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        CSV
        IGNOREHEADER 1
        DATEFORMAT 'auto'
        TIMEFORMAT 'auto'
        BLANKSASNULL
        EMPTYASNULL
        REGION '{REGION}'
        COMPUPDATE OFF
        STATUPDATE OFF;
    """

    print(f"   📥 COPY from: {s3_path}")
    start = time.time()
    cursor.execute(copy_sql)
    duration = time.time() - start

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM public.{staging_table}")
    row_count = cursor.fetchone()[0]

    print(f"   ✅ COPY complete: {row_count:,} rows in {duration:.1f}s")
    return row_count


def deduplicate_staging(cursor, staging_table, pk_column):
    """
    Remove duplicate rows from staging table.
    
    WHY duplicates appear:
    → Overlap window (5 minutes) re-extracts rows from previous run
    → Same order_id might appear twice if updated in overlap period
    → Keep only the LATEST version (highest updated_at)
    
    METHOD:
    → Use ROW_NUMBER() window function
    → Partition by PK, order by updated_at DESC
    → Delete all rows where row_number > 1
    """
    # Count before dedup
    cursor.execute(f"SELECT COUNT(*) FROM public.{staging_table}")
    before_count = cursor.fetchone()[0]

    # Find duplicates
    cursor.execute(f"""
        SELECT COUNT(*) - COUNT(DISTINCT {pk_column})
        FROM public.{staging_table}
    """)
    dup_count = cursor.fetchone()[0]

    if dup_count == 0:
        print(f"   ✅ No duplicates found in staging ({before_count} unique rows)")
        return before_count

    print(f"   ⚠️  {dup_count} duplicate(s) found — deduplicating...")

    # Deduplicate: keep latest version per PK
    # Redshift doesn't support DELETE with window functions directly
    # Strategy: create deduped temp → swap
    cursor.execute(f"""
        CREATE TEMP TABLE staging_deduped AS
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {pk_column}
                       ORDER BY updated_at DESC
                   ) AS rn
            FROM public.{staging_table}
        )
        WHERE rn = 1
    """)

    # Replace staging with deduped version
    cursor.execute(f"DELETE FROM public.{staging_table}")
    cursor.execute(f"""
        INSERT INTO public.{staging_table}
        SELECT {get_columns_without_rn(cursor, staging_table)}
        FROM staging_deduped
    """)
    cursor.execute("DROP TABLE staging_deduped")

    cursor.execute(f"SELECT COUNT(*) FROM public.{staging_table}")
    after_count = cursor.fetchone()[0]

    print(f"   ✅ Deduplication complete: {before_count} → {after_count} rows "
          f"({dup_count} duplicates removed)")
    return after_count


def get_columns_without_rn(cursor, staging_table):
    """Get column list for staging table (excluding any temp columns)"""
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '{staging_table}'
        ORDER BY ordinal_position
    """)
    columns = [row[0] for row in cursor.fetchall()]
    return ", ".join(columns)


def merge_into_target_modern(cursor, entity_name, staging_table, pk_column):
    """
    MERGE CDC data from staging into target table.
    
    REDSHIFT MERGE (available since late 2023):
    → Single atomic statement
    → WHEN MATCHED: update all non-PK columns
    → WHEN NOT MATCHED: insert new row
    → No window where data is missing (unlike DELETE+INSERT)
    → Concurrent queries see consistent state (MVCC)
    
    PERFORMANCE for 11K rows into 5.2M row table:
    → Hash join on PK: O(staging_rows)
    → ~2-5 seconds total
    """
    # Get all columns for the entity
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '{entity_name}'
        ORDER BY ordinal_position
    """)
    all_columns = [row[0] for row in cursor.fetchall()]
    non_pk_columns = [c for c in all_columns if c != pk_column]

    # Build UPDATE SET clause
    update_set = ",\n            ".join(
        [f"{col} = source.{col}" for col in non_pk_columns]
    )

    # Build INSERT columns and values
    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"source.{col}" for col in all_columns])

    merge_sql = f"""
        MERGE INTO public.{entity_name} AS target
        USING public.{staging_table} AS source
        ON target.{pk_column} = source.{pk_column}
        WHEN MATCHED THEN
            UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values});
    """

    print(f"   🔀 Executing MERGE: staging_{entity_name} → {entity_name}")
    start = time.time()
    cursor.execute(merge_sql)
    duration = time.time() - start
    print(f"   ✅ MERGE complete in {duration:.1f}s")

    return duration


def merge_into_target_classic(cursor, entity_name, staging_table, pk_column):
    """
    FALLBACK: DELETE+INSERT pattern for older Redshift versions
    that don't support MERGE.
    
    MUST be wrapped in a transaction for atomicity.
    
    FLOW:
    1. BEGIN
    2. DELETE target rows that exist in staging (by PK)
    3. INSERT all staging rows into target
    4. COMMIT
    
    Between DELETE and INSERT: concurrent queries see missing rows
    → This is the main disadvantage vs MERGE
    → In practice: transaction is fast enough that this is rarely noticed
    """
    # Get all columns
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = '{entity_name}'
        ORDER BY ordinal_position
    """)
    all_columns = [row[0] for row in cursor.fetchall()]
    columns_str = ", ".join(all_columns)

    print(f"   🔀 Executing DELETE+INSERT (classic pattern)")
    start = time.time()

    # Step 1: Delete existing rows that have updates
    delete_sql = f"""
        DELETE FROM public.{entity_name}
        WHERE {pk_column} IN (
            SELECT {pk_column} FROM public.{staging_table}
        )
    """
    cursor.execute(delete_sql)

    # Get delete count from cursor
    # Redshift doesn't directly return affected rows in all drivers
    # So we rely on the staging count for logging

    # Step 2: Insert all rows from staging
    insert_sql = f"""
        INSERT INTO public.{entity_name} ({columns_str})
        SELECT {columns_str} FROM public.{staging_table}
    """
    cursor.execute(insert_sql)

    duration = time.time() - start
    print(f"   ✅ DELETE+INSERT complete in {duration:.1f}s")

    return duration


def run_cdc_merge(entity_name, s3_key, use_modern_merge=True):
    """
    Full CDC merge pipeline for one entity.
    
    FLOW:
    1. Ensure target table exists
    2. Create staging table
    3. COPY CDC file into staging
    4. Deduplicate staging
    5. MERGE staging into target
    6. DROP staging
    7. Log results
    """
    pk_map = {
        "orders": "order_id",
        "customers": "customer_id",
        "products": "product_id"
    }

    pk_column = pk_map[entity_name]
    s3_path = f"s3://{BUCKET_NAME}/{s3_key}"

    print(f"\n{'━'*60}")
    print(f"🔄 CDC MERGE: {entity_name}")
    print(f"{'━'*60}")

    conn = get_redshift_connection()
    conn.autocommit = False  # Explicit transaction control
    cursor = conn.cursor()

    try:
        # Pre-merge count
        cursor.execute(f"SELECT COUNT(*) FROM public.{entity_name}")
        pre_count = cursor.fetchone()[0]
        print(f"   📊 Target rows before merge: {pre_count:,}")

        # Step 1: Ensure target exists
        ensure_target_table_exists(cursor, entity_name)

        # Step 2: Create staging table
        staging_table = create_staging_table(cursor, entity_name)
        conn.commit()  # DDL requires commit

        # Step 3: COPY CDC into staging
        staging_rows = copy_cdc_to_staging(cursor, staging_table, s3_path)
        conn.commit()

        if staging_rows == 0:
            print(f"   ℹ️  No rows in staging — skipping merge")
            cursor.execute(f"DROP TABLE IF EXISTS public.{staging_table}")
            conn.commit()
            return {"status": "SKIPPED", "reason": "empty staging"}

        # Step 4: Deduplicate staging
        unique_rows = deduplicate_staging(cursor, staging_table, pk_column)
        conn.commit()

        # Step 5: MERGE (or DELETE+INSERT fallback)
        # Wrap in explicit transaction for atomicity
        cursor.execute("BEGIN")

        if use_modern_merge:
            try:
                merge_duration = merge_into_target_modern(
                    cursor, entity_name, staging_table, pk_column
                )
            except Exception as merge_err:
                if "MERGE" in str(merge_err).upper():
                    print(f"   ⚠️  MERGE not supported — falling back to DELETE+INSERT")
                    cursor.execute("ROLLBACK")
                    cursor.execute("BEGIN")
                    merge_duration = merge_into_target_classic(
                        cursor, entity_name, staging_table, pk_column
                    )
                else:
                    raise
        else:
            merge_duration = merge_into_target_classic(
                cursor, entity_name, staging_table, pk_column
            )

        cursor.execute("COMMIT")

        # Post-merge count
        cursor.execute(f"SELECT COUNT(*) FROM public.{entity_name}")
        post_count = cursor.fetchone()[0]
        new_rows = post_count - pre_count

        print(f"   📊 Target rows after merge: {post_count:,}")
        print(f"   📊 Net new rows: {new_rows:,}")
        print(f"   📊 Updated rows: {unique_rows - new_rows:,}")

        # Step 6: Drop staging
        cursor.execute(f"DROP TABLE IF EXISTS public.{staging_table}")
        conn.commit()
        print(f"   ✅ Staging table dropped")

        # Step 7: Results
        result = {
            "status": "SUCCESS",
            "staging_rows": staging_rows,
            "unique_rows": unique_rows,
            "new_rows": new_rows,
            "updated_rows": unique_rows - new_rows,
            "pre_count": pre_count,
            "post_count": post_count,
            "merge_duration_seconds": merge_duration
        }

        print(f"\n   🎉 MERGE COMPLETE for {entity_name}")
        return result

    except Exception as e:
        conn.rollback()
        print(f"   ❌ MERGE FAILED: {e}")

        # Cleanup staging table on failure
        try:
            cursor.execute(f"DROP TABLE IF EXISTS public.staging_{entity_name}")
            conn.commit()
        except Exception:
            pass

        return {"status": "FAILED", "error": str(e)}

    finally:
        cursor.close()
        conn.close()


def run_full_cdc_merge(cdc_results):
    """
    Merge all CDC extractions into Redshift.
    
    INPUT: results dict from 42_03 extraction script
    Each entity has: s3_key, rows extracted
    """
    print("=" * 70)
    print(f"🔄 REDSHIFT CDC MERGE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    merge_results = {}

    for entity_name, extraction in cdc_results.items():
        if extraction["status"] != "SUCCESS":
            print(f"\n⏭️  Skipping {entity_name} — extraction failed")
            merge_results[entity_name] = {"status": "SKIPPED", "reason": "extraction failed"}
            continue

        if extraction["rows"] == 0:
            print(f"\n⏭️  Skipping {entity_name} — no changed rows")
            merge_results[entity_name] = {"status": "SKIPPED", "reason": "no changes"}
            continue

        s3_key = extraction["s3_key"]
        result = run_cdc_merge(entity_name, s3_key)
        merge_results[entity_name] = result

    # Summary
    print(f"\n{'='*70}")
    print(f"📊 CDC MERGE SUMMARY")
    print(f"{'='*70}")

    for entity, result in merge_results.items():
        status = result["status"]
        if status == "SUCCESS":
            print(f"  ✅ {entity}: "
                  f"{result['new_rows']:,} new + "
                  f"{result['updated_rows']:,} updated "
                  f"({result['merge_duration_seconds']:.1f}s)")
        elif status == "SKIPPED":
            print(f"  ⏭️  {entity}: SKIPPED — {result['reason']}")
        else:
            print(f"  ❌ {entity}: FAILED — {result.get('error', 'Unknown')}")

    return merge_results


# Standalone test
if __name__ == "__main__":
    # Simulate CDC extraction results (normally comes from 42_03)
    today = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%H%M%S")

    sample_results = {
        "orders": {
            "status": "SUCCESS",
            "rows": 11247,
            "s3_key": f"cdc_exports/orders/dt={today}/orders_cdc_{today}_{timestamp}.csv"
        }
    }

    run_full_cdc_merge(sample_results)