# file: 49_02_create_stored_procedures.py
# Purpose: Create all ETL stored procedures in Redshift
# These are the reusable, testable, atomic procedures

import psycopg2

REDSHIFT_CONFIG = {
    "host": "quickcart-cluster.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin",
    "password": "[REDACTED:PASSWORD]"
}


def create_all_procedures():
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    print("=" * 60)
    print("🔧 CREATING STORED PROCEDURES")
    print("=" * 60)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROCEDURE 1: etl.load_entity
    # Core reusable procedure — loads ONE entity from S3 to target
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
    CREATE OR REPLACE PROCEDURE etl.load_entity(
        p_entity_name   VARCHAR(50),
        p_s3_path       VARCHAR(500),
        p_pk_column     VARCHAR(50),
        p_target_schema VARCHAR(50),
        p_target_table  VARCHAR(100),
        p_iam_role      VARCHAR(200),
        p_run_group_id  VARCHAR(50),
        INOUT p_rows_loaded BIGINT
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_staging_table VARCHAR(100);
        v_staging_count BIGINT;
        v_dedup_count   BIGINT;
        v_dup_removed   BIGINT;
        v_target_before BIGINT;
        v_target_after  BIGINT;
        v_start_time    TIMESTAMP;
        v_duration      DECIMAL(10,2);
        v_sql           VARCHAR(65535);
        v_col_list      VARCHAR(65535);
        v_update_set    VARCHAR(65535);
    BEGIN
        v_start_time := GETDATE();
        v_staging_table := 'etl.staging_' || p_entity_name;
        
        RAISE INFO '[%] Starting load for entity: %', p_run_group_id, p_entity_name;

        -- ━━━ STEP 1: Create staging table ━━━
        -- Drop if exists from previous failed run
        EXECUTE 'DROP TABLE IF EXISTS ' || v_staging_table;
        EXECUTE 'CREATE TABLE ' || v_staging_table || ' (LIKE ' || p_target_schema || '.' || p_target_table || ')';
        RAISE INFO '[%] Staging table created: %', p_run_group_id, v_staging_table;

        -- ━━━ STEP 2: COPY from S3 ━━━
        v_sql := 'COPY ' || v_staging_table || 
                 ' FROM ''' || p_s3_path || '''' ||
                 ' IAM_ROLE ''' || p_iam_role || '''' ||
                 ' FORMAT PARQUET';
        
        RAISE INFO '[%] COPY from: %', p_run_group_id, p_s3_path;
        EXECUTE v_sql;

        -- ━━━ STEP 3: Validate staging ━━━
        EXECUTE 'SELECT COUNT(*) FROM ' || v_staging_table INTO v_staging_count;
        RAISE INFO '[%] Staging rows: %', p_run_group_id, v_staging_count;

        IF v_staging_count = 0 THEN
            RAISE EXCEPTION 'No data loaded into staging for entity: %. S3 path: %', 
                            p_entity_name, p_s3_path;
        END IF;

        -- Log staging result
        INSERT INTO etl.run_log (run_group_id, procedure_name, entity_name, status, rows_staged, s3_source_path)
        VALUES (p_run_group_id, 'load_entity', p_entity_name, 'STAGING_COMPLETE', v_staging_count, p_s3_path);

        -- ━━━ STEP 4: Deduplicate staging ━━━
        -- Keep only the latest row per PK (by updated_at if exists, else arbitrary)
        v_sql := 'DELETE FROM ' || v_staging_table || 
                 ' WHERE ' || p_pk_column || ' IN (' ||
                 '  SELECT ' || p_pk_column || 
                 '  FROM ' || v_staging_table ||
                 '  GROUP BY ' || p_pk_column ||
                 '  HAVING COUNT(*) > 1' ||
                 ') AND ctid NOT IN (' ||
                 '  SELECT MAX(ctid) FROM ' || v_staging_table ||
                 '  GROUP BY ' || p_pk_column || 
                 ')';
        
        EXECUTE v_sql;
        EXECUTE 'SELECT COUNT(*) FROM ' || v_staging_table INTO v_dedup_count;
        v_dup_removed := v_staging_count - v_dedup_count;
        
        IF v_dup_removed > 0 THEN
            RAISE WARNING '[%] Deduplication: removed % duplicates from %', 
                          p_run_group_id, v_dup_removed, p_entity_name;
        ELSE
            RAISE INFO '[%] No duplicates found in %', p_run_group_id, p_entity_name;
        END IF;

        -- ━━━ STEP 5: Get target count before merge ━━━
        EXECUTE 'SELECT COUNT(*) FROM ' || p_target_schema || '.' || p_target_table 
                INTO v_target_before;

        -- ━━━ STEP 6: Build dynamic MERGE ━━━
        -- Get column list for the target table
        SELECT INTO v_col_list
            LISTAGG(column_name, ', ') WITHIN GROUP (ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = p_target_schema
        AND table_name = p_target_table;

        -- Build UPDATE SET clause (all non-PK columns)
        SELECT INTO v_update_set
            LISTAGG(column_name || ' = source.' || column_name, ', ') 
            WITHIN GROUP (ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = p_target_schema
        AND table_name = p_target_table
        AND column_name != p_pk_column;

        -- Build INSERT values
        DECLARE v_insert_values VARCHAR(65535);
        SELECT INTO v_insert_values
            LISTAGG('source.' || column_name, ', ') 
            WITHIN GROUP (ORDER BY ordinal_position)
        FROM information_schema.columns
        WHERE table_schema = p_target_schema
        AND table_name = p_target_table;

        -- Execute MERGE
        v_sql := 'MERGE INTO ' || p_target_schema || '.' || p_target_table || ' AS target ' ||
                 'USING ' || v_staging_table || ' AS source ' ||
                 'ON target.' || p_pk_column || ' = source.' || p_pk_column || ' ' ||
                 'WHEN MATCHED THEN UPDATE SET ' || v_update_set || ' ' ||
                 'WHEN NOT MATCHED THEN INSERT (' || v_col_list || ') VALUES (' || v_insert_values || ')';

        RAISE INFO '[%] Executing MERGE for %', p_run_group_id, p_entity_name;
        EXECUTE v_sql;

        -- ━━━ STEP 7: Get target count after merge ━━━
        EXECUTE 'SELECT COUNT(*) FROM ' || p_target_schema || '.' || p_target_table 
                INTO v_target_after;

        p_rows_loaded := v_dedup_count;

        -- ━━━ STEP 8: Drop staging ━━━
        EXECUTE 'DROP TABLE IF EXISTS ' || v_staging_table;

        -- ━━━ STEP 9: Log success ━━━
        v_duration := EXTRACT(EPOCH FROM GETDATE() - v_start_time);
        
        INSERT INTO etl.run_log 
            (run_group_id, procedure_name, entity_name, status,
             rows_staged, rows_deduped, rows_merged,
             rows_inserted, rows_updated,
             completed_at, duration_seconds, s3_source_path)
        VALUES 
            (p_run_group_id, 'load_entity', p_entity_name, 'SUCCESS',
             v_staging_count, v_dedup_count, v_dedup_count,
             v_target_after - v_target_before,
             v_dedup_count - (v_target_after - v_target_before),
             GETDATE(), v_duration, p_s3_path);

        RAISE INFO '[%] % complete: % staged, % merged, % new, % updated (%.1fs)',
                   p_run_group_id, p_entity_name, v_staging_count, v_dedup_count,
                   v_target_after - v_target_before,
                   v_dedup_count - (v_target_after - v_target_before),
                   v_duration;

    EXCEPTION WHEN OTHERS THEN
        -- Cleanup staging table
        EXECUTE 'DROP TABLE IF EXISTS ' || v_staging_table;
        
        -- Log failure
        v_duration := EXTRACT(EPOCH FROM GETDATE() - v_start_time);
        INSERT INTO etl.run_log 
            (run_group_id, procedure_name, entity_name, status,
             error_message, error_state, completed_at, duration_seconds, s3_source_path)
        VALUES 
            (p_run_group_id, 'load_entity', p_entity_name, 'FAILED',
             SQLERRM, SQLSTATE, GETDATE(), v_duration, p_s3_path);

        RAISE WARNING '[%] FAILED loading %: % (state: %)', 
                      p_run_group_id, p_entity_name, SQLERRM, SQLSTATE;
        
        -- Re-raise to caller
        RAISE;
    END;
    $$;
    """)
    print("✅ Procedure created: etl.load_entity")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROCEDURE 2: etl.refresh_views
    # Refreshes all materialized views after data load
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
    CREATE OR REPLACE PROCEDURE etl.refresh_views(
        p_run_group_id VARCHAR(50)
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_start_time TIMESTAMP;
        v_view_name  VARCHAR(100);
        v_cursor     CURSOR FOR
            SELECT schemaname || '.' || matviewname
            FROM pg_catalog.stv_mv_info
            WHERE schema = 'public'
            ORDER BY matviewname;
    BEGIN
        v_start_time := GETDATE();
        RAISE INFO '[%] Refreshing materialized views...', p_run_group_id;

        OPEN v_cursor;
        LOOP
            FETCH v_cursor INTO v_view_name;
            EXIT WHEN NOT FOUND;
            
            RAISE INFO '[%] Refreshing: %', p_run_group_id, v_view_name;
            EXECUTE 'REFRESH MATERIALIZED VIEW ' || v_view_name;
        END LOOP;
        CLOSE v_cursor;

        INSERT INTO etl.run_log (run_group_id, procedure_name, status, completed_at, 
                                  duration_seconds)
        VALUES (p_run_group_id, 'refresh_views', 'SUCCESS', GETDATE(),
                EXTRACT(EPOCH FROM GETDATE() - v_start_time));

        RAISE INFO '[%] All materialized views refreshed', p_run_group_id;

    EXCEPTION WHEN OTHERS THEN
        INSERT INTO etl.run_log (run_group_id, procedure_name, status, 
                                  error_message, error_state, completed_at)
        VALUES (p_run_group_id, 'refresh_views', 'FAILED', SQLERRM, SQLSTATE, GETDATE());
        RAISE;
    END;
    $$;
    """)
    print("✅ Procedure created: etl.refresh_views")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROCEDURE 3: etl.run_analyze
    # Updates table statistics for query optimizer
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
    CREATE OR REPLACE PROCEDURE etl.run_analyze(
        p_run_group_id VARCHAR(50)
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_entity RECORD;
    BEGIN
        RAISE INFO '[%] Running ANALYZE on loaded tables...', p_run_group_id;

        FOR v_entity IN 
            SELECT target_schema, target_table 
            FROM etl.entity_config 
            WHERE is_active = TRUE
            ORDER BY load_order
        LOOP
            RAISE INFO '[%] ANALYZE %.%', p_run_group_id, v_entity.target_schema, v_entity.target_table;
            EXECUTE 'ANALYZE ' || v_entity.target_schema || '.' || v_entity.target_table;
        END LOOP;

        RAISE INFO '[%] ANALYZE complete', p_run_group_id;
    END;
    $$;
    """)
    print("✅ Procedure created: etl.run_analyze")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROCEDURE 4: etl.load_daily_data — MASTER ORCHESTRATOR
    # Loads ALL entities, refreshes views, runs ANALYZE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
    CREATE OR REPLACE PROCEDURE etl.load_daily_data(
        p_process_date  DATE,
        INOUT p_status  VARCHAR(20)
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_run_group_id  VARCHAR(50);
        v_start_time    TIMESTAMP;
        v_duration      DECIMAL(10,2);
        v_entity        RECORD;
        v_s3_path       VARCHAR(500);
        v_rows_loaded   BIGINT;
        v_total_rows    BIGINT := 0;
        v_entities_loaded INT := 0;
        v_year          VARCHAR(4);
        v_month         VARCHAR(2);
        v_day           VARCHAR(2);
    BEGIN
        v_start_time := GETDATE();
        v_run_group_id := 'run_' || TO_CHAR(GETDATE(), 'YYYYMMDD_HH24MISS');
        v_year  := TO_CHAR(p_process_date, 'YYYY');
        v_month := TO_CHAR(p_process_date, 'MM');
        v_day   := TO_CHAR(p_process_date, 'DD');

        RAISE INFO '════════════════════════════════════════════════════';
        RAISE INFO '[%] ETL PIPELINE START — Date: %', v_run_group_id, p_process_date;
        RAISE INFO '════════════════════════════════════════════════════';

        -- Log pipeline start
        INSERT INTO etl.run_log (run_group_id, procedure_name, process_date, status)
        VALUES (v_run_group_id, 'load_daily_data', p_process_date, 'STARTED');

        -- ━━━ PHASE 1: Load each entity ━━━
        RAISE INFO '[%] PHASE 1: Loading entities...', v_run_group_id;

        FOR v_entity IN 
            SELECT entity_name, target_schema, target_table, pk_column, s3_prefix, iam_role
            FROM etl.entity_config 
            WHERE is_active = TRUE 
            ORDER BY load_order
        LOOP
            -- Build S3 path with date partition
            v_s3_path := v_entity.s3_prefix || 
                         'year=' || v_year || '/month=' || v_month || '/day=' || v_day || '/';
            
            v_rows_loaded := 0;

            RAISE INFO '[%] Loading: %', v_run_group_id, v_entity.entity_name;

            CALL etl.load_entity(
                v_entity.entity_name,
                v_s3_path,
                v_entity.pk_column,
                v_entity.target_schema,
                v_entity.target_table,
                v_entity.iam_role,
                v_run_group_id,
                v_rows_loaded
            );

            v_total_rows := v_total_rows + v_rows_loaded;
            v_entities_loaded := v_entities_loaded + 1;
        END LOOP;

        -- ━━━ PHASE 2: Refresh materialized views ━━━
        RAISE INFO '[%] PHASE 2: Refreshing views...', v_run_group_id;
        CALL etl.refresh_views(v_run_group_id);

        -- ━━━ PHASE 3: Update statistics ━━━
        RAISE INFO '[%] PHASE 3: Running ANALYZE...', v_run_group_id;
        CALL etl.run_analyze(v_run_group_id);

        -- ━━━ PHASE 4: Log success ━━━
        v_duration := EXTRACT(EPOCH FROM GETDATE() - v_start_time);

        INSERT INTO etl.run_log 
            (run_group_id, procedure_name, process_date, status,
             rows_merged, completed_at, duration_seconds)
        VALUES 
            (v_run_group_id, 'load_daily_data', p_process_date, 'SUCCESS',
             v_total_rows, GETDATE(), v_duration);

        p_status := 'SUCCESS';

        RAISE INFO '════════════════════════════════════════════════════';
        RAISE INFO '[%] ETL PIPELINE COMPLETE', v_run_group_id;
        RAISE INFO '[%] Entities loaded: %', v_run_group_id, v_entities_loaded;
        RAISE INFO '[%] Total rows: %', v_run_group_id, v_total_rows;
        RAISE INFO '[%] Duration: % seconds', v_run_group_id, v_duration;
        RAISE INFO '════════════════════════════════════════════════════';

    EXCEPTION WHEN OTHERS THEN
        v_duration := EXTRACT(EPOCH FROM GETDATE() - v_start_time);

        -- Log failure
        INSERT INTO etl.run_log 
            (run_group_id, procedure_name, process_date, status,
             error_message, error_state, completed_at, duration_seconds)
        VALUES 
            (v_run_group_id, 'load_daily_data', p_process_date, 'FAILED',
             SQLERRM, SQLSTATE, GETDATE(), v_duration);

        p_status := 'FAILED';

        RAISE WARNING '════════════════════════════════════════════════════';
        RAISE WARNING '[%] ETL PIPELINE FAILED', v_run_group_id;
        RAISE WARNING '[%] Error: %', v_run_group_id, SQLERRM;
        RAISE WARNING '[%] State: %', v_run_group_id, SQLSTATE;
        RAISE WARNING '[%] Duration: % seconds', v_run_group_id, v_duration;
        RAISE WARNING '════════════════════════════════════════════════════';

        -- Re-raise so caller knows it failed
        RAISE;
    END;
    $$;
    """)
    print("✅ Procedure created: etl.load_daily_data (MASTER)")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROCEDURE 5: etl.get_run_summary — Query audit log
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
    CREATE OR REPLACE PROCEDURE etl.get_run_summary(
        p_days_back INT DEFAULT 7
    )
    LANGUAGE plpgsql
    AS $$
    DECLARE
        v_rec RECORD;
    BEGIN
        RAISE INFO 'ETL Run Summary — Last % days', p_days_back;
        RAISE INFO '────────────────────────────────────────────────';

        FOR v_rec IN
            SELECT 
                run_group_id,
                process_date,
                status,
                SUM(rows_merged) as total_rows,
                MAX(duration_seconds) as duration,
                MAX(error_message) as error_msg
            FROM etl.run_log
            WHERE procedure_name = 'load_daily_data'
            AND started_at >= DATEADD(day, -p_days_back, GETDATE())
            GROUP BY run_group_id, process_date, status
            ORDER BY MAX(started_at) DESC
            LIMIT 20
        LOOP
            IF v_rec.status = 'SUCCESS' THEN
                RAISE INFO '  ✅ % | Date: % | Rows: % | Duration: %s',
                           v_rec.run_group_id, v_rec.process_date, 
                           v_rec.total_rows, v_rec.duration;
            ELSIF v_rec.status = 'FAILED' THEN
                RAISE INFO '  ❌ % | Date: % | Error: %',
                           v_rec.run_group_id, v_rec.process_date, 
                           LEFT(v_rec.error_msg, 100);
            ELSE
                RAISE INFO '  ⏳ % | Date: % | Status: %',
                           v_rec.run_group_id, v_rec.process_date, v_rec.status;
            END IF;
        END LOOP;
    END;
    $$;
    """)
    print("✅ Procedure created: etl.get_run_summary")

    cursor.close()
    conn.close()

    print(f"\n✅ ALL PROCEDURES CREATED")


if __name__ == "__main__":
    create_all_procedures()