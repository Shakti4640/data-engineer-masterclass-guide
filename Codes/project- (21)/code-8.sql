-- file: 08_copy_diagnostics.sql
-- Essential diagnostic queries for troubleshooting COPY

-- ═══════════════════════════════════════════════
-- 1. FIND YOUR MOST RECENT COPY QUERY ID
-- ═══════════════════════════════════════════════
SELECT query, TRIM(querytxt) as sql, 
       starttime, endtime,
       DATEDIFF(seconds, starttime, endtime) as duration_sec
FROM stl_query
WHERE TRIM(querytxt) LIKE 'COPY%'
ORDER BY starttime DESC
LIMIT 5;

-- ═══════════════════════════════════════════════
-- 2. ROW-LEVEL ERRORS (most important diagnostic)
-- ═══════════════════════════════════════════════
-- Replace {query_id} with your COPY query ID
SELECT 
    TRIM(filename) as file,
    line_number,
    TRIM(colname) as column_name,
    TRIM(type) as data_type,
    TRIM(raw_field_value) as raw_value,
    TRIM(err_reason) as error_reason
FROM stl_load_errors
WHERE query = {query_id}    -- or use pg_last_copy_id()
ORDER BY line_number
LIMIT 50;

-- ═══════════════════════════════════════════════
-- 3. PER-FILE LOAD STATISTICS
-- ═══════════════════════════════════════════════
SELECT 
    TRIM(filename) as file,
    lines_scanned as rows,
    ROUND(bytes_scanned / 1024.0 / 1024.0, 2) as mb
FROM stl_load_commits
WHERE query = pg_last_copy_id()
ORDER BY filename;

-- ═══════════════════════════════════════════════
-- 4. S3 READ PERFORMANCE PER SLICE
-- ═══════════════════════════════════════════════
SELECT 
    slice,
    TRIM(key) as s3_key,
    ROUND(transfer_size / 1024.0 / 1024.0, 2) as mb_transferred,
    DATEDIFF(milliseconds, start_time, end_time) as duration_ms
FROM stl_file_scan
WHERE query = pg_last_copy_id()
ORDER BY slice, start_time;

-- ═══════════════════════════════════════════════
-- 5. S3 CONNECTION ERRORS
-- ═══════════════════════════════════════════════
SELECT 
    TRIM(key) as s3_key,
    TRIM(error) as error_message,
    recordtime
FROM stl_s3client_error
ORDER BY recordtime DESC
LIMIT 20;

-- ═══════════════════════════════════════════════
-- 6. CURRENTLY RUNNING COPY (live monitoring)
-- ═══════════════════════════════════════════════
SELECT 
    query,
    TRIM(table_name) as target_table,
    lines_scanned,
    ROUND(bytes_scanned / 1024.0 / 1024.0, 2) as mb_scanned,
    pct_complete
FROM stv_load_state;

-- ═══════════════════════════════════════════════
-- 7. SLICE DISTRIBUTION CHECK (after load)
-- ═══════════════════════════════════════════════
-- Verifies data is evenly distributed across slices
-- Skewed distribution = performance problems
SELECT 
    slice,
    COUNT(*) as rows_on_slice,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct
FROM stv_blocklist
WHERE tbl = (
    SELECT id FROM stv_tbl_perm WHERE name = 'orders' LIMIT 1
)
GROUP BY slice
ORDER BY slice;

-- ═══════════════════════════════════════════════
-- 8. COPY HISTORY (last 7 days)
-- ═══════════════════════════════════════════════
SELECT 
    q.query,
    TRIM(q.querytxt) as copy_command,
    q.starttime,
    DATEDIFF(seconds, q.starttime, q.endtime) as duration_sec,
    COALESCE(lc.total_rows, 0) as rows_loaded,
    COALESCE(le.error_count, 0) as errors
FROM stl_query q
LEFT JOIN (
    SELECT query, SUM(lines_scanned) as total_rows
    FROM stl_load_commits GROUP BY query
) lc ON q.query = lc.query
LEFT JOIN (
    SELECT query, COUNT(*) as error_count
    FROM stl_load_errors GROUP BY query
) le ON q.query = le.query
WHERE TRIM(q.querytxt) LIKE 'COPY%'
  AND q.starttime > GETDATE() - INTERVAL '7 days'
ORDER BY q.starttime DESC;