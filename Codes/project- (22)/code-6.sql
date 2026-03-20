-- file: 06_unload_diagnostics.sql
-- Essential queries for monitoring and troubleshooting UNLOAD

-- ═══════════════════════════════════════════════════════════
-- 1. RECENT UNLOAD HISTORY (last 7 days)
-- ═══════════════════════════════════════════════════════════
SELECT 
    q.query,
    q.starttime,
    DATEDIFF(seconds, q.starttime, q.endtime) as duration_sec,
    ul.file_count,
    ul.total_rows,
    ul.total_mb
FROM stl_query q
JOIN (
    SELECT 
        query,
        COUNT(*) as file_count,
        SUM(line_count) as total_rows,
        ROUND(SUM(transfer_size) / 1024.0 / 1024.0, 2) as total_mb
    FROM stl_unload_log
    GROUP BY query
) ul ON q.query = ul.query
WHERE TRIM(q.querytxt) LIKE 'UNLOAD%'
  AND q.starttime > GETDATE() - INTERVAL '7 days'
ORDER BY q.starttime DESC;

-- ═══════════════════════════════════════════════════════════
-- 2. FILE-LEVEL DETAILS FOR MOST RECENT UNLOAD
-- ═══════════════════════════════════════════════════════════
SELECT 
    TRIM(path) as s3_path,
    line_count as rows_written,
    ROUND(transfer_size / 1024.0 / 1024.0, 2) as size_mb
FROM stl_unload_log
WHERE query = pg_last_query_id()
ORDER BY path;

-- ═══════════════════════════════════════════════════════════
-- 3. COMPARE UNLOAD OUTPUT vs SOURCE QUERY
-- ═══════════════════════════════════════════════════════════
-- Validates no data loss during export
WITH unload_stats AS (
    SELECT 
        SUM(line_count) as unloaded_rows
    FROM stl_unload_log
    WHERE query = pg_last_query_id()
),
source_stats AS (
    -- Replace with your actual UNLOAD inner query
    SELECT COUNT(*) as source_rows
    FROM public.orders
    WHERE order_date = '2025-01-29'
)
SELECT 
    s.source_rows,
    u.unloaded_rows,
    CASE 
        WHEN s.source_rows = u.unloaded_rows THEN '✅ MATCH'
        ELSE '❌ MISMATCH — investigate!'
    END as validation
FROM source_stats s, unload_stats u;

-- ═══════════════════════════════════════════════════════════
-- 4. UNLOAD PERFORMANCE BY SLICE (parallel efficiency)
-- ═══════════════════════════════════════════════════════════
SELECT 
    SUBSTRING(TRIM(path), LEN(TRIM(path)) - 20, 21) as file_suffix,
    line_count,
    ROUND(transfer_size / 1024.0 / 1024.0, 2) as size_mb
FROM stl_unload_log
WHERE query = pg_last_query_id()
ORDER BY line_count DESC;

-- If one file has 80% of the rows → data skew problem
-- All files should have roughly equal row counts

-- ═══════════════════════════════════════════════════════════
-- 5. FIND UNLOAD COMMANDS THAT TOOK LONGER THAN 5 MINUTES
-- ═══════════════════════════════════════════════════════════
SELECT 
    query,
    TRIM(querytxt) as unload_command,
    starttime,
    DATEDIFF(seconds, starttime, endtime) as duration_sec,
    aborted
FROM stl_query
WHERE TRIM(querytxt) LIKE 'UNLOAD%'
  AND DATEDIFF(seconds, starttime, endtime) > 300
ORDER BY starttime DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════
-- 6. DAILY UNLOAD VOLUME TREND
-- ═══════════════════════════════════════════════════════════
SELECT 
    DATE(q.starttime) as export_date,
    COUNT(DISTINCT q.query) as unload_count,
    SUM(ul.total_rows) as total_rows_exported,
    ROUND(SUM(ul.total_mb), 2) as total_mb_exported
FROM stl_query q
JOIN (
    SELECT 
        query,
        SUM(line_count) as total_rows,
        SUM(transfer_size) / 1024.0 / 1024.0 as total_mb
    FROM stl_unload_log
    GROUP BY query
) ul ON q.query = ul.query
WHERE TRIM(q.querytxt) LIKE 'UNLOAD%'
  AND q.starttime > GETDATE() - INTERVAL '30 days'
GROUP BY DATE(q.starttime)
ORDER BY export_date DESC;