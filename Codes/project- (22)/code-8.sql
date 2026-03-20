-- file: 08_unload_options_reference.sql
-- Every UNLOAD option — with explanation and use case

-- ═══════════════════════════════════════════════════════════
-- BASIC SYNTAX
-- ═══════════════════════════════════════════════════════════

-- Simplest possible UNLOAD
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole';

-- ═══════════════════════════════════════════════════════════
-- OUTPUT FORMAT OPTIONS
-- ═══════════════════════════════════════════════════════════

-- CSV (comma-separated, handles quoting automatically)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn' CSV;

-- Pipe-delimited (default if no CSV/FORMAT specified)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn' DELIMITER '|';

-- Tab-delimited
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn' DELIMITER '\t';

-- Parquet (columnar, self-describing, compressed)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn' FORMAT AS PARQUET;

-- JSON
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/'
IAM_ROLE 'arn' FORMAT AS JSON;

-- ═══════════════════════════════════════════════════════════
-- COMPRESSION OPTIONS
-- ═══════════════════════════════════════════════════════════

-- GZIP (most common, good compression ratio)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' GZIP;

-- BZIP2 (better compression, slower)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' BZIP2;

-- ZSTD (best balance of speed and compression)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' ZSTD;

-- Parquet: uses Snappy internally (no separate keyword needed)

-- ═══════════════════════════════════════════════════════════
-- HEADER AND FORMATTING
-- ═══════════════════════════════════════════════════════════

-- Add column headers to CSV output
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' CSV HEADER;

-- Control NULL representation
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' NULL AS 'NULL';

-- Add quotes around all fields (not just those with special chars)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' ADDQUOTES;

-- Fixed-width output
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
FIXEDWIDTH '20,15,12,5,10,12,10,10';

-- ═══════════════════════════════════════════════════════════
-- PARALLELISM AND FILE CONTROL
-- ═══════════════════════════════════════════════════════════

-- Parallel (default) — one file per slice
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' PARALLEL ON;

-- Single file — all data through leader node (SLOW)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' PARALLEL OFF;

-- Max file size (splits large output into smaller files)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' MAXFILESIZE 256 MB;

-- ═══════════════════════════════════════════════════════════
-- OVERWRITE CONTROL
-- ═══════════════════════════════════════════════════════════

-- Overwrite files with same name (keeps other files)
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' ALLOWOVERWRITE;

-- Delete ALL existing files at prefix, then write new
-- ⚠️  DANGEROUS — deletes everything under the prefix
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn' CLEANPATH;

-- ═══════════════════════════════════════════════════════════
-- PARTITIONING
-- ═══════════════════════════════════════════════════════════

-- Hive-style partition by single column
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
PARTITION BY (order_date)
CSV HEADER GZIP;

-- Partition by multiple columns
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
PARTITION BY (order_date, status)
FORMAT AS PARQUET;

-- INCLUDE partitioned columns in output files
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
PARTITION BY (order_date) INCLUDE
CSV HEADER;

-- By default: partition columns are EXCLUDED from file content
-- INCLUDE keyword: partition columns appear in BOTH path AND file

-- ═══════════════════════════════════════════════════════════
-- ENCRYPTION
-- ═══════════════════════════════════════════════════════════

-- Client-side encryption with KMS
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
ENCRYPTED
KMS_KEY_ID 'arn:aws:kms:us-east-2:123456789012:key/abcd-1234';

-- Server-side encryption (uses bucket default encryption)
-- No special UNLOAD keyword needed — SSE-S3 applied by bucket policy

-- ═══════════════════════════════════════════════════════════
-- MANIFEST OUTPUT (for tracking what was written)
-- ═══════════════════════════════════════════════════════════

-- Generate manifest listing all output files
UNLOAD ('SELECT * FROM orders')
TO 's3://bucket/prefix/' IAM_ROLE 'arn'
MANIFEST
CSV GZIP;

-- Creates: s3://bucket/prefix/manifest
-- Contains JSON listing all output file paths
-- Useful for: downstream COPY (feed manifest back as input)
-- Creates perfect COPY → UNLOAD → COPY round-trip

-- ═══════════════════════════════════════════════════════════
-- PRODUCTION TEMPLATE (copy-paste and customize)
-- ═══════════════════════════════════════════════════════════

-- TEMPLATE A: Human-readable CSV (Finance, Excel)
UNLOAD ('SELECT * FROM public.orders WHERE order_date = ''2025-01-29''')
TO 's3://quickcart-raw-data-prod/exports/finance/2025-01-29/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
CSV
HEADER
PARALLEL OFF
ALLOWOVERWRITE
REGION 'us-east-2';

-- TEMPLATE B: Compressed partitioned CSV (partner sharing)
UNLOAD ('SELECT * FROM public.orders')
TO 's3://quickcart-raw-data-prod/exports/partner/orders/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
CSV
HEADER
GZIP
PARTITION BY (order_date)
CLEANPATH
MANIFEST
REGION 'us-east-2';

-- TEMPLATE C: Analytics Parquet (data science, Athena, Glue)
UNLOAD ('SELECT * FROM public.orders')
TO 's3://quickcart-raw-data-prod/exports/analytics/orders/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
FORMAT AS PARQUET
PARTITION BY (order_date)
CLEANPATH
MAXFILESIZE 512 MB
REGION 'us-east-2';