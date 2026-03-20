-- file: 13_copy_options_reference.sql
-- Every COPY option you need to know — with explanation

-- ═══════════════════════════════════════════════════════════
-- FORMAT OPTIONS
-- ═══════════════════════════════════════════════════════════

-- CSV format (comma-separated with optional quoting)
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' CSV;

-- Custom delimiter (pipe-separated)
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' DELIMITER '|';

-- Tab-separated
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' DELIMITER '\t';

-- JSON format (auto-detect structure)
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' FORMAT AS JSON 'auto';

-- JSON with explicit JSONPaths mapping
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' 
FORMAT AS JSON 's3://bucket/jsonpaths.json';

-- Parquet format (schema from file metadata)
COPY t FROM 's3://bucket/path/' IAM_ROLE 'arn' FORMAT AS PARQUET;

-- ═══════════════════════════════════════════════════════════
-- COMPRESSION OPTIONS
-- ═══════════════════════════════════════════════════════════

COPY t FROM 's3://...' IAM_ROLE 'arn' GZIP;        -- .gz files
COPY t FROM 's3://...' IAM_ROLE 'arn' LZOP;        -- .lzo files
COPY t FROM 's3://...' IAM_ROLE 'arn' BZIP2;       -- .bz2 files
COPY t FROM 's3://...' IAM_ROLE 'arn' ZSTD;        -- .zst files
-- Parquet: compression handled internally (no keyword needed)

-- ═══════════════════════════════════════════════════════════
-- HEADER / ROW HANDLING
-- ═══════════════════════════════════════════════════════════

COPY t FROM 's3://...' IAM_ROLE 'arn' IGNOREHEADER 1;   -- Skip 1 header row
COPY t FROM 's3://...' IAM_ROLE 'arn' IGNOREHEADER 3;   -- Skip 3 header rows
COPY t FROM 's3://...' IAM_ROLE 'arn' ACCEPTINVCHARS;   -- Replace invalid UTF-8
COPY t FROM 's3://...' IAM_ROLE 'arn' BLANKSASNULL;     -- Empty string → NULL
COPY t FROM 's3://...' IAM_ROLE 'arn' EMPTYASNULL;      -- '' → NULL
COPY t FROM 's3://...' IAM_ROLE 'arn' NULL AS 'NA';     -- 'NA' string → NULL
COPY t FROM 's3://...' IAM_ROLE 'arn' REMOVEQUOTES;     -- Strip surrounding quotes
COPY t FROM 's3://...' IAM_ROLE 'arn' TRIMBLANKS;       -- Trim trailing spaces

-- ═══════════════════════════════════════════════════════════
-- DATE / TIME HANDLING
-- ═══════════════════════════════════════════════════════════

COPY t FROM 's3://...' IAM_ROLE 'arn' DATEFORMAT 'auto';
COPY t FROM 's3://...' IAM_ROLE 'arn' DATEFORMAT 'YYYY-MM-DD';
COPY t FROM 's3://...' IAM_ROLE 'arn' TIMEFORMAT 'auto';
COPY t FROM 's3://...' IAM_ROLE 'arn' TIMEFORMAT 'YYYY-MM-DD HH:MI:SS';
COPY t FROM 's3://...' IAM_ROLE 'arn' TIMEFORMAT 'epochsecs';
COPY t FROM 's3://...' IAM_ROLE 'arn' TIMEFORMAT 'epochmillisecs';

-- ═══════════════════════════════════════════════════════════
-- ERROR HANDLING
-- ═══════════════════════════════════════════════════════════

COPY t FROM 's3://...' IAM_ROLE 'arn' MAXERROR 0;     -- Fail on ANY error (strictest)
COPY t FROM 's3://...' IAM_ROLE 'arn' MAXERROR 100;   -- Tolerate up to 100 bad rows
COPY t FROM 's3://...' IAM_ROLE 'arn' MAXERROR 100000; -- Very lenient (data exploration)

-- ═══════════════════════════════════════════════════════════
-- PERFORMANCE OPTIONS
-- ═══════════════════════════════════════════════════════════

COPY t FROM 's3://...' IAM_ROLE 'arn' COMPUPDATE ON;   -- Analyze + apply encoding (slow, first load only)
COPY t FROM 's3://...' IAM_ROLE 'arn' COMPUPDATE OFF;  -- Skip encoding analysis (fast, subsequent loads)
COPY t FROM 's3://...' IAM_ROLE 'arn' STATUPDATE ON;   -- Update statistics after load (recommended)
COPY t FROM 's3://...' IAM_ROLE 'arn' STATUPDATE OFF;  -- Skip statistics update (rare)

-- ═══════════════════════════════════════════════════════════
-- COLUMN MAPPING (when CSV columns ≠ table columns)
-- ═══════════════════════════════════════════════════════════

-- Load only specific columns (others get DEFAULT or NULL)
COPY orders (order_id, customer_id, total_amount)
FROM 's3://...' IAM_ROLE 'arn' CSV IGNOREHEADER 1;

-- ═══════════════════════════════════════════════════════════
-- MANIFEST vs PREFIX
-- ═══════════════════════════════════════════════════════════

-- PREFIX: loads ALL files matching the prefix
COPY t FROM 's3://bucket/data/orders/' IAM_ROLE 'arn' CSV;

-- MANIFEST: loads ONLY files listed in manifest JSON
COPY t FROM 's3://bucket/manifests/load.json' IAM_ROLE 'arn' MANIFEST CSV;

-- ═══════════════════════════════════════════════════════════
-- PRODUCTION TEMPLATE (copy-paste and customize)
-- ═══════════════════════════════════════════════════════════

COPY public.orders
FROM 's3://quickcart-raw-data-prod/manifests/orders_manifest.json'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
MANIFEST
CSV
IGNOREHEADER 1
GZIP
REGION 'us-east-2'
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
MAXERROR 0
COMPUPDATE OFF
STATUPDATE ON
BLANKSASNULL
EMPTYASNULL
TRIMBLANKS
ACCEPTINVCHARS;