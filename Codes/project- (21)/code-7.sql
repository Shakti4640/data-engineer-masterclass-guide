-- file: 07_simplest_copy.sql
-- The absolute minimum COPY command
-- Good for understanding; add options for production

-- Simplest possible (uncompressed CSV with headers)
COPY public.orders
FROM 's3://quickcart-raw-data-prod/daily_exports/orders/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3ReadRole'
CSV
IGNOREHEADER 1;

-- Check what loaded
SELECT COUNT(*) FROM public.orders;

-- Check for any errors
SELECT * FROM stl_load_errors ORDER BY starttime DESC LIMIT 10;