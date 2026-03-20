# file: 02_table_config.py
# Configuration-driven mapping: S3 key → Redshift table → COPY options
# Adding a new table = adding one entry here (no code changes)

REDSHIFT_IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"

TABLE_CONFIGS = {
    "orders": {
        "redshift_schema": "public",
        "redshift_table": "orders",
        "copy_options": {
            "format": "CSV",
            "compression": "GZIP",
            "ignore_header": 1,
            "dateformat": "auto",
            "timeformat": "auto",
            "maxerror": 0,
            "compupdate": "OFF",
            "statupdate": "ON",
            "blanksasnull": True,
            "emptyasnull": True
        },
        "validation": {
            "min_rows": 1000,           # Alert if fewer than 1K rows
            "max_rows": 50000000,       # Alert if more than 50M rows
            "expected_columns": 8
        },
        "load_strategy": "TRUNCATE_COPY"  # or "STAGING_MERGE" (Project 65)
    },
    "customers": {
        "redshift_schema": "public",
        "redshift_table": "customers",
        "copy_options": {
            "format": "CSV",
            "compression": "GZIP",
            "ignore_header": 1,
            "dateformat": "auto",
            "maxerror": 0,
            "compupdate": "OFF",
            "statupdate": "ON"
        },
        "validation": {
            "min_rows": 100,
            "max_rows": 1000000,
            "expected_columns": 7
        },
        "load_strategy": "TRUNCATE_COPY"
    },
    "products": {
        "redshift_schema": "public",
        "redshift_table": "products",
        "copy_options": {
            "format": "CSV",
            "compression": "GZIP",
            "ignore_header": 1,
            "dateformat": "auto",
            "maxerror": 0,
            "compupdate": "OFF",
            "statupdate": "ON"
        },
        "validation": {
            "min_rows": 10,
            "max_rows": 100000,
            "expected_columns": 6
        },
        "load_strategy": "TRUNCATE_COPY"
    }
}


def get_table_config(s3_key):
    """
    Extract table name from S3 key and return configuration
    
    S3 key format: mariadb_exports/{table}/export_date=YYYY-MM-DD/{file}.csv.gz
    Extract: {table} portion
    """
    # Parse: mariadb_exports/orders/export_date=2025-01-30/orders.csv.gz
    parts = s3_key.split("/")

    if len(parts) < 3 or parts[0] != "mariadb_exports":
        return None, None

    table_name = parts[1]  # "orders", "customers", "products"

    if table_name not in TABLE_CONFIGS:
        return table_name, None

    return table_name, TABLE_CONFIGS[table_name]


def extract_export_date(s3_key):
    """Extract export_date from S3 key path"""
    # Key: mariadb_exports/orders/export_date=2025-01-30/orders.csv.gz
    for part in s3_key.split("/"):
        if part.startswith("export_date="):
            return part.split("=")[1]
    return None


def build_copy_sql(s3_path, config):
    """Build Redshift COPY SQL from configuration"""
    schema = config["redshift_schema"]
    table = config["redshift_table"]
    opts = config["copy_options"]

    copy_parts = [
        f"COPY {schema}.{table}",
        f"FROM '{s3_path}'",
        f"IAM_ROLE '{REDSHIFT_IAM_ROLE}'"
    ]

    if opts.get("format") == "CSV":
        copy_parts.append("CSV")
    if opts.get("compression") == "GZIP":
        copy_parts.append("GZIP")
    if opts.get("ignore_header"):
        copy_parts.append(f"IGNOREHEADER {opts['ignore_header']}")
    if opts.get("dateformat"):
        copy_parts.append(f"DATEFORMAT '{opts['dateformat']}'")
    if opts.get("timeformat"):
        copy_parts.append(f"TIMEFORMAT '{opts['timeformat']}'")
    if opts.get("maxerror") is not None:
        copy_parts.append(f"MAXERROR {opts['maxerror']}")
    if opts.get("compupdate"):
        copy_parts.append(f"COMPUPDATE {opts['compupdate']}")
    if opts.get("statupdate"):
        copy_parts.append(f"STATUPDATE {opts['statupdate']}")
    if opts.get("blanksasnull"):
        copy_parts.append("BLANKSASNULL")
    if opts.get("emptyasnull"):
        copy_parts.append("EMPTYASNULL")

    copy_parts.append(f"REGION '{REGION}'")

    return "\n".join(copy_parts) + ";"


REGION = "us-east-2"