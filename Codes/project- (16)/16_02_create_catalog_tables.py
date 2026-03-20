# file: 16_02_create_catalog_tables.py
# Purpose: Create Glue Data Catalog database and tables using Athena DDL
# These tables point to S3 CSVs uploaded in Project 1

import boto3
import time

REGION = "us-east-2"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"
SOURCE_BUCKET = "quickcart-raw-data-prod"
DATABASE_NAME = "quickcart_raw"


def run_athena_query(athena_client, query, database=None):
    """
    Execute a query in Athena and wait for completion.
    
    HOW THIS WORKS INTERNALLY:
    1. StartQueryExecution → Athena accepts query, returns QueryExecutionId
    2. Query enters internal queue → assigned to Trino coordinator
    3. We poll GetQueryExecution until state != RUNNING
    4. States: QUEUED → RUNNING → SUCCEEDED / FAILED / CANCELLED
    5. Results written to S3 output location
    """
    params = {
        "QueryString": query,
        "ResultConfiguration": {
            "OutputLocation": ATHENA_OUTPUT
        }
    }

    if database:
        params["QueryExecutionContext"] = {"Database": database}

    # --- SUBMIT QUERY ---
    response = athena_client.start_query_execution(**params)
    query_id = response["QueryExecutionId"]
    print(f"   Query ID: {query_id}")

    # --- POLL FOR COMPLETION ---
    while True:
        status = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )
        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            # Extract data scanned
            stats = status["QueryExecution"].get("Statistics", {})
            scanned_bytes = stats.get("DataScannedInBytes", 0)
            exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
            print(f"   ✅ SUCCEEDED | Scanned: {scanned_bytes:,} bytes | "
                  f"Time: {exec_time_ms}ms")
            return query_id

        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            print(f"   ❌ {state}: {reason}")
            return None

        else:
            # QUEUED or RUNNING — wait and retry
            time.sleep(1)


def get_query_results(athena_client, query_id, max_rows=10):
    """
    Fetch results of a completed Athena query.
    
    INTERNAL NOTE:
    → Results are paginated (max 1000 rows per call)
    → First row is ALWAYS the column headers
    → Use NextToken for pagination on large results
    """
    response = athena_client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=max_rows + 1  # +1 because first row = headers
    )

    rows = response["ResultSet"]["Rows"]
    if not rows:
        return [], []

    # First row = column headers
    headers = [col["VarCharValue"] for col in rows[0]["Data"]]

    # Remaining rows = data
    data = []
    for row in rows[1:]:
        values = []
        for col in row["Data"]:
            values.append(col.get("VarCharValue", "NULL"))
        data.append(values)

    return headers, data


def print_results(headers, data):
    """Pretty-print query results as a table"""
    if not headers:
        print("   (no results)")
        return

    # Calculate column widths
    widths = [len(h) for h in headers]
    for row in data:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    # Print header
    header_line = " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    print(f"   {header_line}")
    print(f"   {'-+-'.join('-' * w for w in widths)}")

    # Print data rows
    for row in data:
        row_line = " | ".join(
            str(val).ljust(widths[i]) for i, val in enumerate(row)
        )
        print(f"   {row_line}")


def setup_database_and_tables():
    athena_client = boto3.client("athena", region_name=REGION)

    # ================================================================
    # STEP 1: CREATE DATABASE
    # ================================================================
    # This creates a Glue Data Catalog DATABASE (logical namespace)
    # Visible in both Athena AND Glue console
    print("\n" + "=" * 70)
    print("STEP 1: Creating Glue Catalog Database")
    print("=" * 70)

    run_athena_query(
        athena_client,
        f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} "
        f"COMMENT 'QuickCart raw CSV data from daily exports' "
        f"LOCATION 's3://{SOURCE_BUCKET}/daily_exports/'"
    )
    print(f"   ✅ Database '{DATABASE_NAME}' ready")

    # ================================================================
    # STEP 2: CREATE ORDERS TABLE
    # ================================================================
    # Points to: s3://quickcart-raw-data-prod/daily_exports/orders/
    # Contains ALL orders_YYYYMMDD.csv files from Project 1
    print("\n" + "=" * 70)
    print("STEP 2: Creating 'orders' table")
    print("=" * 70)

    orders_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.orders (
        order_id        STRING,
        customer_id     STRING,
        product_id      STRING,
        quantity         INT,
        unit_price      DOUBLE,
        total_amount    DOUBLE,
        status          STRING,
        order_date      STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar'     = '"',
        'escapeChar'    = '\\\\'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{SOURCE_BUCKET}/daily_exports/orders/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1',
        'classification'         = 'csv',
        'has_encrypted_data'     = 'false'
    )
    """

    # ─── KEY DECISIONS EXPLAINED ───
    #
    # EXTERNAL TABLE:
    #   → Athena NEVER owns the data
    #   → DROP TABLE deletes metadata ONLY — S3 files remain untouched
    #   → vs MANAGED TABLE: DROP deletes S3 files too (dangerous!)
    #
    # OpenCSVSerDe:
    #   → Handles quoted fields (values containing commas)
    #   → IMPORTANT: all columns read as STRING internally
    #   → INT/DOUBLE in DDL = Athena casts STRING→type at query time
    #   → If cast fails → NULL (not error)
    #
    # skip.header.line.count:
    #   → Tells Athena to skip first line of each CSV file
    #   → Without this: "order_id" becomes a data row in every file
    #   → 180 files = 180 phantom header rows
    #
    # LOCATION ends with '/':
    #   → Athena scans ALL files under this prefix recursively
    #   → New files added to this prefix = instantly queryable
    #   → No need to re-register or refresh

    run_athena_query(athena_client, orders_ddl)
    print("   ✅ Table 'orders' created")

    # ================================================================
    # STEP 3: CREATE CUSTOMERS TABLE
    # ================================================================
    print("\n" + "=" * 70)
    print("STEP 3: Creating 'customers' table")
    print("=" * 70)

    customers_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.customers (
        customer_id     STRING,
        name            STRING,
        email           STRING,
        city            STRING,
        state           STRING,
        tier            STRING,
        signup_date     STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar'     = '"',
        'escapeChar'    = '\\\\'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{SOURCE_BUCKET}/daily_exports/customers/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1',
        'classification'         = 'csv'
    )
    """

    run_athena_query(athena_client, customers_ddl)
    print("   ✅ Table 'customers' created")

    # ================================================================
    # STEP 4: CREATE PRODUCTS TABLE
    # ================================================================
    print("\n" + "=" * 70)
    print("STEP 4: Creating 'products' table")
    print("=" * 70)

    products_ddl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.products (
        product_id       STRING,
        name             STRING,
        category         STRING,
        price            DOUBLE,
        stock_quantity   INT,
        is_active        STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
        'separatorChar' = ',',
        'quoteChar'     = '"',
        'escapeChar'    = '\\\\'
    )
    STORED AS TEXTFILE
    LOCATION 's3://{SOURCE_BUCKET}/daily_exports/products/'
    TBLPROPERTIES (
        'skip.header.line.count' = '1',
        'classification'         = 'csv'
    )
    """

    run_athena_query(athena_client, products_ddl)
    print("   ✅ Table 'products' created")

    return athena_client


if __name__ == "__main__":
    setup_database_and_tables()
    print("\n🎯 All tables created. Run 16_03_run_queries.py next.")