# file: 31_05_validate_with_athena.py
# Purpose: Validate extracted data by querying Athena
# Connects to: Project 24 (Athena querying Glue Catalog)

import boto3
import time

AWS_REGION = "us-east-2"
S3_BUCKET = "quickcart-datalake-prod"
ATHENA_OUTPUT = f"s3://{S3_BUCKET}/athena-results/"

athena = boto3.client("athena", region_name=AWS_REGION)


def run_athena_query(query, database="bronze_mariadb"):
    """Execute Athena query and wait for results"""
    print(f"\n📊 Running: {query[:80]}...")

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )

    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state == "SUCCEEDED":
        results = athena.get_query_results(QueryExecutionId=query_id)
        rows = results["ResultSet"]["Rows"]

        # Print header
        header = [col["VarCharValue"] for col in rows[0]["Data"]]
        print(f"   {'  |  '.join(header)}")
        print(f"   {'-' * 50}")

        # Print data rows
        for row in rows[1:]:
            values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
            print(f"   {'  |  '.join(values)}")

        return rows
    else:
        error = status["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        print(f"   ❌ Query failed: {error}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("🔍 POST-EXTRACTION VALIDATION")
    print("=" * 60)

    # Validation 1: Row counts per table
    run_athena_query("""
        SELECT 'orders' as table_name, COUNT(*) as row_count 
        FROM orders
        UNION ALL
        SELECT 'customers', COUNT(*) FROM customers
        UNION ALL
        SELECT 'products', COUNT(*) FROM products
    """)

    # Validation 2: Check partitions exist
    run_athena_query("SHOW PARTITIONS orders")

    # Validation 3: Sample data
    run_athena_query("SELECT * FROM orders LIMIT 5")

    # Validation 4: Check metadata columns
    run_athena_query("""
        SELECT _source_table, _extraction_date, COUNT(*) as rows
        FROM orders
        GROUP BY _source_table, _extraction_date
    """)

    # Validation 5: Schema verification
    run_athena_query("DESCRIBE orders")

    print("\n" + "=" * 60)
    print("✅ VALIDATION COMPLETE")
    print("=" * 60)