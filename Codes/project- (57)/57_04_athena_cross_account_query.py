# file: 57_04_athena_cross_account_query.py
# Purpose: Demonstrate Athena querying Account A's Catalog from Account B

import boto3
import json
import time

REGION = "us-east-2"
ATHENA_OUTPUT = "s3://quickcart-analytics-results/athena-output/"


def load_config():
    with open("/tmp/catalog_sharing_config.json", "r") as f:
        return json.load(f)


def run_cross_account_athena_query():
    """
    Demonstrate Athena querying Account A's Catalog tables from Account B
    
    KEY SYNTAX:
    → "CATALOG_ID"."DATABASE"."TABLE"
    → "111111111111"."quickcart_db"."orders_gold"
    
    This works because:
    → Account A's Catalog Resource Policy allows Account B's Athena role
    → Account B's Athena role has glue:GetTable permission on Account A
    → Account B's Athena role has s3:GetObject on Account A's S3 (Project 56)
    """
    config = load_config()
    athena_client = boto3.client("athena", region_name=REGION)
    account_a_id = config["account_a_id"]

    print("=" * 70)
    print("🔍 ATHENA CROSS-ACCOUNT CATALOG QUERIES")
    print(f"   Querying Account A's Catalog ({account_a_id}) from Account B")
    print("=" * 70)

    queries = [
        {
            "name": "List tables in cross-account database",
            "sql": f'SHOW TABLES IN "{account_a_id}"."quickcart_db"',
            "description": "See all tables Account B can access"
        },
        {
            "name": "Describe table schema",
            "sql": f'DESCRIBE "{account_a_id}"."quickcart_db"."orders_gold"',
            "description": "Get column names and types from Account A's table"
        },
        {
            "name": "Query cross-account table",
            "sql": (
                f'SELECT order_date, COUNT(*) as order_count, '
                f'SUM(total_amount) as total_revenue '
                f'FROM "{account_a_id}"."quickcart_db"."orders_gold" '
                f'GROUP BY order_date '
                f'ORDER BY order_date DESC '
                f'LIMIT 10'
            ),
            "description": "Aggregate query on Account A's data"
        },
        {
            "name": "Join cross-account tables",
            "sql": (
                f'SELECT o.order_id, c.name, o.total_amount '
                f'FROM "{account_a_id}"."quickcart_db"."orders_gold" o '
                f'JOIN "{account_a_id}"."quickcart_db"."customers_silver" c '
                f'ON o.customer_id = c.customer_id '
                f'LIMIT 5'
            ),
            "description": "Join two tables from Account A's Catalog"
        }
    ]

    for i, query in enumerate(queries, 1):
        print(f"\n{'─' * 50}")
        print(f"📌 Query {i}: {query['name']}")
        print(f"   {query['description']}")
        print(f"   SQL: {query['sql'][:100]}...")

        try:
            response = athena_client.start_query_execution(
                QueryString=query["sql"],
                ResultConfiguration={
                    "OutputLocation": ATHENA_OUTPUT
                },
                WorkGroup="primary"
            )
            query_id = response["QueryExecutionId"]
            print(f"   Query ID: {query_id}")

            # Wait for completion
            status = wait_for_query(athena_client, query_id)

            if status == "SUCCEEDED":
                # Get results
                results = athena_client.get_query_results(
                    QueryExecutionId=query_id,
                    MaxResults=10
                )
                rows = results["ResultSet"]["Rows"]
                if rows:
                    # Header
                    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
                    print(f"   ✅ SUCCEEDED — {len(rows)-1} rows returned")
                    print(f"   Headers: {headers}")
                    for row in rows[1:4]:  # Show first 3 data rows
                        values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
                        print(f"   → {values}")
                    if len(rows) > 4:
                        print(f"   ... and {len(rows)-4} more rows")
            else:
                print(f"   ❌ Query {status}")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print(f"\n{'=' * 70}")
    print(f"📊 ATHENA CROSS-ACCOUNT QUERY SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Syntax: \"CATALOG_ID\".\"DATABASE\".\"TABLE\"")
    print(f"  Example: \"{account_a_id}\".\"quickcart_db\".\"orders_gold\"")
    print(f"  Requires: Catalog Resource Policy + S3 cross-account access")
    print(f"{'=' * 70}")


def wait_for_query(athena_client, query_id, max_wait=120):
    """Wait for Athena query to complete"""
    start = time.time()
    while (time.time() - start) < max_wait:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )
        status = response["QueryExecution"]["Status"]["State"]

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            if status == "FAILED":
                reason = response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                print(f"   Failure reason: {reason[:200]}")
            return status

        time.sleep(2)

    return "TIMEOUT"


if __name__ == "__main__":
    run_cross_account_athena_query()