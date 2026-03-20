# file: 72_08_consumer_realtime_query.py
# Account C (Analytics): Query real-time clickstream from Account A's Gold layer
# Uses cross-account catalog reference from Project 71

import boto3
import time
from datetime import datetime

REGION = "us-east-2"
CONSUMER_ACCOUNT_ID =[REDACTED:BANK_ACCOUNT_NUMBER]33"
ATHENA_WORKGROUP = "mesh-analytics"
OUTPUT_BUCKET = "analytics-data-prod"


def query_realtime_clickstream(athena_client):
    """
    Analytics team queries real-time clickstream data
    from Orders domain (Account A) via cross-account catalog reference
    
    This is the PAYOFF of the entire architecture:
    → Kinesis → Firehose → S3 → Glue → Gold (Account A)
    → Cross-account catalog reference (Project 71)
    → Athena query from Account C
    → Data is < 15 minutes old
    → Zero data copies
    """
    now = datetime.utcnow()

    queries = {
        "Real-Time Traffic Summary": f"""
            -- What's happening RIGHT NOW on the website?
            -- Data freshness: < 15 minutes
            SELECT 
                event_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(DISTINCT session_id) as unique_sessions
            FROM orders_domain.clickstream_events
            WHERE year = '{now.strftime("%Y")}'
              AND month = '{now.strftime("%m")}'
              AND day = '{now.strftime("%d")}'
              AND hour = '{now.strftime("%H")}'
            GROUP BY event_type
            ORDER BY event_count DESC
        """,

        "Conversion Funnel (Last Hour)": f"""
            -- How many users progress through each funnel stage?
            SELECT 
                event_type,
                COUNT(DISTINCT user_id) as users,
                ROUND(
                    COUNT(DISTINCT user_id) * 100.0 / 
                    MAX(COUNT(DISTINCT user_id)) OVER (),
                    1
                ) as pct_of_max
            FROM orders_domain.clickstream_events
            WHERE year = '{now.strftime("%Y")}'
              AND month = '{now.strftime("%m")}'
              AND day = '{now.strftime("%d")}'
              AND hour = '{now.strftime("%H")}'
              AND event_type IN (
                  'page_view', 'product_click', 'add_to_cart',
                  'checkout_start', 'checkout_complete'
              )
            GROUP BY event_type
            ORDER BY 
                CASE event_type
                    WHEN 'page_view' THEN 1
                    WHEN 'product_click' THEN 2
                    WHEN 'add_to_cart' THEN 3
                    WHEN 'checkout_start' THEN 4
                    WHEN 'checkout_complete' THEN 5
                END
        """,

        "Top Products Being Viewed": f"""
            -- Which products are trending RIGHT NOW?
            SELECT 
                product_id,
                COUNT(*) as click_count,
                COUNT(DISTINCT user_id) as unique_viewers,
                COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) as add_to_cart_count
            FROM orders_domain.clickstream_events
            WHERE year = '{now.strftime("%Y")}'
              AND month = '{now.strftime("%m")}'
              AND day = '{now.strftime("%d")}'
              AND product_id IS NOT NULL
            GROUP BY product_id
            ORDER BY click_count DESC
            LIMIT 20
        """,

        "Device & Geography Breakdown": f"""
            -- Where are users coming from and what devices?
            SELECT 
                device_type,
                ip_country,
                COUNT(*) as events,
                COUNT(DISTINCT user_id) as users,
                COUNT(CASE WHEN event_type = 'checkout_complete' THEN 1 END) as purchases
            FROM orders_domain.clickstream_events
            WHERE year = '{now.strftime("%Y")}'
              AND month = '{now.strftime("%m")}'
              AND day = '{now.strftime("%d")}'
            GROUP BY device_type, ip_country
            ORDER BY events DESC
            LIMIT 30
        """,

        "Cross-Domain Join: Clickstream + Customer Segments": f"""
            -- JOIN real-time clicks with customer segments (from Customer domain)
            -- This is the Data Mesh magic: two domains, zero copies
            SELECT 
                cs.tier as customer_tier,
                ce.event_type,
                COUNT(*) as events,
                COUNT(DISTINCT ce.user_id) as users,
                ROUND(AVG(ce.cart_value), 2) as avg_cart_value
            FROM orders_domain.clickstream_events ce
            LEFT JOIN customers_domain.customer_segments cs
                ON ce.user_id = cs.customer_id
            WHERE ce.year = '{now.strftime("%Y")}'
              AND ce.month = '{now.strftime("%m")}'
              AND ce.day = '{now.strftime("%d")}'
              AND ce.event_type IN ('add_to_cart', 'checkout_complete')
            GROUP BY cs.tier, ce.event_type
            ORDER BY cs.tier, ce.event_type
        """
    }

    print("=" * 70)
    print("📊 ANALYTICS CONSUMER — REAL-TIME CLICKSTREAM QUERIES")
    print(f"   Account: {CONSUMER_ACCOUNT_ID} (Analytics Domain)")
    print(f"   Source: orders_domain.clickstream_events (Account A)")
    print(f"   Time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 70)

    for query_name, query_sql in queries.items():
        print(f"\n{'─' * 60}")
        print(f"📋 {query_name}")
        print(f"{'─' * 60}")

        try:
            response = athena_client.start_query_execution(
                QueryString=query_sql,
                QueryExecutionContext={"Database": "orders_domain"},
                WorkGroup=ATHENA_WORKGROUP,
                ResultConfiguration={
                    "OutputLocation": f"s3://{OUTPUT_BUCKET}/athena-results/realtime-clickstream/"
                }
            )
            query_id = response["QueryExecutionId"]
            print(f"   Query ID: {query_id}")

            # Wait for results
            status = wait_for_query(athena_client, query_id)
            if status == "SUCCEEDED":
                print_results(athena_client, query_id)
            else:
                print(f"   ❌ Query {status}")

        except Exception as e:
            print(f"   ❌ Failed: {e}")


def wait_for_query(athena_client, query_id, timeout=120):
    """Poll Athena for query completion"""
    start = time.time()
    while time.time() - start < timeout:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = response["QueryExecution"]["Status"]["State"]

        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            elapsed = round(time.time() - start, 1)

            if status == "SUCCEEDED":
                stats = response["QueryExecution"]["Statistics"]
                scanned_mb = round(
                    stats.get("DataScannedInBytes", 0) / (1024 * 1024), 2
                )
                runtime_ms = stats.get("EngineExecutionTimeInMillis", 0)
                cost = round(scanned_mb / (1024 * 1024) * 5, 6)
                print(f"   ✅ {elapsed}s | Scanned: {scanned_mb} MB | "
                      f"Runtime: {runtime_ms}ms | Cost: ${cost}")

            if status == "FAILED":
                reason = response["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                print(f"   ❌ Failed: {reason}")

            return status
        time.sleep(2)

    return "TIMEOUT"


def print_results(athena_client, query_id, max_rows=15):
    """Print query results in tabular format"""
    response = athena_client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=max_rows + 1  # +1 for header
    )

    rows = response["ResultSet"]["Rows"]
    if len(rows) <= 1:
        print("   (no data — pipeline may not have processed yet)")
        return

    # Extract headers
    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows[1:]:
        for i, col in enumerate(row["Data"]):
            val = col.get("VarCharValue", "NULL")
            col_widths[i] = max(col_widths[i], len(val))

    # Print header
    header_line = "   " + " | ".join(
        h.ljust(col_widths[i]) for i, h in enumerate(headers)
    )
    print(header_line)
    print("   " + "-" * len(header_line.strip()))

    # Print data rows
    for row in rows[1:]:
        values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
        line = "   " + " | ".join(
            v.ljust(col_widths[i]) for i, v in enumerate(values)
        )
        print(line)


def main():
    athena_client = boto3.client("athena", region_name=REGION)
    query_realtime_clickstream(athena_client)

    print("\n" + "=" * 70)
    print("✅ REAL-TIME QUERIES COMPLETE")
    print("   All data read cross-account — zero copies, always fresh")
    print("=" * 70)


if __name__ == "__main__":
    main()