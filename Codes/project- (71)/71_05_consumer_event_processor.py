# file: 71_05_consumer_event_processor.py
# Runs in Account C (333333333333): Analytics Domain
# Polls SQS for data product events → triggers cross-account queries

import boto3
import json
import time
from datetime import datetime

# --- CONFIGURATION ---
CONSUMER_ACCOUNT_ID = [REDACTED:BANK_ACCOUNT_NUMBER]3"
REGION = "us-east-2"
QUEUE_NAME = "mesh-analytics-data-events"
ATHENA_WORKGROUP = "mesh-analytics"
OUTPUT_BUCKET = "analytics-data-prod"


def get_queue_url(sqs_client):
    """Get the SQS queue URL by name"""
    response = sqs_client.get_queue_url(QueueName=QUEUE_NAME)
    return response["QueueUrl"]


def poll_for_events(sqs_client, queue_url, max_messages=5, wait_seconds=20):
    """
    Long-poll SQS for data product events (Project 27 pattern)
    
    Long polling (wait_seconds=20):
    → Reduces empty responses
    → Reduces API calls (and cost)
    → Returns immediately when messages arrive
    """
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=max_messages,
        WaitTimeSeconds=wait_seconds,
        MessageAttributeNames=["All"],
        AttributeNames=["All"]
    )
    return response.get("Messages", [])


def process_data_product_event(event_body):
    """
    Parse and validate the data product event
    
    Returns dict with:
    → domain, product_name, s3_location, partition, quality
    """
    event = json.loads(event_body)

    print(f"\n📨 Event received:")
    print(f"   Type:    {event['event_type']}")
    print(f"   Domain:  {event['source_domain']}")
    print(f"   Product: {event['data_product']['name']}")
    print(f"   Version: {event['data_product']['version']}")
    print(f"   Quality: {event['quality']['score']}% "
          f"({'✅' if event['quality']['passed'] else '❌'})")
    print(f"   Rows:    {event['data_product']['row_count']:,}")

    # Only process if quality passed
    if not event["quality"]["passed"]:
        print(f"   ⚠️  SKIPPING: quality below threshold")
        return None

    return event


def run_cross_account_athena_query(athena_client, event):
    """
    Run Athena query against producer's data via external catalog reference
    
    HOW THIS WORKS:
    1. Athena uses local database "orders_domain" (created in 71_02)
    2. "orders_domain" has TargetDatabase pointing to Account A's Glue Catalog
    3. Athena resolves schema from Account A's catalog (cross-account)
    4. Athena reads Parquet from Account A's S3 (cross-account)
    5. Results written to Account C's own S3 bucket
    
    The consumer never copies the data — reads in place
    """
    domain = event["source_domain"]
    product = event["data_product"]["name"]
    partition = event["data_product"]["partition"]

    # Build query that reads from producer's Gold layer
    query = f"""
    -- Cross-account query: Analytics → {domain} domain
    -- Reads from {domain}_domain (external catalog reference)
    SELECT 
        status,
        COUNT(*) as order_count,
        SUM(order_total) as total_revenue,
        AVG(order_total) as avg_order_value,
        AVG(item_count) as avg_items_per_order
    FROM {domain}_domain.{product}
    WHERE year = '{partition["year"]}'
      AND month = '{partition["month"]}'
      AND day = '{partition["day"]}'
    GROUP BY status
    ORDER BY total_revenue DESC
    """

    print(f"\n🔍 Running cross-account Athena query:")
    print(f"   Source: {domain}_domain.{product}")
    print(f"   Partition: {partition}")

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                "Database": f"{domain}_domain"  # Local external reference
            },
            WorkGroup=ATHENA_WORKGROUP,
            ResultConfiguration={
                "OutputLocation": f"s3://{OUTPUT_BUCKET}/athena-results/mesh-queries/"
            }
        )
        query_id = response["QueryExecutionId"]
        print(f"   Query ID: {query_id}")

        # Wait for completion
        status = wait_for_query(athena_client, query_id)
        if status == "SUCCEEDED":
            print_query_results(athena_client, query_id)
        else:
            print(f"   ❌ Query {status}")

        return query_id

    except Exception as e:
        print(f"   ❌ Query failed: {e}")
        return None


def wait_for_query(athena_client, query_id, timeout=120):
    """Poll Athena for query completion"""
    start = time.time()
    while time.time() - start < timeout:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_id
        )
        status = response["QueryExecution"]["Status"]["State"]

        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            elapsed = round(time.time() - start, 1)
            print(f"   Status: {status} (took {elapsed}s)")

            if status == "SUCCEEDED":
                stats = response["QueryExecution"]["Statistics"]
                scanned_mb = round(
                    stats.get("DataScannedInBytes", 0) / (1024 * 1024), 2
                )
                runtime_ms = stats.get("EngineExecutionTimeInMillis", 0)
                print(f"   Data scanned: {scanned_mb} MB")
                print(f"   Runtime: {runtime_ms} ms")
                # Cost: $5 per TB scanned
                cost = round(scanned_mb / (1024 * 1024) * 5, 6)
                print(f"   Est. cost: ${cost}")

            return status

        time.sleep(2)

    print(f"   ⚠️  Query timed out after {timeout}s")
    return "TIMEOUT"


def print_query_results(athena_client, query_id):
    """Print query results from Athena"""
    response = athena_client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=20
    )

    rows = response["ResultSet"]["Rows"]
    if not rows:
        print("   (no results)")
        return

    # Header
    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    print(f"\n   {'  |  '.join(headers)}")
    print(f"   {'-' * 70}")

    # Data rows
    for row in rows[1:]:
        values = [col.get("VarCharValue", "NULL") for col in row["Data"]]
        print(f"   {'  |  '.join(values)}")


def create_derived_dataset(athena_client, event):
    """
    Analytics consumer creates DERIVED dataset by joining 
    multiple producer domains
    
    THIS IS THE POWER OF DATA MESH:
    → Orders domain publishes daily_orders
    → Customer domain publishes customer_segments
    → Analytics joins them WITHOUT copying either dataset
    → Result written to Analytics' own S3 bucket
    """
    partition = event["data_product"]["partition"]

    # CTAS: Create derived table from cross-domain join (Project 36 pattern)
    query = f"""
    CREATE TABLE analytics_derived.orders_with_segments
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = 's3://{OUTPUT_BUCKET}/derived/orders_with_segments/year={partition["year"]}/month={partition["month"]}/day={partition["day"]}/',
        partitioned_by = ARRAY[]
    ) AS
    SELECT 
        o.order_id,
        o.customer_id,
        o.order_total,
        o.status,
        o.order_date,
        c.tier as customer_tier,
        c.city as customer_city
    FROM orders_domain.daily_orders o
    LEFT JOIN customers_domain.customer_segments c
        ON o.customer_id = c.customer_id
    WHERE o.year = '{partition["year"]}'
      AND o.month = '{partition["month"]}'
      AND o.day = '{partition["day"]}'
    """

    print(f"\n🔗 Creating derived dataset: orders_with_segments")
    print(f"   Joining: orders_domain × customers_domain")
    print(f"   Output: s3://{OUTPUT_BUCKET}/derived/orders_with_segments/")

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": "analytics_derived"},
            WorkGroup=ATHENA_WORKGROUP
        )
        query_id = response["QueryExecutionId"]
        status = wait_for_query(athena_client, query_id, timeout=300)
        if status == "SUCCEEDED":
            print(f"   ✅ Derived dataset created successfully")
        return query_id
    except Exception as e:
        print(f"   ❌ Derived dataset creation failed: {e}")
        return None


def run_consumer_loop(max_iterations=3):
    """
    Main consumer loop: poll → process → query → derive
    
    In production this runs as:
    → ECS Fargate task (long-running)
    → OR Lambda triggered by SQS (event-driven)
    → OR Step Functions with SQS integration
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    athena_client = boto3.client("athena", region_name=REGION)

    queue_url = get_queue_url(sqs_client)
    print(f"🔄 Consumer loop started")
    print(f"   Queue: {QUEUE_NAME}")
    print(f"   Workgroup: {ATHENA_WORKGROUP}")

    iteration = 0
    while iteration < max_iterations:
        iteration += 1
        print(f"\n{'=' * 60}")
        print(f"📡 Polling iteration {iteration}/{max_iterations}...")

        messages = poll_for_events(sqs_client, queue_url)

        if not messages:
            print("   No messages — waiting...")
            continue

        print(f"   Received {len(messages)} message(s)")

        for msg in messages:
            receipt_handle = msg["ReceiptHandle"]

            try:
                # Parse event
                event = process_data_product_event(msg["Body"])

                if event is None:
                    # Quality failed — acknowledge but skip processing
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    continue

                # Run cross-account query
                run_cross_account_athena_query(athena_client, event)

                # Create derived dataset (joins across domains)
                create_derived_dataset(athena_client, event)

                # Acknowledge message — remove from queue
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
                print(f"   ✅ Message processed and acknowledged")

            except Exception as e:
                print(f"   ❌ Processing failed: {e}")
                # Message returns to queue after visibility timeout
                # After 3 failures → goes to DLQ (Project 50)

    print(f"\n{'=' * 60}")
    print(f"🏁 Consumer loop completed ({max_iterations} iterations)")


if __name__ == "__main__":
    run_consumer_loop(max_iterations=3)