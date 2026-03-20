# file: 72_03_event_producer.py
# Simulates website/app sending clickstream events to Kinesis
# In production: this is your web server SDK or API Gateway → Kinesis

import boto3
import json
import uuid
import random
import time
from datetime import datetime, timezone

REGION = "us-east-2"
STREAM_NAME = "clickstream-events"

# Realistic event configuration
EVENT_TYPES = [
    ("page_view", 0.45),        # 45% of events
    ("product_click", 0.25),    # 25%
    ("search", 0.12),           # 12%
    ("add_to_cart", 0.10),      # 10%
    ("checkout_start", 0.05),   # 5%
    ("checkout_complete", 0.03) # 3%
]

PAGES = [
    "/", "/products", "/products/electronics", "/products/clothing",
    "/products/home-kitchen", "/cart", "/checkout", "/account",
    "/search", "/deals", "/products/electronics/laptop-xyz",
    "/products/clothing/winter-jacket", "/products/home-kitchen/blender"
]

DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge", "Samsung Internet"]
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "CA", "AU", "BR", "IN", "MX"]
REFERRERS = ["google.com", "facebook.com", "instagram.com", "direct", "email", "tiktok.com"]


def generate_event():
    """
    Generate a single realistic clickstream event
    
    PARTITION KEY STRATEGY:
    → Use user_id as partition key
    → Same user's events go to same shard → preserves ORDER per user
    → Different users distributed across shards → parallelism
    → This is critical for session analysis (events in order per user)
    """
    # Weighted random event type selection
    rand = random.random()
    cumulative = 0
    event_type = "page_view"
    for etype, weight in EVENT_TYPES:
        cumulative += weight
        if rand <= cumulative:
            event_type = etype
            break

    user_id = f"CUST-{random.randint(1, 50000):06d}"
    session_id = f"sess-{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "event_timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "page_url": random.choice(PAGES),
        "product_id": (
            f"PROD-{random.randint(1, 5000):05d}"
            if event_type in ("product_click", "add_to_cart", "checkout_complete")
            else None
        ),
        "search_query": (
            random.choice(["laptop", "winter jacket", "blender", "headphones", "running shoes"])
            if event_type == "search"
            else None
        ),
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "ip_country": random.choice(COUNTRIES),
        "referrer": random.choice(REFERRERS),
        "cart_value": (
            round(random.uniform(19.99, 999.99), 2)
            if event_type in ("add_to_cart", "checkout_start", "checkout_complete")
            else None
        ),
        "items_in_cart": (
            random.randint(1, 8)
            if event_type in ("add_to_cart", "checkout_start", "checkout_complete")
            else None
        )
    }

    # Remove None values to keep JSON clean
    event = {k: v for k, v in event.items() if v is not None}

    return event, user_id  # user_id is the partition key


def send_single_event(kinesis_client, event, partition_key):
    """
    Send single event to Kinesis using PutRecord
    
    WHEN TO USE PutRecord vs PutRecords:
    → PutRecord: single record, simpler, lower throughput
    → PutRecords: batch up to 500 records, higher throughput
    → For production at 800+ events/sec: ALWAYS use PutRecords
    """
    response = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=partition_key
    )
    return response["ShardId"], response["SequenceNumber"]


def send_batch_events(kinesis_client, events_with_keys, max_batch=500):
    """
    Send batch of events using PutRecords (high throughput)
    
    CRITICAL: PutRecords can PARTIALLY fail
    → Response includes FailedRecordCount
    → Must check and RETRY failed records
    → This is the #1 missed bug in Kinesis producers
    """
    records = []
    for event, partition_key in events_with_keys:
        records.append({
            "Data": json.dumps(event).encode("utf-8"),
            "PartitionKey": partition_key
        })

    total_sent = 0
    total_failed = 0

    # Process in batches of max_batch (Kinesis limit: 500 per PutRecords)
    for i in range(0, len(records), max_batch):
        batch = records[i:i + max_batch]
        retry_count = 0
        max_retries = 3

        while batch and retry_count < max_retries:
            response = kinesis_client.put_records(
                StreamName=STREAM_NAME,
                Records=batch
            )

            failed_count = response["FailedRecordCount"]

            if failed_count == 0:
                total_sent += len(batch)
                break

            # Retry ONLY the failed records
            retry_batch = []
            for idx, record_response in enumerate(response["Records"]):
                if "ErrorCode" in record_response:
                    retry_batch.append(batch[idx])
                else:
                    total_sent += 1

            if retry_batch:
                retry_count += 1
                # Exponential backoff before retry
                backoff = min(2 ** retry_count * 0.1, 2.0)
                time.sleep(backoff)
                batch = retry_batch
            else:
                break

        if retry_count >= max_retries and batch:
            total_failed += len(batch)

    return total_sent, total_failed


def run_producer(events_per_second=50, duration_seconds=30):
    """
    Simulate clickstream traffic at specified rate
    
    DEFAULT: 50 events/sec for 30 seconds = 1,500 events
    → Low rate for testing
    → Production would be 800-5000 events/sec
    """
    kinesis_client = boto3.client("kinesis", region_name=REGION)

    print("=" * 70)
    print(f"🌐 CLICKSTREAM PRODUCER — Sending to '{STREAM_NAME}'")
    print(f"   Rate: {events_per_second} events/sec")
    print(f"   Duration: {duration_seconds} seconds")
    print(f"   Expected total: {events_per_second * duration_seconds:,} events")
    print("=" * 70)

    total_sent = 0
    total_failed = 0
    event_type_counts = {}
    start_time = time.time()

    for second in range(duration_seconds):
        batch_start = time.time()

        # Generate batch for this second
        events_with_keys = []
        for _ in range(events_per_second):
            event, partition_key = generate_event()
            events_with_keys.append((event, partition_key))

            # Track event type distribution
            etype = event["event_type"]
            event_type_counts[etype] = event_type_counts.get(etype, 0) + 1

        # Send batch
        sent, failed = send_batch_events(kinesis_client, events_with_keys)
        total_sent += sent
        total_failed += failed

        # Progress update every 5 seconds
        if (second + 1) % 5 == 0:
            elapsed = time.time() - start_time
            rate = total_sent / elapsed if elapsed > 0 else 0
            print(f"   [{second+1:3d}s] Sent: {total_sent:,} | "
                  f"Failed: {total_failed} | "
                  f"Rate: {rate:.0f} events/sec")

        # Pace to maintain target rate
        batch_elapsed = time.time() - batch_start
        sleep_time = max(0, 1.0 - batch_elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    elapsed = time.time() - start_time

    # Summary
    print("\n" + "=" * 70)
    print("📊 PRODUCER SUMMARY")
    print(f"   Total sent:     {total_sent:,}")
    print(f"   Total failed:   {total_failed}")
    print(f"   Duration:       {elapsed:.1f}s")
    print(f"   Actual rate:    {total_sent/elapsed:.0f} events/sec")
    print(f"\n   Event distribution:")
    for etype, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
        pct = count / max(total_sent, 1) * 100
        print(f"     {etype:<22} {count:>6,} ({pct:.1f}%)")
    print("=" * 70)

    return total_sent, total_failed


if __name__ == "__main__":
    run_producer(events_per_second=50, duration_seconds=30)