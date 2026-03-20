# file: 17_04_parse_s3_event.py
# Purpose: Parse and understand the S3 event JSON structure
# This is REFERENCE CODE — used in Projects 18, 27, 28, 41

import json
from urllib.parse import unquote_plus

# ── SAMPLE S3 EVENT (what S3 sends to SNS) ──
# This is the EXACT structure you'll see in the SNS email body
# And the EXACT structure consumed by SQS (Project 18), Lambda, EventBridge

SAMPLE_S3_EVENT = """
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-2",
      "eventTime": "2025-01-15T00:03:12.345Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "AIDACKCEVSQ6C2EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "172.31.10.25"
      },
      "responseElements": {
        "x-amz-request-id": "C3D13FE58DE4C810",
        "x-amz-id-2": "FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "daily-csv-upload-alert",
        "bucket": {
          "name": "quickcart-raw-data-prod",
          "ownerIdentity": {
            "principalId": "A3NL1KOZZKExample"
          },
          "arn": "arn:aws:s3:::quickcart-raw-data-prod"
        },
        "object": {
          "key": "daily_exports/orders/orders_20250115.csv",
          "size": 83942,
          "eTag": "d41d8cd98f00b204e9800998ecf8427e",
          "sequencer": "0055AED6DCD90281E5"
        }
      }
    }
  ]
}
"""

# ── SAMPLE SNS ENVELOPE (what SNS wraps around the S3 event) ──
# When S3 event goes through SNS, SNS wraps it in this envelope
# SQS subscribers (Project 18) receive THIS, not the raw S3 event
# Must unwrap: SQS Message Body → SNS Envelope → S3 Event

SAMPLE_SNS_ENVELOPE = """
{
  "Type": "Notification",
  "MessageId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "TopicArn": "arn:aws:sns:us-east-2:123456789012:quickcart-s3-file-alerts",
  "Subject": "Amazon S3 Notification",
  "Message": "<THE S3 EVENT JSON AS A STRING — must json.loads() again>",
  "Timestamp": "2025-01-15T00:03:13.456Z",
  "SignatureVersion": "1",
  "Signature": "BASE64_ENCODED_SIGNATURE...",
  "SigningCertURL": "https://sns.us-east-2.amazonaws.com/...",
  "UnsubscribeURL": "https://sns.us-east-2.amazonaws.com/..."
}
"""


def parse_s3_event_from_raw(event_json_string):
    """
    Parse raw S3 event JSON (direct from S3 or from SNS email body)
    
    USED IN:
    → This project: understanding the structure
    → Project 18: SQS consumer parsing S3 events
    → Project 27: EC2 worker processing file arrivals
    → Project 28: end-to-end pipeline parsing
    → Project 41: EventBridge / Step Functions triggers
    """
    event = json.loads(event_json_string)

    print("=" * 70)
    print("PARSED S3 EVENT")
    print("=" * 70)

    for i, record in enumerate(event["Records"]):
        print(f"\n   Record #{i + 1}:")
        print(f"   ─────────────")

        # ── EVENT METADATA ──
        event_name = record["eventName"]
        event_time = record["eventTime"]
        aws_region = record["awsRegion"]

        print(f"   Event Type:    {event_name}")
        print(f"   Event Time:    {event_time}")
        print(f"   Region:        {aws_region}")

        # ── BUCKET INFO ──
        bucket_name = record["s3"]["bucket"]["name"]
        bucket_arn = record["s3"]["bucket"]["arn"]

        print(f"   Bucket:        {bucket_name}")
        print(f"   Bucket ARN:    {bucket_arn}")

        # ── OBJECT INFO ──
        # CRITICAL: key is URL-encoded — must decode
        raw_key = record["s3"]["object"]["key"]
        decoded_key = unquote_plus(raw_key)
        object_size = record["s3"]["object"]["size"]
        object_etag = record["s3"]["object"]["eTag"]
        sequencer = record["s3"]["object"]["sequencer"]

        print(f"   Object Key:    {decoded_key}")
        print(f"   Raw Key:       {raw_key}")
        if raw_key != decoded_key:
            print(f"   ⚠️  Key was URL-encoded — decoded for use")
        print(f"   Size:          {object_size:,} bytes ({object_size / 1024:.1f} KB)")
        print(f"   ETag:          {object_etag}")
        print(f"   Sequencer:     {sequencer}")

        # ── WHICH NOTIFICATION RULE FIRED ──
        config_id = record["s3"]["configurationId"]
        print(f"   Config Rule:   {config_id}")

        # ── EXTRACT ENTITY TYPE FROM KEY ──
        # Key pattern: daily_exports/{entity}/{filename}
        # Extract: "orders", "customers", "products"
        parts = decoded_key.split("/")
        if len(parts) >= 3:
            entity_type = parts[1]
            filename = parts[-1]
            print(f"   Entity Type:   {entity_type}")
            print(f"   Filename:      {filename}")

        # ── BUILD S3 URI ──
        s3_uri = f"s3://{bucket_name}/{decoded_key}"
        print(f"   Full S3 URI:   {s3_uri}")

        return {
            "bucket": bucket_name,
            "key": decoded_key,
            "size": object_size,
            "etag": object_etag,
            "event_name": event_name,
            "event_time": event_time,
            "s3_uri": s3_uri
        }


def parse_s3_event_from_sns_envelope(sns_message_string):
    """
    Parse S3 event that was WRAPPED in SNS envelope
    
    FLOW: S3 → SNS → SQS
    SQS message body = SNS envelope
    SNS envelope "Message" field = S3 event (as STRING, needs second parse)
    
    USED IN: Project 18 (SQS consumer), Project 28 (full pipeline)
    """
    print("=" * 70)
    print("PARSING SNS ENVELOPE → S3 EVENT (double unwrap)")
    print("=" * 70)

    # First parse: SNS envelope
    sns_envelope = json.loads(sns_message_string)

    print(f"   SNS Message ID: {sns_envelope['MessageId']}")
    print(f"   SNS Topic:      {sns_envelope['TopicArn']}")
    print(f"   SNS Timestamp:  {sns_envelope['Timestamp']}")
    print(f"   SNS Subject:    {sns_envelope.get('Subject', 'N/A')}")

    # Second parse: S3 event is a STRING inside the "Message" field
    s3_event_string = sns_envelope["Message"]
    print(f"\n   Unwrapping S3 event from SNS Message field...")

    # Now parse the actual S3 event
    return parse_s3_event_from_raw(s3_event_string)


def demonstrate_url_encoding():
    """
    Show why URL decoding of object key is CRITICAL
    
    PITFALL: Using raw (encoded) key in S3 GetObject → AccessDenied or 404
    Because the ACTUAL key has spaces/special chars, not + and %XX
    """
    print("\n" + "=" * 70)
    print("DEMONSTRATION: URL Encoding of Object Keys")
    print("=" * 70)

    test_cases = [
        # (raw_from_event, expected_actual_key)
        (
            "daily_exports/orders/orders_20250115.csv",
            "daily_exports/orders/orders_20250115.csv"
        ),
        (
            "daily_exports/reports/Q1+2025+Report.csv",
            "daily_exports/reports/Q1 2025 Report.csv"
        ),
        (
            "daily_exports/special/file+%28copy%29.csv",
            "daily_exports/special/file (copy).csv"
        ),
        (
            "daily_exports/unicode/caf%C3%A9+menu.csv",
            "daily_exports/unicode/café menu.csv"
        ),
    ]

    for raw, expected in test_cases:
        decoded = unquote_plus(raw)
        match = "✅" if decoded == expected else "❌"
        print(f"\n   Raw (from event):  {raw}")
        print(f"   Decoded:           {decoded}")
        print(f"   Expected:          {expected} {match}")

    print(f"\n   RULE: ALWAYS use urllib.parse.unquote_plus() on s3.object.key")
    print(f"   BEFORE passing to s3_client.get_object(Key=decoded_key)")


if __name__ == "__main__":
    # Parse raw S3 event (as seen in SNS email body)
    parsed = parse_s3_event_from_raw(SAMPLE_S3_EVENT)

    print(f"\n   📦 Parsed result dict:")
    for k, v in parsed.items():
        print(f"      {k}: {v}")

    # Show URL encoding importance
    demonstrate_url_encoding()

    print("\n" + "=" * 70)
    print("🎯 KEY STRUCTURES TO REMEMBER")
    print("=" * 70)
    print("""
    DIRECT S3 EVENT (S3 → SNS email, S3 → Lambda, S3 → EventBridge):
    ─────────────────────────────────────────────────────────────────
    json.loads(message_body)["Records"][0]["s3"]["bucket"]["name"]
    json.loads(message_body)["Records"][0]["s3"]["object"]["key"]
    
    SNS-WRAPPED S3 EVENT (S3 → SNS → SQS):
    ────────────────────────────────────────
    Layer 1: json.loads(sqs_message["Body"])                → SNS envelope
    Layer 2: json.loads(sns_envelope["Message"])             → S3 event
    Layer 3: s3_event["Records"][0]["s3"]["object"]["key"]   → object key
    Layer 4: unquote_plus(key)                               → actual key
    
    This 4-layer unwrap is used in Projects 18, 27, 28
    """)