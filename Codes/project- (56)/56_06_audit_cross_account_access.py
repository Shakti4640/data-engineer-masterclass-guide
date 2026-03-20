# file: 56_06_audit_cross_account_access.py
# Purpose: Verify CloudTrail logs show cross-account access events

import boto3
import json
from datetime import datetime, timedelta

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_config.json", "r") as f:
        return json.load(f)


def check_cloudtrail_events():
    """
    Verify cross-account access is logged in CloudTrail
    
    CloudTrail records:
    → sts:AssumeRole (who assumed what role, when, from where)
    → s3:GetObject (who accessed what object, when)
    → Both events linked by the assumed role session name
    
    THIS IS HOW SECURITY TEAM AUDITS CROSS-ACCOUNT ACCESS
    """
    config = load_config()
    ct_client = boto3.client("cloudtrail", region_name=REGION)

    print("=" * 70)
    print("🔍 CLOUDTRAIL AUDIT — CROSS-ACCOUNT ACCESS EVENTS")
    print("=" * 70)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    # --- Look for AssumeRole events ---
    print(f"\n📌 Searching for sts:AssumeRole events (last 1 hour)...")

    try:
        response = ct_client.lookup_events(
            LookupAttributes=[
                {
                    "AttributeKey": "EventName",
                    "AttributeValue": "AssumeRole"
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=10
        )

        events = response.get("Events", [])
        if events:
            print(f"  Found {len(events)} AssumeRole event(s):")
            for event in events:
                event_time = event["EventTime"].strftime("%Y-%m-%d %H:%M:%S")
                username = event.get("Username", "N/A")
                
                # Parse CloudTrail event JSON for details
                ct_event = json.loads(event.get("CloudTrailEvent", "{}"))
                request_params = ct_event.get("requestParameters", {})
                role_arn = request_params.get("roleArn", "N/A")
                session_name = request_params.get("roleSessionName", "N/A")
                external_id = request_params.get("externalId", "N/A")
                source_ip = ct_event.get("sourceIPAddress", "N/A")
                
                print(f"\n    📝 Event at {event_time}")
                print(f"       Caller: {username}")
                print(f"       Role assumed: {role_arn}")
                print(f"       Session name: {session_name}")
                print(f"       External ID: {'[present]' if external_id != 'N/A' else '[missing]'}")
                print(f"       Source IP: {source_ip}")
                
                # Check if this is our cross-account role
                if config.get("cross_account_role_arn", "") in str(role_arn):
                    print(f"       ✅ This is our cross-account role!")
        else:
            print(f"  ℹ️  No AssumeRole events found in last hour")
            print(f"  → Run 56_05_test_cross_account_access.py first")

    except Exception as e:
        print(f"  ⚠️  CloudTrail lookup error: {e}")
        print(f"  → CloudTrail may not be enabled or may have access restrictions")

    # --- Look for S3 GetObject events ---
    print(f"\n📌 Searching for s3:GetObject events (last 1 hour)...")

    try:
        response = ct_client.lookup_events(
            LookupAttributes=[
                {
                    "AttributeKey": "EventName",
                    "AttributeValue": "GetObject"
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=10
        )

        events = response.get("Events", [])
        if events:
            print(f"  Found {len(events)} GetObject event(s):")
            for event in events[:5]:
                event_time = event["EventTime"].strftime("%Y-%m-%d %H:%M:%S")
                username = event.get("Username", "N/A")
                
                ct_event = json.loads(event.get("CloudTrailEvent", "{}"))
                request_params = ct_event.get("requestParameters", {})
                bucket = request_params.get("bucketName", "N/A")
                key = request_params.get("key", "N/A")
                
                # Check if caller is our assumed role
                user_identity = ct_event.get("userIdentity", {})
                arn = user_identity.get("arn", "")
                identity_type = user_identity.get("type", "")
                
                print(f"\n    📝 GetObject at {event_time}")
                print(f"       Caller: {username}")
                print(f"       Identity type: {identity_type}")
                print(f"       Bucket: {bucket}")
                print(f"       Key: {key[:60]}...")
                
                if "CrossAccount" in arn or "assumed-role" in arn:
                    print(f"       ✅ Cross-account access detected!")
                    print(f"       Full ARN: {arn}")
        else:
            print(f"  ℹ️  No GetObject events found (S3 data events may not be enabled)")
            print(f"  → Enable S3 data events in CloudTrail for full audit")

    except Exception as e:
        print(f"  ⚠️  CloudTrail lookup error: {e}")

    # --- AUDIT RECOMMENDATIONS ---
    print(f"\n{'=' * 70}")
    print(f"📋 AUDIT RECOMMENDATIONS")
    print(f"{'=' * 70}")
    print(f"  1. Enable CloudTrail in BOTH accounts")
    print(f"  2. Enable S3 data events for production buckets")
    print(f"  3. Set up CloudWatch alarm on unexpected AssumeRole calls")
    print(f"  4. Review cross-account access monthly")
    print(f"  5. Use IAM Access Analyzer to detect unintended access")
    print(f"  6. Enable S3 Access Logging for detailed request logs")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    check_cloudtrail_events()