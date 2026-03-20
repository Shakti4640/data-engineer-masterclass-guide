# file: 06_08_audit_role_usage.py
# Purpose: Query CloudTrail to verify role-based access is working
# and no old access keys are still being used anywhere

import boto3
from datetime import datetime, timedelta


def audit_s3_access_last_24h():
    """
    Look up CloudTrail events for S3 access
    Verify ALL calls come from assumed-role (not IAM user keys)
    """
    ct_client = boto3.client("cloudtrail")

    print("=" * 60)
    print("🔍 CLOUDTRAIL AUDIT — S3 Access (Last 24 Hours)")
    print("=" * 60)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    # --- Look up PutObject events ---
    for event_name in ["PutObject", "GetObject", "ListObjects"]:
        print(f"\n📋 Event: {event_name}")
        print("-" * 50)

        try:
            response = ct_client.lookup_events(
                LookupAttributes=[
                    {
                        "AttributeKey": "EventName",
                        "AttributeValue": event_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                MaxResults=10
            )

            events = response.get("Events", [])

            if not events:
                print(f"   No {event_name} events in last 24 hours")
                continue

            for event in events:
                event_time = event["EventTime"].strftime("%Y-%m-%d %H:%M:%S")
                username = event.get("Username", "Unknown")
                
                # Parse the CloudTrail event for detailed info
                import json
                detail = json.loads(event.get("CloudTrailEvent", "{}"))
                
                user_identity = detail.get("userIdentity", {})
                identity_type = user_identity.get("type", "Unknown")
                arn = user_identity.get("arn", "Unknown")
                source_ip = detail.get("sourceIPAddress", "Unknown")

                # Determine if this is role-based or key-based
                if identity_type == "AssumedRole":
                    status = "✅ IAM Role"
                elif identity_type == "IAMUser":
                    status = "⚠️  IAM User (static keys!)"
                else:
                    status = f"❓ {identity_type}"

                print(f"   {event_time} | {status}")
                print(f"     ARN: {arn}")
                print(f"     IP:  {source_ip}")
                print()

        except Exception as e:
            print(f"   Error querying CloudTrail: {e}")
            print(f"   → CloudTrail may need time to populate events")
            print(f"   → Events typically appear within 15 minutes")


def check_for_stale_key_usage():
    """
    Specifically check if any DEACTIVATED keys were attempted
    These show up as AccessDenied events in CloudTrail
    """
    ct_client = boto3.client("cloudtrail")

    print(f"\n{'=' * 60}")
    print("🚨 CHECKING FOR FAILED ACCESS (Stale Keys)")
    print("=" * 60)

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=7)

    try:
        response = ct_client.lookup_events(
            LookupAttributes=[
                {
                    "AttributeKey": "EventName",
                    "AttributeValue": "PutObject"
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            MaxResults=50
        )

        failed_events = []
        for event in response.get("Events", []):
            import json
            detail = json.loads(event.get("CloudTrailEvent", "{}"))
            
            error_code = detail.get("errorCode", "")
            if error_code in ["AccessDenied", "InvalidClientTokenId", "SignatureDoesNotMatch"]:
                failed_events.append({
                    "time": event["EventTime"].strftime("%Y-%m-%d %H:%M"),
                    "error": error_code,
                    "ip": detail.get("sourceIPAddress", "Unknown"),
                    "arn": detail.get("userIdentity", {}).get("arn", "Unknown")
                })

        if failed_events:
            print(f"\n⚠️  Found {len(failed_events)} FAILED access attempts:")
            for fe in failed_events:
                print(f"   {fe['time']} | {fe['error']} | IP: {fe['ip']}")
                print(f"     ARN: {fe['arn']}")
            print(f"\n   → These may be scripts still using OLD deactivated keys")
            print(f"   → Find and migrate them to IAM Role")
        else:
            print(f"\n✅ No failed access attempts in the last 7 days")
            print(f"   → Safe to permanently delete old keys")

    except Exception as e:
        print(f"   Error: {e}")


if __name__ == "__main__":
    audit_s3_access_last_24h()
    check_for_stale_key_usage()