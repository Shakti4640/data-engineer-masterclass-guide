# file: 18_02_configure_s3_to_sqs.py
# Purpose: Configure S3 bucket to send ObjectCreated events to SQS
# Dependency: Queue from 18_01, S3 bucket from Project 1
# NOTE: Must remove Project 17 SNS notification if same prefix/suffix

import boto3
import json
from botocore.exceptions import ClientError

REGION = "us-east-2"
SOURCE_BUCKET = "quickcart-raw-data-prod"
QUEUE_NAME = "quickcart-file-processing-queue"


def get_queue_arn():
    """Get queue ARN from queue URL"""
    sqs_client = boto3.client("sqs", region_name=REGION)
    queue_url = sqs_client.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    return attrs["Attributes"]["QueueArn"]


def check_for_conflicting_notifications(s3_client):
    """
    Check if existing notifications conflict with our new one
    
    S3 RULE: No two notifications can have overlapping prefix+suffix
    targeting different service types (SNS vs SQS vs Lambda)
    
    If Project 17 SNS notification exists with same filter:
    → Must remove it first OR use different prefix
    """
    config = s3_client.get_bucket_notification_configuration(
        Bucket=SOURCE_BUCKET
    )
    config.pop("ResponseMetadata", None)

    our_prefix = "daily_exports/"
    our_suffix = ".csv"

    conflicts = []

    # Check SNS notifications for overlap
    for tc in config.get("TopicConfigurations", []):
        filters = tc.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        tc_prefix = ""
        tc_suffix = ""
        for f in filters:
            if f["Name"] == "prefix":
                tc_prefix = f["Value"]
            if f["Name"] == "suffix":
                tc_suffix = f["Value"]

        # Check overlap: our prefix starts with theirs OR theirs starts with ours
        prefix_overlap = (
            our_prefix.startswith(tc_prefix) or
            tc_prefix.startswith(our_prefix)
        )
        suffix_overlap = (our_suffix == tc_suffix) or (not tc_suffix) or (not our_suffix)

        if prefix_overlap and suffix_overlap:
            conflicts.append({
                "type": "SNS",
                "id": tc.get("Id", "unknown"),
                "prefix": tc_prefix,
                "suffix": tc_suffix,
                "arn": tc["TopicArn"]
            })

    return conflicts, config


def configure_s3_to_sqs():
    s3_client = boto3.client("s3", region_name=REGION)
    queue_arn = get_queue_arn()

    # ── CHECK FOR CONFLICTS ──
    print("=" * 70)
    print("STEP 1: Checking for conflicting notifications")
    print("=" * 70)

    conflicts, existing_config = check_for_conflicting_notifications(s3_client)

    if conflicts:
        print(f"   ⚠️  Found {len(conflicts)} conflicting notification(s):")
        for c in conflicts:
            print(f"      Type: {c['type']} | ID: {c['id']}")
            print(f"      Prefix: '{c['prefix']}' | Suffix: '{c['suffix']}'")
            print(f"      ARN: {c['arn']}")

        print(f"\n   RESOLVING: Removing conflicting notifications...")
        print(f"   → Project 17 SNS notification will be removed")
        print(f"   → This is safe — we're replacing SNS with SQS")

        # Remove conflicting SNS notifications
        conflict_ids = {c["id"] for c in conflicts}
        existing_config["TopicConfigurations"] = [
            tc for tc in existing_config.get("TopicConfigurations", [])
            if tc.get("Id") not in conflict_ids
        ]
    else:
        print("   ✅ No conflicting notifications found")

    # ── BUILD SQS NOTIFICATION ──
    print("\n" + "=" * 70)
    print("STEP 2: Building S3 → SQS notification configuration")
    print("=" * 70)

    sqs_notification = {
        "Id": "daily-csv-sqs-processing",
        "QueueArn": queue_arn,
        "Events": [
            "s3:ObjectCreated:*"
        ],
        "Filter": {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": "daily_exports/"},
                    {"Name": "suffix", "Value": ".csv"}
                ]
            }
        }
    }

    print(f"   Queue ARN:  {queue_arn}")
    print(f"   Events:     s3:ObjectCreated:*")
    print(f"   Prefix:     daily_exports/")
    print(f"   Suffix:     .csv")

    # ── MERGE WITH EXISTING CONFIG ──
    # Same safe merge pattern as Project 17
    print("\n" + "=" * 70)
    print("STEP 3: Merging with existing configuration")
    print("=" * 70)

    queue_configs = existing_config.get("QueueConfigurations", [])
    # Remove any existing config with same ID (update scenario)
    queue_configs = [
        qc for qc in queue_configs
        if qc.get("Id") != "daily-csv-sqs-processing"
    ]
    queue_configs.append(sqs_notification)

    full_config = {
        "TopicConfigurations": existing_config.get("TopicConfigurations", []),
        "QueueConfigurations": queue_configs,
        "LambdaFunctionConfigurations": existing_config.get(
            "LambdaFunctionConfigurations", []
        )
    }

    # Remove empty lists
    full_config = {k: v for k, v in full_config.items() if v}

    sns_count = len(full_config.get("TopicConfigurations", []))
    sqs_count = len(full_config.get("QueueConfigurations", []))
    lambda_count = len(full_config.get("LambdaFunctionConfigurations", []))
    print(f"   SNS notifications: {sns_count}")
    print(f"   SQS notifications: {sqs_count}")
    print(f"   Lambda notifications: {lambda_count}")

    # ── APPLY ──
    print("\n" + "=" * 70)
    print("STEP 4: Applying notification configuration")
    print("=" * 70)

    try:
        s3_client.put_bucket_notification_configuration(
            Bucket=SOURCE_BUCKET,
            NotificationConfiguration=full_config
        )
        print(f"   ✅ S3 → SQS notification configured on '{SOURCE_BUCKET}'")
    except ClientError as e:
        if "Unable to validate" in str(e):
            print(f"   ❌ VALIDATION FAILED")
            print(f"   → Queue Policy may not allow S3")
            print(f"   → Run 18_01_create_sqs_queue.py first")
            print(f"   → Or: check for remaining prefix/suffix conflicts")
            return False
        elif "ambiguously defined" in str(e).lower():
            print(f"   ❌ OVERLAPPING FILTER CONFLICT")
            print(f"   → Another notification has same prefix+suffix")
            print(f"   → Remove conflicting notification first")
            return False
        else:
            raise

    # ── VERIFY ──
    print("\n" + "=" * 70)
    print("STEP 5: Verifying applied configuration")
    print("=" * 70)

    verify = s3_client.get_bucket_notification_configuration(Bucket=SOURCE_BUCKET)

    for qc in verify.get("QueueConfigurations", []):
        print(f"   ID:         {qc.get('Id', 'N/A')}")
        print(f"   Queue ARN:  {qc['QueueArn']}")
        print(f"   Events:     {', '.join(qc['Events'])}")
        filters = qc.get("Filter", {}).get("Key", {}).get("FilterRules", [])
        for f in filters:
            print(f"   Filter {f['Name']:>6}: {f['Value']}")

    return True


if __name__ == "__main__":
    success = configure_s3_to_sqs()
    if success:
        print("\n🎯 S3 → SQS notification is LIVE")
        print("   → Upload a CSV to daily_exports/ to send message to queue")
        print("   → Run 18_03_test_and_consume.py to test")
    else:
        print("\n❌ Setup failed — see errors above")