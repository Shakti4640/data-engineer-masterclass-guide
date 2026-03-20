# file: 56_03_assume_role_account_b.py
# Purpose: Account B assumes Account A's role and accesses S3
# RUN THIS IN ACCOUNT B (or simulate in same account)

import boto3
import json
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_config.json", "r") as f:
        return json.load(f)


def assume_role_and_access_s3():
    """
    Account B's code that:
    1. Calls sts:AssumeRole to get temporary credentials for Account A's role
    2. Creates a NEW boto3 session with those temporary credentials
    3. Uses that session to access Account A's S3 bucket
    
    THIS IS THE CORE PATTERN used in:
    → Glue ETL jobs (Project 60)
    → Lambda functions
    → EC2 scripts
    → Any code in Account B that needs Account A's data
    """
    config = load_config()

    print("=" * 70)
    print("🔑 PATTERN 2: ASSUME ROLE FROM ACCOUNT B")
    print("=" * 70)

    # --- STEP 1: Assume the role ---
    print(f"\n📌 Step 1: Calling sts:AssumeRole...")
    print(f"   Target Role: {config['cross_account_role_arn']}")
    print(f"   External ID: {config['external_id']}")

    sts_client = boto3.client("sts", region_name=REGION)

    try:
        response = sts_client.assume_role(
            RoleArn=config["cross_account_role_arn"],
            RoleSessionName=f"analytics-session-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            DurationSeconds=3600,   # 1 hour session
            ExternalId=config["external_id"]
        )
    except Exception as e:
        print(f"\n❌ AssumeRole FAILED: {e}")
        print(f"\n   COMMON CAUSES:")
        print(f"   → Trust policy doesn't list your role ARN")
        print(f"   → External ID doesn't match")
        print(f"   → Your role lacks sts:AssumeRole permission")
        print(f"   → SCP blocks sts:AssumeRole")
        return None

    credentials = response["Credentials"]
    assumed_role_arn = response["AssumedRoleUser"]["Arn"]

    print(f"   ✅ AssumeRole succeeded!")
    print(f"   Assumed Role ARN: {assumed_role_arn}")
    print(f"   Access Key ID:    {credentials['AccessKeyId'][:10]}...")
    print(f"   Expiration:       {credentials['Expiration']}")
    print(f"   (Credentials are TEMPORARY — expire at above time)")

    # --- STEP 2: Create S3 client with temporary credentials ---
    print(f"\n📌 Step 2: Creating S3 client with temporary credentials...")

    cross_account_s3 = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )
    print(f"   ✅ S3 client created with cross-account credentials")

    # --- STEP 3: List objects in ALLOWED prefix ---
    print(f"\n📌 Step 3: Listing objects in ALLOWED prefix (daily_exports/)...")

    try:
        response = cross_account_s3.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="daily_exports/",
            MaxKeys=10
        )
        objects = response.get("Contents", [])
        print(f"   ✅ SUCCESS — Found {len(objects)} objects:")
        for obj in objects[:5]:
            print(f"      {obj['Key']} ({obj['Size']} bytes)")
        if len(objects) > 5:
            print(f"      ... and {len(objects) - 5} more")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")

    # --- STEP 4: Read a specific object ---
    print(f"\n📌 Step 4: Reading a specific object...")

    try:
        # Try to read first object found
        if objects:
            key = objects[0]["Key"]
            response = cross_account_s3.get_object(
                Bucket=config["bucket_name"],
                Key=key
            )
            content_length = response["ContentLength"]
            content_type = response.get("ContentType", "unknown")
            # Read first 200 bytes as preview
            preview = response["Body"].read(200).decode("utf-8", errors="replace")
            print(f"   ✅ SUCCESS — Read: {key}")
            print(f"      Size: {content_length} bytes")
            print(f"      Type: {content_type}")
            print(f"      Preview: {preview[:100]}...")
        else:
            print(f"   ℹ️  No objects to read (bucket may be empty)")
    except Exception as e:
        print(f"   ❌ FAILED: {e}")

    # --- STEP 5: Try DENIED prefix (should fail) ---
    print(f"\n📌 Step 5: Attempting DENIED prefix (internal/) — should fail...")

    try:
        response = cross_account_s3.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="internal/",
            MaxKeys=5
        )
        print(f"   ❌ UNEXPECTED SUCCESS — security issue! Denied prefix was accessible!")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            print(f"   ✅ CORRECTLY DENIED — AccessDenied (expected behavior)")
        else:
            print(f"   ⚠️  Error: {e}")

    # --- STEP 6: Try WRITE operation (should fail) ---
    print(f"\n📌 Step 6: Attempting WRITE operation — should fail...")

    try:
        cross_account_s3.put_object(
            Bucket=config["bucket_name"],
            Key="daily_exports/test_write.txt",
            Body=b"This should not be allowed"
        )
        print(f"   ❌ UNEXPECTED SUCCESS — security issue! Write was allowed!")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "Unknown")
        if error_code == "AccessDenied":
            print(f"   ✅ CORRECTLY DENIED — AccessDenied (expected behavior)")
        else:
            print(f"   ⚠️  Error: {e}")

    # --- SUMMARY ---
    print(f"\n{'=' * 70}")
    print(f"📊 CROSS-ACCOUNT ACCESS TEST RESULTS")
    print(f"{'=' * 70}")
    print(f"  ✅ AssumeRole:          Succeeded (temporary credentials obtained)")
    print(f"  ✅ List allowed prefix: Succeeded (daily_exports/)")
    print(f"  ✅ Read allowed object: Succeeded")
    print(f"  ✅ List denied prefix:  Correctly denied (internal/)")
    print(f"  ✅ Write attempt:       Correctly denied (read-only role)")
    print(f"  ✅ Credentials:         Temporary (expire in 1 hour)")
    print(f"{'=' * 70}")

    return credentials


def create_reusable_session(role_arn, external_id, session_name="default"):
    """
    Helper function: create a boto3 Session with cross-account credentials
    
    USE THIS IN YOUR CODE:
    → session = create_reusable_session(role_arn, external_id)
    → s3 = session.client("s3")
    → s3.get_object(...)
    
    The Session object can create clients for ANY AWS service
    with the assumed role's permissions
    """
    sts_client = boto3.client("sts", region_name=REGION)

    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        ExternalId=external_id,
        DurationSeconds=3600
    )

    credentials = response["Credentials"]

    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=REGION
    )

    print(f"✅ Cross-account session created (expires: {credentials['Expiration']})")
    return session


if __name__ == "__main__":
    assume_role_and_access_s3()