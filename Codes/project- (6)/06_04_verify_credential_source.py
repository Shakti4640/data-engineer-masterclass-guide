# file: 06_04_verify_credential_source.py
# Purpose: Confirm boto3 is using IAM Role, not static keys

import boto3
import json
from datetime import datetime


def check_credential_source():
    """
    Determine exactly WHERE boto3 is getting credentials from
    """
    print("=" * 60)
    print("🔐 CREDENTIAL SOURCE VERIFICATION")
    print("=" * 60)

    session = boto3.Session()
    credentials = session.get_credentials()

    if credentials is None:
        print("\n❌ NO CREDENTIALS FOUND")
        print("   → Instance Profile may not be attached")
        print("   → Run: 06_02_attach_profile_to_ec2.py")
        return False

    # --- Resolve to actual credentials ---
    resolved = credentials.get_frozen_credentials()

    print(f"\n📋 Credential Details:")
    print(f"   Method:           {credentials.method}")
    print(f"   Access Key ID:    {resolved.access_key[:8]}...{resolved.access_key[-4:]}")

    # AKIA = permanent (IAM User) | ASIA = temporary (Role/STS)
    key_prefix = resolved.access_key[:4]
    if key_prefix == "ASIA":
        print(f"   Key Type:         TEMPORARY (starts with ASIA) ✅")
    elif key_prefix == "AKIA":
        print(f"   Key Type:         PERMANENT (starts with AKIA) ⚠️")
        print(f"   → This means you're using IAM User access keys")
        print(f"   → Delete ~/.aws/credentials and use IAM Role instead")

    if resolved.token:
        print(f"   Session Token:    {resolved.token[:20]}... (present) ✅")
    else:
        print(f"   Session Token:    NONE ⚠️ (permanent credentials)")

    # --- Get caller identity ---
    sts_client = boto3.client("sts")
    identity = sts_client.get_caller_identity()

    print(f"\n📋 Caller Identity:")
    print(f"   Account:          {identity['Account']}")
    print(f"   ARN:              {identity['Arn']}")
    print(f"   User ID:          {identity['UserId']}")

    # Parse the ARN to determine identity type
    arn = identity["Arn"]
    if "assumed-role" in arn:
        role_name = arn.split("/")[1]
        instance_id = arn.split("/")[2] if len(arn.split("/")) > 2 else "unknown"
        print(f"\n✅ CONFIRMED: Using IAM Role")
        print(f"   Role Name:        {role_name}")
        print(f"   Instance ID:      {instance_id}")
        return True
    elif "user/" in arn:
        user_name = arn.split("/")[-1]
        print(f"\n⚠️  WARNING: Using IAM User '{user_name}' (static credentials)")
        print(f"   → Migrate to IAM Role immediately")
        return False
    else:
        print(f"\n❓ Unknown identity type: {arn}")
        return False


def test_s3_access_with_role():
    """
    Test actual S3 operations to confirm permissions work
    """
    print("\n" + "=" * 60)
    print("📦 S3 ACCESS TEST")
    print("=" * 60)

    s3_client = boto3.client("s3")
    bucket = "quickcart-raw-data-prod"

    # --- Test 1: ListBucket ---
    print("\n🧪 Test 1: ListBucket (s3:ListBucket)")
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            MaxKeys=3
        )
        count = response.get("KeyCount", 0)
        print(f"   ✅ PASS — Found {count} objects")
    except Exception as e:
        print(f"   ❌ FAIL — {e}")

    # --- Test 2: PutObject ---
    print("\n🧪 Test 2: PutObject (s3:PutObject)")
    test_key = "test/credential_verification_test.txt"
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=test_key,
            Body=f"Credential test at {datetime.now().isoformat()}".encode(),
            ContentType="text/plain"
        )
        print(f"   ✅ PASS — Wrote to s3://{bucket}/{test_key}")
    except Exception as e:
        print(f"   ❌ FAIL — {e}")

    # --- Test 3: GetObject ---
    print("\n🧪 Test 3: GetObject (s3:GetObject)")
    try:
        response = s3_client.get_object(
            Bucket=bucket,
            Key=test_key
        )
        body = response["Body"].read().decode()
        print(f"   ✅ PASS — Read back: '{body}'")
    except Exception as e:
        print(f"   ❌ FAIL — {e}")

    # --- Test 4: DeleteObject (should FAIL — we didn't grant this) ---
    print("\n🧪 Test 4: DeleteObject (should FAIL — not in policy)")
    try:
        s3_client.delete_object(
            Bucket=bucket,
            Key=test_key
        )
        print(f"   ⚠️  UNEXPECTED PASS — delete worked!")
        print(f"   → Role may have broader permissions than intended")
        print(f"   → Review the permission policy")
    except Exception as e:
        if "AccessDenied" in str(e):
            print(f"   ✅ CORRECTLY DENIED — least privilege working!")
        else:
            print(f"   ❌ Unexpected error: {e}")


def show_credential_metadata_from_imds():
    """
    Show what the EC2 Instance Metadata Service returns
    This only works ON the EC2 instance
    """
    print("\n" + "=" * 60)
    print("📡 IMDS CREDENTIAL ENDPOINT (EC2 only)")
    print("=" * 60)

    try:
        import urllib.request

        # --- IMDSv2: Get session token first ---
        token_url = "http://169.254.169.254/latest/api/token"
        token_req = urllib.request.Request(
            token_url,
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
        )
        token = urllib.request.urlopen(token_req, timeout=2).read().decode()

        # --- Get role name ---
        role_url = "http://169.254.169.254/latest/meta-data/iam/security-credentials/"
        role_req = urllib.request.Request(
            role_url,
            headers={"X-aws-ec2-metadata-token": token}
        )
        role_name = urllib.request.urlopen(role_req, timeout=2).read().decode().strip()
        print(f"\n   Role Name: {role_name}")

        # --- Get credentials ---
        cred_url = f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{role_name}"
        cred_req = urllib.request.Request(
            cred_url,
            headers={"X-aws-ec2-metadata-token": token}
        )
        cred_data = json.loads(
            urllib.request.urlopen(cred_req, timeout=2).read().decode()
        )

        print(f"   Access Key:      {cred_data['AccessKeyId'][:8]}...{cred_data['AccessKeyId'][-4:]}")
        print(f"   Key Prefix:      {cred_data['AccessKeyId'][:4]} ({'TEMPORARY' if cred_data['AccessKeyId'][:4] == 'ASIA' else 'PERMANENT'})")
        print(f"   Token Present:   {'Yes' if cred_data.get('Token') else 'No'}")
        print(f"   Expiration:      {cred_data['Expiration']}")
        print(f"   Last Updated:    {cred_data['LastUpdated']}")
        print(f"   Type:            {cred_data['Type']}")

    except urllib.error.URLError:
        print("\n   ℹ️  Cannot reach IMDS — this script is not running on EC2")
        print("   → IMDS (169.254.169.254) is only available from within EC2 instances")
    except Exception as e:
        print(f"\n   ⚠️  Error querying IMDS: {e}")


if __name__ == "__main__":
    is_role = check_credential_source()
    if is_role:
        test_s3_access_with_role()
    show_credential_metadata_from_imds()