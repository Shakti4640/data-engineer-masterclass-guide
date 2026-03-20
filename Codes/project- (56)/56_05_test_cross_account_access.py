# file: 56_05_test_cross_account_access.py
# Purpose: Comprehensive test suite for both cross-account patterns

import boto3
import json
import time
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/cross_account_config.json", "r") as f:
        return json.load(f)


def test_pattern1_direct_access(config):
    """
    Test Pattern 1: Direct S3 access via bucket policy
    
    Account B's role accesses Account A's bucket directly
    No AssumeRole needed — bucket policy grants access
    """
    print(f"\n{'═' * 60}")
    print(f"🧪 TESTING PATTERN 1: BUCKET POLICY (Direct Access)")
    print(f"{'═' * 60}")

    s3_client = boto3.client("s3", region_name=REGION)
    results = {}

    # Test 1: List objects in allowed prefix
    print(f"\n  Test 1.1: List allowed prefix (daily_exports/)...")
    try:
        response = s3_client.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="daily_exports/",
            MaxKeys=5
        )
        count = len(response.get("Contents", []))
        results["list_allowed"] = "✅ PASS"
        print(f"    {results['list_allowed']} — Found {count} objects")
    except Exception as e:
        results["list_allowed"] = f"❌ FAIL: {e}"
        print(f"    {results['list_allowed']}")

    # Test 2: Read object in allowed prefix
    print(f"\n  Test 1.2: Read object in allowed prefix...")
    try:
        response = s3_client.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="daily_exports/",
            MaxKeys=1
        )
        if response.get("Contents"):
            key = response["Contents"][0]["Key"]
            obj = s3_client.get_object(Bucket=config["bucket_name"], Key=key)
            size = obj["ContentLength"]
            results["read_allowed"] = "✅ PASS"
            print(f"    {results['read_allowed']} — Read {key} ({size} bytes)")
        else:
            results["read_allowed"] = "⚠️  SKIP — no objects found"
            print(f"    {results['read_allowed']}")
    except Exception as e:
        results["read_allowed"] = f"❌ FAIL: {e}"
        print(f"    {results['read_allowed']}")

    # Test 3: List denied prefix (should fail)
    print(f"\n  Test 1.3: List denied prefix (internal/) — should FAIL...")
    try:
        s3_client.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="internal/",
            MaxKeys=1
        )
        results["list_denied"] = "❌ SECURITY ISSUE — should have been denied!"
        print(f"    {results['list_denied']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["list_denied"] = "✅ PASS — correctly denied"
            print(f"    {results['list_denied']}")
        else:
            results["list_denied"] = f"⚠️  Unexpected error: {e}"
            print(f"    {results['list_denied']}")

    # Test 4: Write attempt (should fail)
    print(f"\n  Test 1.4: Write attempt — should FAIL...")
    try:
        s3_client.put_object(
            Bucket=config["bucket_name"],
            Key="daily_exports/unauthorized_write.txt",
            Body=b"This should not be allowed"
        )
        results["write_attempt"] = "❌ SECURITY ISSUE — write should be denied!"
        print(f"    {results['write_attempt']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["write_attempt"] = "✅ PASS — correctly denied"
            print(f"    {results['write_attempt']}")
        else:
            results["write_attempt"] = f"⚠️  Unexpected error: {e}"
            print(f"    {results['write_attempt']}")

    # Test 5: Delete attempt (should fail)
    print(f"\n  Test 1.5: Delete attempt — should FAIL...")
    try:
        s3_client.delete_object(
            Bucket=config["bucket_name"],
            Key="daily_exports/orders/orders_20250115.csv"
        )
        results["delete_attempt"] = "❌ SECURITY ISSUE — delete should be denied!"
        print(f"    {results['delete_attempt']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["delete_attempt"] = "✅ PASS — correctly denied"
            print(f"    {results['delete_attempt']}")
        else:
            results["delete_attempt"] = f"⚠️  Unexpected error: {e}"
            print(f"    {results['delete_attempt']}")

    return results


def test_pattern2_assume_role(config):
    """
    Test Pattern 2: AssumeRole + temporary credentials
    """
    print(f"\n{'═' * 60}")
    print(f"🧪 TESTING PATTERN 2: ASSUME ROLE (STS Temporary Credentials)")
    print(f"{'═' * 60}")

    sts_client = boto3.client("sts", region_name=REGION)
    results = {}

    # Test 1: AssumeRole with correct External ID
    print(f"\n  Test 2.1: AssumeRole with correct External ID...")
    try:
        response = sts_client.assume_role(
            RoleArn=config["cross_account_role_arn"],
            RoleSessionName="test-session",
            ExternalId=config["external_id"],
            DurationSeconds=900     # 15 min (minimum for testing)
        )
        credentials = response["Credentials"]
        results["assume_role"] = "✅ PASS"
        print(f"    {results['assume_role']} — Got temporary credentials")
        print(f"    Expires: {credentials['Expiration']}")
    except Exception as e:
        results["assume_role"] = f"❌ FAIL: {e}"
        print(f"    {results['assume_role']}")
        return results  # Can't continue without credentials

    # Test 2: AssumeRole with WRONG External ID (should fail)
    print(f"\n  Test 2.2: AssumeRole with WRONG External ID — should FAIL...")
    try:
        sts_client.assume_role(
            RoleArn=config["cross_account_role_arn"],
            RoleSessionName="test-wrong-id",
            ExternalId="wrong-external-id-12345",
            DurationSeconds=900
        )
        results["wrong_external_id"] = "❌ SECURITY ISSUE — wrong ID accepted!"
        print(f"    {results['wrong_external_id']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["wrong_external_id"] = "✅ PASS — correctly denied"
            print(f"    {results['wrong_external_id']}")
        else:
            results["wrong_external_id"] = f"⚠️  Error: {e}"
            print(f"    {results['wrong_external_id']}")

    # Test 3: AssumeRole WITHOUT External ID (should fail)
    print(f"\n  Test 2.3: AssumeRole WITHOUT External ID — should FAIL...")
    try:
        sts_client.assume_role(
            RoleArn=config["cross_account_role_arn"],
            RoleSessionName="test-no-id",
            DurationSeconds=900
            # No ExternalId parameter
        )
        results["no_external_id"] = "❌ SECURITY ISSUE — missing ID accepted!"
        print(f"    {results['no_external_id']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["no_external_id"] = "✅ PASS — correctly denied"
            print(f"    {results['no_external_id']}")
        else:
            results["no_external_id"] = f"⚠️  Error: {e}"
            print(f"    {results['no_external_id']}")

    # Test 4: Use assumed credentials for S3 access
    print(f"\n  Test 2.4: S3 access with assumed role credentials...")
    cross_s3 = boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"]
    )

    try:
        response = cross_s3.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="daily_exports/",
            MaxKeys=5
        )
        count = len(response.get("Contents", []))
        results["s3_via_assumed"] = "✅ PASS"
        print(f"    {results['s3_via_assumed']} — Listed {count} objects via assumed role")
    except Exception as e:
        results["s3_via_assumed"] = f"❌ FAIL: {e}"
        print(f"    {results['s3_via_assumed']}")

    # Test 5: Denied prefix via assumed role
    print(f"\n  Test 2.5: Denied prefix via assumed role — should FAIL...")
    try:
        cross_s3.list_objects_v2(
            Bucket=config["bucket_name"],
            Prefix="internal/",
            MaxKeys=1
        )
        results["denied_via_assumed"] = "❌ SECURITY ISSUE"
        print(f"    {results['denied_via_assumed']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["denied_via_assumed"] = "✅ PASS — correctly denied"
            print(f"    {results['denied_via_assumed']}")
        else:
            results["denied_via_assumed"] = f"⚠️  Error: {e}"
            print(f"    {results['denied_via_assumed']}")

    # Test 6: Write via assumed role (should fail — read-only role)
    print(f"\n  Test 2.6: Write via assumed role — should FAIL...")
    try:
        cross_s3.put_object(
            Bucket=config["bucket_name"],
            Key="daily_exports/unauthorized.txt",
            Body=b"should not work"
        )
        results["write_via_assumed"] = "❌ SECURITY ISSUE"
        print(f"    {results['write_via_assumed']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDenied":
            results["write_via_assumed"] = "✅ PASS — correctly denied"
            print(f"    {results['write_via_assumed']}")
        else:
            results["write_via_assumed"] = f"⚠️  Error: {e}"
            print(f"    {results['write_via_assumed']}")

    return results


def run_all_tests():
    """Run complete test suite for both patterns"""
    config = load_config()

    print("=" * 70)
    print("🧪 CROSS-ACCOUNT ACCESS — COMPREHENSIVE TEST SUITE")
    print(f"   Account A (data): {config['account_a_id']}")
    print(f"   Account B (analytics): {config['account_b_id']}")
    print(f"   Bucket: {config['bucket_name']}")
    print(f"   Time: {datetime.utcnow().isoformat()}Z")
    print("=" * 70)

    # Run Pattern 1 tests
    pattern1_results = test_pattern1_direct_access(config)

    # Run Pattern 2 tests
    pattern2_results = test_pattern2_assume_role(config)

    # --- FINAL REPORT ---
    all_results = {**pattern1_results, **pattern2_results}
    passed = sum(1 for v in all_results.values() if v.startswith("✅"))
    failed = sum(1 for v in all_results.values() if v.startswith("❌"))
    warnings = sum(1 for v in all_results.values() if v.startswith("⚠️"))

    print(f"\n{'=' * 70}")
    print(f"📊 FINAL TEST REPORT")
    print(f"{'=' * 70}")
    print(f"  Total tests:  {len(all_results)}")
    print(f"  ✅ Passed:    {passed}")
    print(f"  ❌ Failed:    {failed}")
    print(f"  ⚠️  Warnings: {warnings}")
    print(f"")

    for test_name, result in all_results.items():
        print(f"  {result[:2]} {test_name}")

    if failed > 0:
        print(f"\n  🔴 SECURITY REVIEW NEEDED — {failed} test(s) failed!")
    elif warnings > 0:
        print(f"\n  🟡 MOSTLY GOOD — {warnings} warning(s) to review")
    else:
        print(f"\n  🟢 ALL TESTS PASSED — cross-account access is correctly configured")

    print(f"{'=' * 70}")

    return all_results


if __name__ == "__main__":
    run_all_tests()