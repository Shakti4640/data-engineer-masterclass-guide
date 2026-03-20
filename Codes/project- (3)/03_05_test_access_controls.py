# file: 14_test_access_controls.py
# Purpose: Verify that each user can ONLY do what they're supposed to
# DEPENDS ON: Steps 1-4

import boto3
from botocore.exceptions import ClientError
import json

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"

# Test scenarios: (description, action, key, expected_result)
# Each user will be tested against these

TEST_SCENARIOS = [
    # --- READ TESTS ---
    {
        "name": "List daily_exports/orders/",
        "action": "list",
        "prefix": "daily_exports/orders/",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "ALLOW",
            "QuickCart-Marketing": "DENY",
            "QuickCart-Interns": "ALLOW",
        }
    },
    {
        "name": "List daily_exports/customers/",
        "action": "list",
        "prefix": "daily_exports/customers/",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "DENY",
            "QuickCart-Marketing": "ALLOW",
            "QuickCart-Interns": "ALLOW",
        }
    },
    {
        "name": "List finance_reports/",
        "action": "list",
        "prefix": "finance_reports/",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "ALLOW",
            "QuickCart-Marketing": "DENY",
            "QuickCart-Interns": "DENY",
        }
    },
    {
        "name": "List marketing_data/",
        "action": "list",
        "prefix": "marketing_data/",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "DENY",
            "QuickCart-Marketing": "ALLOW",
            "QuickCart-Interns": "DENY",
        }
    },
    # --- WRITE TESTS ---
    {
        "name": "Upload to daily_exports/orders/",
        "action": "put",
        "key": "daily_exports/orders/test_access.csv",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "DENY",
            "QuickCart-Marketing": "DENY",
            "QuickCart-Interns": "DENY",
        }
    },
    # --- DELETE TESTS ---
    {
        "name": "Delete from daily_exports/orders/",
        "action": "delete",
        "key": "daily_exports/orders/test_access.csv",
        "expected": {
            "QuickCart-DataEngineering": "ALLOW",
            "QuickCart-Finance": "DENY",
            "QuickCart-Marketing": "DENY",
            "QuickCart-Interns": "DENY",
        }
    },
]


def test_with_policy_simulator(iam_client, user_arn, action_name, resource_arn):
    """
    Use IAM Policy Simulator to test without making actual S3 calls
    
    WHY SIMULATOR:
    → Don't need actual access keys for test users
    → Don't need actual objects in S3
    → Shows WHICH policy allowed/denied
    → Safe — doesn't modify any resources
    
    LIMITATION:
    → Simulator does NOT evaluate bucket policies
    → Only evaluates IAM policies attached to the principal
    → For full testing: must make actual API calls
    """
    # Map friendly action to IAM action
    action_map = {
        "list": "s3:ListBucket",
        "get": "s3:GetObject",
        "put": "s3:PutObject",
        "delete": "s3:DeleteObject"
    }

    iam_action = action_map.get(action_name, action_name)

    try:
        response = iam_client.simulate_principal_policy(
            PolicySourceArn=user_arn,
            ActionNames=[iam_action],
            ResourceArns=[resource_arn],
        )

        result = response["EvaluationResults"][0]
        decision = result["EvalDecision"]  # "allowed" or "implicitDeny" or "explicitDeny"
        return decision

    except ClientError as e:
        return f"ERROR: {e}"


def run_access_tests():
    """
    Run all test scenarios using Policy Simulator
    Display results as a matrix
    """
    iam_client = boto3.client("iam")
    account_id = boto3.client("sts").get_caller_identity()["Account"]

    # Map groups to test users
    group_users = {
        "QuickCart-DataEngineering": f"arn:aws:iam::{account_id}:user/testuser-alice-eng",
        "QuickCart-Finance": f"arn:aws:iam::{account_id}:user/testuser-bob-fin",
        "QuickCart-Marketing": f"arn:aws:iam::{account_id}:user/testuser-carol-mkt",
        "QuickCart-Interns": f"arn:aws:iam::{account_id}:user/testuser-dave-intern",
    }

    print("=" * 90)
    print("🧪 ACCESS CONTROL TEST RESULTS")
    print("=" * 90)

    all_passed = True

    for scenario in TEST_SCENARIOS:
        print(f"\n📌 Test: {scenario['name']}")
        print(f"   Action: {scenario['action']}")
        print("-" * 70)

        # Determine resource ARN
        if scenario["action"] == "list":
            resource_arn = f"arn:aws:s3:::{BUCKET_NAME}"
        else:
            resource_arn = f"arn:aws:s3:::{BUCKET_NAME}/{scenario.get('key', '')}"

        for group_name, user_arn in group_users.items():
            expected = scenario["expected"].get(group_name, "UNKNOWN")

            # Simulate
            result = test_with_policy_simulator(
                iam_client, user_arn, scenario["action"], resource_arn
            )

            # Normalize result
            if "allowed" in str(result).lower():
                actual = "ALLOW"
            else:
                actual = "DENY"

            # Compare
            match = "✅" if actual == expected else "❌"
            if actual != expected:
                all_passed = False

            short_group = group_name.replace("QuickCart-", "")
            print(f"   {match} {short_group:<20} Expected: {expected:<6} Got: {actual:<6}")

    print("\n" + "=" * 90)
    if all_passed:
        print("✅ ALL TESTS PASSED — Access controls working correctly")
    else:
        print("❌ SOME TESTS FAILED — Review policies")
        print("   ⚠️  Note: Simulator doesn't evaluate bucket policies")
        print("   → Bucket policy DENY may cause 'ALLOW' in simulator")
        print("     but 'DENY' in actual API calls")
    print("=" * 90)


if __name__ == "__main__":
    run_access_tests()