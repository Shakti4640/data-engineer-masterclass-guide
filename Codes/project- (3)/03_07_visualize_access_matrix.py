# file: 16_visualize_access_matrix.py
# Purpose: Generate a visual access matrix for documentation and interviews
# DEPENDS ON: Understanding from Steps 1-5
# USE WHEN: presenting access design to stakeholders or during interviews

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "quickcart-raw-data-prod"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# Complete access matrix — source of truth
ACCESS_MATRIX = {
    "Prefixes": [
        "daily_exports/orders/",
        "daily_exports/customers/",
        "daily_exports/products/",
        "finance_reports/",
        "marketing_data/"
    ],
    "Teams": {
        "DataEngineering": {
            "daily_exports/orders/":     {"list": "✅", "read": "✅", "write": "✅", "delete": "✅"},
            "daily_exports/customers/":  {"list": "✅", "read": "✅", "write": "✅", "delete": "✅"},
            "daily_exports/products/":   {"list": "✅", "read": "✅", "write": "✅", "delete": "✅"},
            "finance_reports/":          {"list": "✅", "read": "✅", "write": "✅", "delete": "✅"},
            "marketing_data/":           {"list": "✅", "read": "✅", "write": "✅", "delete": "✅"},
        },
        "Finance": {
            "daily_exports/orders/":     {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "daily_exports/customers/":  {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
            "daily_exports/products/":   {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "finance_reports/":          {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "marketing_data/":           {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
        },
        "Marketing": {
            "daily_exports/orders/":     {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
            "daily_exports/customers/":  {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "daily_exports/products/":   {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "finance_reports/":          {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
            "marketing_data/":           {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
        },
        "Interns": {
            "daily_exports/orders/":     {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "daily_exports/customers/":  {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "daily_exports/products/":   {"list": "✅", "read": "✅", "write": "❌", "delete": "❌"},
            "finance_reports/":          {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
            "marketing_data/":           {"list": "❌", "read": "❌", "write": "❌", "delete": "❌"},
        },
    }
}


def print_access_matrix():
    """
    Print formatted access matrix
    Perfect for: documentation, interviews, SOC 2 audit evidence
    """
    teams = ACCESS_MATRIX["Teams"]
    prefixes = ACCESS_MATRIX["Prefixes"]
    actions = ["list", "read", "write", "delete"]

    print("=" * 100)
    print("📊 S3 ACCESS CONTROL MATRIX — quickcart-raw-data-prod")
    print("=" * 100)

    for team_name, prefix_perms in teams.items():
        print(f"\n🏷️  TEAM: {team_name}")
        print("-" * 80)
        header = f"  {'Prefix':<30}"
        for action in actions:
            header += f" {action.upper():<8}"
        print(header)
        print("-" * 80)

        for prefix in prefixes:
            perms = prefix_perms[prefix]
            row = f"  {prefix:<30}"
            for action in actions:
                row += f" {perms[action]:<8}"
            print(row)

    print("\n" + "=" * 100)


def print_policy_enforcement_layers():
    """Show which layer enforces each permission"""
    print("\n🛡️  ENFORCEMENT LAYERS:")
    print("-" * 70)
    print(f"  {'Layer':<25} {'Mechanism':<25} {'Type':<15}")
    print("-" * 70)
    print(f"  {'1. Public Access Block':<25} {'S3 BPA Setting':<25} {'PREVENTIVE':<15}")
    print(f"  {'2. Bucket Policy DENY':<25} {'Resource-Based':<25} {'PREVENTIVE':<15}")
    print(f"  {'3. IAM Group Policy':<25} {'Identity-Based':<25} {'PERMISSIVE':<15}")
    print(f"  {'4. Versioning':<25} {'S3 Configuration':<25} {'DETECTIVE':<15}")
    print(f"  {'5. CloudTrail Logging':<25} {'Audit Service':<25} {'DETECTIVE':<15}")
    print(f"  {'6. SSL Enforcement':<25} {'Bucket Policy':<25} {'PREVENTIVE':<15}")
    print("-" * 70)
    print("  PREVENTIVE = blocks the action before it happens")
    print("  DETECTIVE  = records the action for investigation after")
    print("  PERMISSIVE = grants the action when conditions are met")


def generate_audit_report():
    """
    Generate a simple audit report
    Useful for: SOC 2, internal compliance reviews
    """
    iam_client = boto3.client("iam")

    print("\n📋 IAM AUDIT REPORT")
    print("=" * 70)

    groups = [
        "QuickCart-DataEngineering",
        "QuickCart-Finance",
        "QuickCart-Marketing",
        "QuickCart-Interns"
    ]

    for group_name in groups:
        try:
            # Get group members
            group_response = iam_client.get_group(GroupName=group_name)
            users = [u["UserName"] for u in group_response["Users"]]

            # Get attached policies
            policies_response = iam_client.list_attached_group_policies(
                GroupName=group_name
            )
            policies = [p["PolicyName"] for p in policies_response["AttachedPolicies"]]

            print(f"\n  📌 {group_name}")
            print(f"     Members ({len(users)}): {', '.join(users) if users else '(none)'}")
            print(f"     Policies ({len(policies)}): {', '.join(policies) if policies else '(none)'}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                print(f"\n  ⚠️  {group_name}: GROUP NOT FOUND")
            else:
                raise

    print("\n" + "=" * 70)


if __name__ == "__main__":
    print_access_matrix()
    print_policy_enforcement_layers()
    generate_audit_report()