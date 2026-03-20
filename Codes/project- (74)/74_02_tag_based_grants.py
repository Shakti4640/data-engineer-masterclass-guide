# file: 74_02_tag_based_grants.py
# Create LF-Tag-based permission grants for each role
# One grant expression covers ALL tables — current and future

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
CONSUMER_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Roles to grant access to
ROLES = {
    "analyst": f"arn:aws:iam::{ACCOUNT_ID}:role/AnalystRole",
    "data_scientist": f"arn:aws:iam::{ACCOUNT_ID}:role/DataScientistRole",
    "etl": f"arn:aws:iam::{ACCOUNT_ID}:role/GlueETLRole",
    "security": f"arn:aws:iam::{ACCOUNT_ID}:role/SecurityAuditRole",
    "spectrum": f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftSpectrumRole"
}


def grant_tag_permissions_to_role(lf_client, role_name, role_arn, sensitivity_levels,
                                   permissions, with_grant=False):
    """
    Grant tag-based permissions to a specific role
    
    HOW LF-TAG GRANTS WORK:
    → You don't grant on a specific table
    → You grant on a TAG EXPRESSION
    → Any resource (database, table, column) matching the expression → accessible
    → New resources with matching tags → automatically accessible
    
    GRANT structure:
    → Principal: IAM role ARN
    → Resource: LF-Tag expression (key=value pairs)
    → Permissions: SELECT, INSERT, DELETE, DESCRIBE, ALTER, etc.
    """
    tag_expression = [
        {"TagKey": "sensitivity", "TagValues": sensitivity_levels}
    ]

    grant_option = permissions if with_grant else []

    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": role_arn},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "TABLE",
                    "Expression": tag_expression
                }
            },
            Permissions=permissions,
            PermissionsWithGrantOption=grant_option
        )
        levels_str = ", ".join(sensitivity_levels)
        perms_str = ", ".join(permissions)
        print(f"   ✅ {role_name}: {perms_str} WHERE sensitivity IN ({levels_str})")

    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  {role_name}: grant already exists")
        else:
            print(f"   ⚠️  {role_name}: {e}")

    # Also grant on DATABASE level for the same tags
    # Required for Athena to list databases
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": role_arn},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "DATABASE",
                    "Expression": [
                        {"TagKey": "layer", "TagValues": ["gold", "derived"]}
                    ]
                }
            },
            Permissions=["DESCRIBE"],
            PermissionsWithGrantOption=[]
        )
    except Exception:
        pass  # Already exists or not critical


def setup_local_role_grants(lf_client):
    """
    Grant tag-based permissions to local (same-account) roles
    
    ACCESS MATRIX:
    ┌─────────────────┬──────────────────────────────────────────────┐
    │ Role            │ sensitivity levels → columns visible         │
    ├─────────────────┼──────────────────────────────────────────────┤
    │ Analyst         │ public, internal → order_id, status,        │
    │                 │   order_total, shipping_city, dates          │
    ├─────────────────┼──────────────────────────────────────────────┤
    │ Data Scientist  │ public, internal, confidential              │
    │                 │   → above + customer_id, payment_method     │
    ├─────────────────┼──────────────────────────────────────────────┤
    │ ETL Role        │ ALL levels → ALL columns (read + write)     │
    ├─────────────────┼──────────────────────────────────────────────┤
    │ Security Audit  │ ALL levels → ALL columns (read only)        │
    ├─────────────────┼──────────────────────────────────────────────┤
    │ Spectrum        │ public, internal → same as Analyst           │
    │ (BI Dashboard)  │ (BI doesn't need PII)                       │
    └─────────────────┴──────────────────────────────────────────────┘
    """
    print("🔐 Granting tag-based permissions to local roles:")

    # Analyst: public + internal only
    grant_tag_permissions_to_role(
        lf_client,
        role_name="analyst",
        role_arn=ROLES["analyst"],
        sensitivity_levels=["public", "internal"],
        permissions=["SELECT", "DESCRIBE"]
    )

    # Data Scientist: public + internal + confidential
    grant_tag_permissions_to_role(
        lf_client,
        role_name="data_scientist",
        role_arn=ROLES["data_scientist"],
        sensitivity_levels=["public", "internal", "confidential"],
        permissions=["SELECT", "DESCRIBE"]
    )

    # ETL: ALL levels, ALL permissions
    grant_tag_permissions_to_role(
        lf_client,
        role_name="etl",
        role_arn=ROLES["etl"],
        sensitivity_levels=["public", "internal", "confidential", "restricted"],
        permissions=["ALL"]
    )

    # Security Audit: ALL levels, read only
    grant_tag_permissions_to_role(
        lf_client,
        role_name="security",
        role_arn=ROLES["security"],
        sensitivity_levels=["public", "internal", "confidential", "restricted"],
        permissions=["SELECT", "DESCRIBE"]
    )

    # Spectrum (BI): public + internal (same as analyst)
    grant_tag_permissions_to_role(
        lf_client,
        role_name="spectrum",
        role_arn=ROLES["spectrum"],
        sensitivity_levels=["public", "internal"],
        permissions=["SELECT", "DESCRIBE"]
    )


def setup_cross_account_tag_sharing(lf_client):
    """
    Share LF-Tags and tag-based grants to consumer account
    
    CROSS-ACCOUNT LF-TAG FLOW:
    1. Share tag DEFINITIONS to consumer account
    2. Grant tag-based SELECT to consumer account
    3. Consumer creates resource link
    4. Consumer re-grants to local roles using shared tags
    
    Step 1 & 2 happen HERE (in producer account)
    Step 3 & 4 happen in consumer account (74_03)
    """
    print("\n🌐 Setting up cross-account tag sharing:")

    # Step 1: Grant TAG ASSOCIATION to consumer account
    # This allows the consumer to SEE the tag definitions
    tags_to_share = ["sensitivity", "pii", "domain", "layer"]

    for tag_key in tags_to_share:
        try:
            # Get current tag values
            tag_info = lf_client.get_lf_tag(TagKey=tag_key)
            tag_values = tag_info["TagValues"]

            lf_client.grant_permissions(
                Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
                Resource={
                    "LFTag": {
                        "TagKey": tag_key,
                        "TagValues": tag_values
                    }
                },
                Permissions=["DESCRIBE", "ASSOCIATE"],
                PermissionsWithGrantOption=["DESCRIBE", "ASSOCIATE"]
            )
            print(f"   ✅ Tag shared: {tag_key} → Account {CONSUMER_ACCOUNT_ID}")

        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"   ℹ️  Tag already shared: {tag_key}")
            else:
                print(f"   ⚠️  Tag share {tag_key}: {e}")

    # Step 2: Grant tag-based SELECT to consumer account
    # Consumer analysts see: public + internal
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "TABLE",
                    "Expression": [
                        {"TagKey": "sensitivity",
                         "TagValues": ["public", "internal"]}
                    ]
                }
            },
            Permissions=["SELECT", "DESCRIBE"],
            PermissionsWithGrantOption=["SELECT", "DESCRIBE"]
            # PermissionsWithGrantOption: consumer LF admin can re-grant
            # to specific roles within their account
        )
        print(f"   ✅ Cross-account TABLE grant: sensitivity IN (public, internal) → Account {CONSUMER_ACCOUNT_ID}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Cross-account TABLE grant already exists")
        else:
            print(f"   ⚠️  Cross-account TABLE grant: {e}")

    # Database-level grant for consumer
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "DATABASE",
                    "Expression": [
                        {"TagKey": "layer", "TagValues": ["gold"]}
                    ]
                }
            },
            Permissions=["DESCRIBE"],
            PermissionsWithGrantOption=["DESCRIBE"]
        )
        print(f"   ✅ Cross-account DATABASE grant: layer=gold → Account {CONSUMER_ACCOUNT_ID}")
    except Exception as e:
        if "already exists" in str(e).lower():
            pass
        else:
            print(f"   ⚠️  Cross-account DATABASE grant: {e}")

    # Consumer data scientists: public + internal + confidential
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
            Resource={
                "LFTagPolicy": {
                    "ResourceType": "TABLE",
                    "Expression": [
                        {"TagKey": "sensitivity",
                         "TagValues": ["public", "internal", "confidential"]}
                    ]
                }
            },
            Permissions=["SELECT", "DESCRIBE"],
            PermissionsWithGrantOption=["SELECT", "DESCRIBE"]
        )
        print(f"   ✅ Cross-account DS grant: sensitivity IN (public, internal, confidential) → Account {CONSUMER_ACCOUNT_ID}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  Cross-account DS grant already exists")
        else:
            print(f"   ⚠️  Cross-account DS grant: {e}")


def audit_all_tag_grants(lf_client):
    """Print complete audit of all tag-based grants"""
    print(f"\n{'=' * 70}")
    print("📋 COMPLETE TAG-BASED GRANT AUDIT")
    print(f"{'=' * 70}")

    paginator = lf_client.get_paginator("list_permissions")

    tag_grants = []
    named_grants = []

    for page in paginator.paginate():
        for perm in page["PrincipalResourcePermissions"]:
            resource = perm["Resource"]
            if "LFTagPolicy" in resource:
                tag_grants.append(perm)
            else:
                named_grants.append(perm)

    # Tag-based grants
    print(f"\n   🏷️  TAG-BASED GRANTS ({len(tag_grants)}):")
    for grant in tag_grants:
        principal = grant["Principal"]["Da[REDACTED:AWS_ACCESS_KEY]er"]
        short_principal = principal.split("/")[-1] if "/" in principal else principal[-12:]
        permissions = grant["Permissions"]
        resource = grant["Resource"]["LFTagPolicy"]
        resource_type = resource["ResourceType"]
        expression = resource["Expression"]

        expr_str = " AND ".join(
            f"{e['TagKey']} IN ({','.join(e['TagValues'])})"
            for e in expression
        )

        print(f"      {short_principal:<30} {','.join(permissions):<20} "
              f"on {resource_type} WHERE {expr_str}")

    # Named resource grants (should be minimal after migration)
    if named_grants:
        print(f"\n   📌 NAMED RESOURCE GRANTS ({len(named_grants)}) — consider migrating to tags:")
        for grant in named_grants[:10]:
            principal = grant["Principal"]["Da[REDACTED:AWS_ACCESS_KEY]er"]
            short = principal.split("/")[-1] if "/" in principal else principal[-12:]
            print(f"      {short}: {grant['Permissions']}")

    print(f"\n   📊 Summary: {len(tag_grants)} tag-based, {len(named_grants)} named-resource")
    print(f"{'=' * 70}")


def main():
    print("=" * 70)
    print("🔐 TAG-BASED PERMISSION GRANTS")
    print("   One expression per role — covers ALL tables automatically")
    print("=" * 70)

    lf_client = boto3.client("lakeformation", region_name=REGION)

    # Step 1: Local role grants
    print("\n--- Step 1: Local Role Grants ---")
    setup_local_role_grants(lf_client)

    # Step 2: Cross-account sharing
    print("\n--- Step 2: Cross-Account Tag Sharing ---")
    setup_cross_account_tag_sharing(lf_client)

    # Step 3: Audit
    audit_all_tag_grants(lf_client)

    print("\n✅ TAG-BASED GRANTS COMPLETE")


if __name__ == "__main__":
    main()