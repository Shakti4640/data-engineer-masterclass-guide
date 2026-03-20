# file: 74_03_consumer_accept_tags.py
# Account C (Analytics Domain): Accept shared tags, create resource links,
# grant to local roles using inherited tags

import boto3
import json

REGION = "us-east-2"
CONSUMER_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
PRODUCER_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Local roles in consumer account
LOCAL_ROLES = {
    "analyst": f"arn:aws:iam::{CONSUMER_ACCOUNT_ID}:role/AnalystRole",
    "data_scientist": f"arn:aws:iam::{CONSUMER_ACCOUNT_ID}:role/DataScientistRole",
    "spectrum": f"arn:aws:iam::{CONSUMER_ACCOUNT_ID}:role/RedshiftSpectrumRole"
}


def accept_ram_shares(ram_client):
    """
    Step 1: Accept AWS RAM shares from producer account
    
    When Account A grants LF-Tags cross-account:
    → Lake Formation automatically creates RAM shares
    → Consumer account must ACCEPT these shares
    → If accounts are in same AWS Organization with auto-accept: skip this
    → Otherwise: must explicitly accept each pending share
    """
    print("📬 Checking for pending RAM shares:")

    try:
        paginator = ram_client.get_paginator("get_resource_share_invitations")
        pending_count = 0

        for page in paginator.paginate():
            for invitation in page.get("resourceShareInvitations", []):
                if invitation["status"] == "PENDING":
                    share_name = invitation.get("resourceShareName", "unnamed")
                    share_arn = invitation["resourceShareInvitationArn"]
                    sender = invitation.get("senderAccountId", "unknown")

                    print(f"   📩 Pending: {share_name} from Account {sender}")

                    # Accept the share
                    ram_client.accept_resource_share_invitation(
                        resourceShareInvitationArn=share_arn
                    )
                    print(f"   ✅ Accepted: {share_name}")
                    pending_count += 1

        if pending_count == 0:
            print("   ℹ️  No pending shares (may be auto-accepted via AWS Organizations)")

    except Exception as e:
        print(f"   ⚠️  RAM check: {e}")
        print(f"   (If using AWS Organizations with auto-accept, this is expected)")


def create_resource_link_databases(glue_client):
    """
    Step 2: Create resource link databases pointing to producer's catalog
    
    Resource links are LOCAL database entries that REFERENCE
    databases in another account. They're the consumer's
    "window" into the producer's catalog.
    
    When consumer queries a resource link table:
    → Glue resolves to producer's catalog
    → Lake Formation in producer evaluates tags
    → Returns filtered schema based on consumer's tag grants
    → Consumer sees only columns matching their sensitivity level
    """
    resource_links = [
        {
            "local_name": "orders_gold_link",
            "target_database": "orders_gold",
            "target_account": PRODUCER_ACCOUNT_ID,
            "description": "Resource link to Orders domain Gold layer in Account A"
        },
        {
            "local_name": "customers_gold_link",
            "target_database": "customers_gold",
            "target_account": PRODUCER_ACCOUNT_ID,
            "description": "Resource link to Customers domain Gold layer in Account A"
        }
    ]

    print("\n🔗 Creating resource link databases:")
    for link in resource_links:
        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": link["local_name"],
                    "Description": link["description"],
                    "TargetDatabase": {
                        "CatalogId": link["target_account"],
                        "DatabaseName": link["target_database"]
                    },
                    "Parameters": {
                        "mesh_type": "resource_link",
                        "source_account": link["target_account"],
                        "source_database": link["target_database"]
                    }
                }
            )
            print(f"   ✅ {link['local_name']} → "
                  f"Account {link['target_account']}:{link['target_database']}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  {link['local_name']}: already exists")
        except Exception as e:
            print(f"   ⚠️  {link['local_name']}: {e}")


def grant_local_roles_via_tags(lf_client):
    """
    Step 3: Grant local roles access using shared tags
    
    IMPORTANT FLOW:
    → Producer shared tags WITH grant option to this account
    → This account's LF admin can now RE-GRANT to local roles
    → Local roles inherit the tag-based column filtering
    → Analyst in Account C sees same filtered view as Analyst in Account A
    
    WHAT HAPPENS:
    → Local AnalystRole gets: sensitivity IN (public, internal)
    → When analyst queries orders_gold_link.daily_orders:
      → LF resolves to Account A's orders_gold.daily_orders
      → LF checks tags: which columns have sensitivity=public or internal?
      → Returns only those columns
      → Analyst sees: order_id, order_total, status, shipping_city, dates
      → Analyst CANNOT see: customer_id, payment_method, email, ip_address
    """
    print("\n🔐 Granting local roles via shared tags:")

    # Analyst: public + internal
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": LOCAL_ROLES["analyst"]},
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
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ AnalystRole: SELECT WHERE sensitivity IN (public, internal)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  AnalystRole: grant already exists")
        else:
            print(f"   ⚠️  AnalystRole: {e}")

    # Data Scientist: public + internal + confidential
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": LOCAL_ROLES["data_scientist"]},
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
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ DataScientistRole: SELECT WHERE sensitivity IN (public, internal, confidential)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  DataScientistRole: grant already exists")
        else:
            print(f"   ⚠️  DataScientistRole: {e}")

    # Spectrum (BI): public + internal
    try:
        lf_client.grant_permissions(
            Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": LOCAL_ROLES["spectrum"]},
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
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ SpectrumRole: SELECT WHERE sensitivity IN (public, internal)")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"   ℹ️  SpectrumRole: grant already exists")
        else:
            print(f"   ⚠️  SpectrumRole: {e}")

    # Database-level DESCRIBE for resource links
    for role_name, role_arn in LOCAL_ROLES.items():
        try:
            lf_client.grant_permissions(
                Principal={"Da[REDACTED:AWS_ACCESS_KEY]er": role_arn},
                Resource={
                    "LFTagPolicy": {
                        "ResourceType": "DATABASE",
                        "Expression": [
                            {"TagKey": "layer", "TagValues": ["gold"]}
                        ]
                    }
                },
                Permissions=["DESCRIBE"],
                PermissionsWithGrantOption=[]
            )
        except Exception:
            pass  # Already exists


def main():
    print("=" * 70)
    print("🔗 CONSUMER ACCOUNT: Accept Tags + Create Resource Links")
    print(f"   Account: {CONSUMER_ACCOUNT_ID}")
    print(f"   Producer: {PRODUCER_ACCOUNT_ID}")
    print("=" * 70)

    ram_client = boto3.client("ram", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)
    lf_client = boto3.client("lakeformation", region_name=REGION)

    # Step 1: Accept RAM shares
    print("\n--- Step 1: Accept RAM Shares ---")
    accept_ram_shares(ram_client)

    # Step 2: Create resource links
    print("\n--- Step 2: Resource Link Databases ---")
    create_resource_link_databases(glue_client)

    # Step 3: Grant local roles
    print("\n--- Step 3: Local Role Grants ---")
    grant_local_roles_via_tags(lf_client)

    print("\n" + "=" * 70)
    print("✅ CONSUMER TAG SETUP COMPLETE")
    print("   Local roles can now query producer data with column filtering")
    print("=" * 70)


if __name__ == "__main__":
    main()