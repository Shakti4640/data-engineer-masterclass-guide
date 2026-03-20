# file: 73_02_lake_formation_setup.py
# Configure Lake Formation as the SINGLE permission layer
# Replaces scattered S3 bucket policies + IAM for data access

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
CONSUMER_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# IAM roles that need data access
ROLES = {
    "etl": f"arn:aws:iam::{ACCOUNT_ID}:role/GlueETLRole",
    "analyst": f"arn:aws:iam::{ACCOUNT_ID}:role/AnalystRole",
    "data_scientist": f"arn:aws:iam::{ACCOUNT_ID}:role/DataScientistRole",
    "bi_redshift": f"arn:aws:iam::{ACCOUNT_ID}:role/RedshiftSpectrumRole",
    "cross_account_analytics": f"arn:aws:iam::{CONSUMER_ACCOUNT_ID}:role/MeshDataConsumerRole"
}

# S3 locations that Lake Formation will govern
S3_LOCATIONS = [
    f"s3://orders-data-prod/bronze/",
    f"s3://orders-data-prod/silver/",
    f"s3://orders-data-prod/gold/",
    f"s3://analytics-data-prod/derived/"
]


def register_lake_formation_admin(lf_client):
    """
    Step 1: Register the Lake Formation administrator
    
    CRITICAL FIRST STEP:
    → Without an admin, Lake Formation cannot manage permissions
    → Admin can grant/revoke permissions on any table
    → Typically: a dedicated "PlatformAdmin" IAM role
    """
    admin_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/LakeFormationAdmin"

    try:
        lf_client.put_data_lake_settings(
            DataLakeSettings={
                "DataLakeAdmins": [
                    {"DataL[REDACTED:AWS_ACCESS_KEY]": admin_arn}
                ],
                "CreateDatabaseDefaultPermissions": [],
                "CreateTableDefaultPermissions": [],
                # CRITICAL: Setting empty defaults means NEW tables
                # do NOT auto-grant to IAMAllowedPrincipals
                # This activates Lake Formation governance
                "Parameters": {
                    "CROSS_ACCOUNT_VERSION": "3"
                    # Version 3 enables cross-account grants
                    # without requiring RAM shares for same-org accounts
                }
            }
        )
        print(f"✅ Lake Formation admin registered: {admin_arn}")
        print(f"   Default permissions: EMPTY (Lake Formation governs all new tables)")
    except Exception as e:
        print(f"⚠️  Admin setup: {e}")


def remove_iam_allowed_principals(lf_client, glue_client):
    """
    Step 2: Remove IAMAllowedPrincipals from ALL existing tables
    
    WHY THIS IS CRITICAL:
    → By default, Lake Formation grants Super permission to IAMAllowedPrincipals
    → This means ANYONE with IAM S3 access can bypass Lake Formation
    → Effectively makes Lake Formation a no-op
    → MUST remove this to activate real governance
    
    WARNING: This WILL break existing access patterns
    → All access must go through Lake Formation grants after this
    → Plan migration carefully — don't do this in production without testing
    """
    databases = glue_client.get_databases()

    print("\n🔒 Removing IAMAllowedPrincipals from all databases:")
    for db in databases["DatabaseList"]:
        db_name = db["Name"]
        if db_name == "default":
            continue

        try:
            # Revoke database-level Super from IAMAllowedPrincipals
            lf_client.revoke_permissions(
                Principal={
                    "Data[REDACTED:AWS_ACCESS_KEY]r": "IAM_ALLOWED_PRINCIPALS"
                },
                Resource={
                    "Database": {
                        "Name": db_name
                    }
                },
                Permissions=["ALL"],
                PermissionsWithGrantOption=["ALL"]
            )
            print(f"   ✅ Revoked from database: {db_name}")
        except lf_client.exceptions.InvalidInputException:
            print(f"   ℹ️  Already revoked from: {db_name}")
        except Exception as e:
            print(f"   ⚠️  {db_name}: {e}")

        # Revoke from all tables in this database
        tables = glue_client.get_tables(DatabaseName=db_name)
        for table in tables.get("TableList", []):
            try:
                lf_client.revoke_permissions(
                    Principal={
                        "DataL[REDACTED:AWS_ACCESS_KEY]": "IAM_ALLOWED_PRINCIPALS"
                    },
                    Resource={
                        "Table": {
                            "DatabaseName": db_name,
                            "Name": table["Name"]
                        }
                    },
                    Permissions=["ALL"],
                    PermissionsWithGrantOption=["ALL"]
                )
                print(f"      ✅ Revoked from table: {db_name}.{table['Name']}")
            except lf_client.exceptions.InvalidInputException:
                pass
            except Exception as e:
                print(f"      ⚠️  {db_name}.{table['Name']}: {e}")


def register_s3_locations(lf_client):
    """
    Step 3: Register S3 locations with Lake Formation
    
    Lake Formation needs to know WHICH S3 paths it governs
    → Registered locations: Lake Formation vends temporary S3 credentials
    → Unregistered locations: IAM-based S3 access (old way)
    → Must register ALL data lake paths
    """
    # Lake Formation needs a service-linked role to access S3
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/aws-service-role/lakeformation.amazonaws.com/AWSServiceRoleForL[REDACTED:AWS_ACCESS_KEY]"

    print("\n📍 Registering S3 locations with Lake Formation:")
    for location in S3_LOCATIONS:
        try:
            lf_client.register_resource(
                ResourceArn=location,
                UseServiceLinkedRole=True
            )
            print(f"   ✅ Registered: {location}")
        except lf_client.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Already registered: {location}")
        except Exception as e:
            print(f"   ⚠️  {location}: {e}")


def grant_etl_permissions(lf_client):
    """
    Step 4: Grant permissions to ETL role
    
    ETL role needs:
    → ALL on bronze (read + write — raw ingestion)
    → ALL on silver (read + write — transformation)
    → ALL on gold (read + write — curated output)
    → CREATE_TABLE on all databases
    """
    etl_role = ROLES["etl"]

    grants = [
        # Bronze: full access for raw ingestion
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Database": {"Name": "orders_bronze"}
            },
            "Permissions": ["ALL", "CREATE_TABLE", "ALTER", "DROP"],
            "PermissionsWithGrantOption": []
        },
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Table": {
                    "DatabaseName": "orders_bronze",
                    "TableWildcard": {}  # ALL tables in database
                }
            },
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": []
        },
        # Silver: full access for transformation
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Database": {"Name": "orders_silver"}
            },
            "Permissions": ["ALL", "CREATE_TABLE", "ALTER", "DROP"],
            "PermissionsWithGrantOption": []
        },
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Table": {
                    "DatabaseName": "orders_silver",
                    "TableWildcard": {}
                }
            },
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": []
        },
        # Gold: full access for curated output
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Database": {"Name": "orders_gold"}
            },
            "Permissions": ["ALL", "CREATE_TABLE", "ALTER", "DROP"],
            "PermissionsWithGrantOption": []
        },
        {
            "Principal": {"Data[REDACTED:AWS_ACCESS_KEY]r": etl_role},
            "Resource": {
                "Table": {
                    "DatabaseName": "orders_gold",
                    "TableWildcard": {}
                }
            },
            "Permissions": ["ALL"],
            "PermissionsWithGrantOption": []
        }
    ]

    print("\n🔐 Granting ETL role permissions:")
    for grant in grants:
        try:
            lf_client.grant_permissions(**grant)
            resource = grant["Resource"]
            if "Database" in resource:
                target = f"database: {resource['Database']['Name']}"
            else:
                target = f"tables: {resource['Table']['DatabaseName']}.*"
            print(f"   ✅ {grant['Permissions']} on {target}")
        except lf_client.exceptions.InvalidInputException as e:
            if "already exists" in str(e).lower():
                pass
            else:
                print(f"   ⚠️  {e}")


def grant_analyst_permissions(lf_client):
    """
    Step 5: Grant permissions to Analyst role
    
    Analysts can:
    → SELECT on Gold tables (all columns except PII)
    → SELECT on analytics_derived tables
    → CANNOT access Bronze or Silver
    → CANNOT write anything
    
    COLUMN-LEVEL FILTERING:
    → daily_orders: EXCLUDE customer_id (PII reference)
    → clickstream_events: EXCLUDE user_id, session_id (PII)
    → This is the power of Lake Formation over IAM
    """
    analyst_role = ROLES["analyst"]

    print("\n🔐 Granting Analyst role permissions:")

    # Gold database: DESCRIBE only (can see it exists)
    lf_client.grant_permissions(
        Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": analyst_role},
        Resource={"Database": {"Name": "orders_gold"}},
        Permissions=["DESCRIBE"],
        PermissionsWithGrantOption=[]
    )
    print(f"   ✅ DESCRIBE on database: orders_gold")

    # daily_orders: SELECT with column filter (exclude PII)
    try:
        lf_client.grant_permissions(
            Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": analyst_role},
            Resource={
                "TableWithColumns": {
                    "DatabaseName": "orders_gold",
                    "Name": "daily_orders",
                    "ColumnWildcard": {
                        "ExcludedColumnNames": [
                            "customer_id"  # PII — analysts can't see this
                        ]
                    }
                }
            },
            Permissions=["SELECT"],
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ SELECT on orders_gold.daily_orders (EXCLUDING customer_id)")
    except Exception as e:
        print(f"   ⚠️  daily_orders grant: {e}")

    # clickstream_events: SELECT with column filter
    try:
        lf_client.grant_permissions(
            Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": analyst_role},
            Resource={
                "TableWithColumns": {
                    "DatabaseName": "orders_gold",
                    "Name": "clickstream_events",
                    "ColumnWildcard": {
                        "ExcludedColumnNames": [
                            "user_id",     # PII
                            "session_id"   # PII-adjacent
                        ]
                    }
                }
            },
            Permissions=["SELECT"],
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ SELECT on orders_gold.clickstream_events (EXCLUDING user_id, session_id)")
    except Exception as e:
        print(f"   ⚠️  clickstream_events grant: {e}")

    # analytics_derived: full SELECT (no PII in derived tables)
    try:
        lf_client.grant_permissions(
            Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": analyst_role},
            Resource={
                "Database": {"Name": "analytics_derived"}
            },
            Permissions=["DESCRIBE"],
            PermissionsWithGrantOption=[]
        )
        lf_client.grant_permissions(
            Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": analyst_role},
            Resource={
                "Table": {
                    "DatabaseName": "analytics_derived",
                    "TableWildcard": {}
                }
            },
            Permissions=["SELECT"],
            PermissionsWithGrantOption=[]
        )
        print(f"   ✅ SELECT on analytics_derived.* (all columns)")
    except Exception as e:
        print(f"   ⚠️  analytics_derived grant: {e}")


def grant_redshift_spectrum_permissions(lf_client):
    """
    Step 6: Grant Redshift Spectrum role access to Gold tables
    
    Spectrum needs SELECT to query S3 from Redshift
    → Same grants as analyst, but for the Redshift IAM role
    → Spectrum reads Glue Catalog → Lake Formation checks grants
    → If granted → Lake Formation vends S3 temp credentials
    """
    spectrum_role = ROLES["bi_redshift"]

    print("\n🔐 Granting Redshift Spectrum role permissions:")

    # Gold databases
    for db_name in ["orders_gold", "customers_gold", "products_gold"]:
        try:
            lf_client.grant_permissions(
                Principal={"Data[REDACTED:AWS_ACCESS_KEY]r": spectrum_role},
                Resource={"Database": {"Name": db_name}},
                Permissions=["DESCRIBE"],
                PermissionsWithGrantOption=[]
            )
            lf_client.grant_permissions(
                Principal={"Data[REDACTED:AWS_ACCESS_KEY]r": spectrum_role},
                Resource={
                    "Table": {
                        "DatabaseName": db_name,
                        "TableWildcard": {}
                    }
                },
                Permissions=["SELECT"],
                PermissionsWithGrantOption=[]
            )
            print(f"   ✅ SELECT on {db_name}.* (Spectrum)")
        except Exception as e:
            print(f"   ⚠️  {db_name}: {e}")


def grant_cross_account_permissions(lf_client):
    """
    Step 7: Grant cross-account access to Analytics domain (Account C)
    
    Lake Formation cross-account flow (Project 71 + 74):
    1. Producer (Account A) grants via Lake Formation
    2. Lake Formation creates RAM share automatically (v3)
    3. Consumer (Account C) accepts and creates resource link
    4. Consumer queries through resource link → Lake Formation checks grant
    
    This REPLACES the manual bucket policy + IAM role chain from Project 56
    """
    consumer_role = ROLES["cross_account_analytics"]

    print("\n🔐 Granting cross-account permissions (Analytics domain):")

    for db_name in ["orders_gold"]:
        try:
            # Grant database access
            lf_client.grant_permissions(
                Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
                Resource={"Database": {"Name": db_name}},
                Permissions=["DESCRIBE"],
                PermissionsWithGrantOption=["DESCRIBE"]
                # PermissionsWithGrantOption allows consumer's LF admin
                # to re-grant to specific roles within their account
            )

            # Grant table access
            lf_client.grant_permissions(
                Principal={"Dat[REDACTED:AWS_ACCESS_KEY]er": CONSUMER_ACCOUNT_ID},
                Resource={
                    "Table": {
                        "DatabaseName": db_name,
                        "TableWildcard": {}
                    }
                },
                Permissions=["SELECT"],
                PermissionsWithGrantOption=["SELECT"]
            )
            print(f"   ✅ SELECT on {db_name}.* → Account {CONSUMER_ACCOUNT_ID}")
        except Exception as e:
            print(f"   ⚠️  Cross-account {db_name}: {e}")


def audit_all_permissions(lf_client):
    """Print complete permission inventory for audit"""
    print(f"\n{'=' * 70}")
    print("📋 LAKE FORMATION PERMISSION AUDIT")
    print(f"{'=' * 70}")

    # List all grants
    paginator = lf_client.get_paginator("list_permissions")

    grants_by_principal = {}
    for page in paginator.paginate():
        for perm in page["PrincipalResourcePermissions"]:
            principal = perm["Principal"]["Da[REDACTED:AWS_ACCESS_KEY]ier"]
            if principal not in grants_by_principal:
                grants_by_principal[principal] = []
            grants_by_principal[principal].append(perm)

    for principal, grants in sorted(grants_by_principal.items()):
        # Shorten principal for display
        short_name = principal.split("/")[-1] if "/" in principal else principal
        print(f"\n   👤 {short_name}")

        for grant in grants:
            resource = grant["Resource"]
            permissions = grant["Permissions"]

            if "Database" in resource:
                target = f"DB: {resource['Database']['Name']}"
            elif "Table" in resource:
                db = resource["Table"]["DatabaseName"]
                tbl = resource["Table"].get("Name", "*")
                target = f"Table: {db}.{tbl}"
            elif "TableWithColumns" in resource:
                db = resource["TableWithColumns"]["DatabaseName"]
                tbl = resource["TableWithColumns"]["Name"]
                excluded = resource["TableWithColumns"].get("ColumnWildcard", {}).get("ExcludedColumnNames", [])
                target = f"Table: {db}.{tbl} (excl: {excluded})"
            else:
                target = str(resource)

            print(f"      → {', '.join(permissions):20s} on {target}")

    print(f"\n{'=' * 70}")


def main():
    print("=" * 70)
    print("🔒 LAKE FORMATION GOVERNANCE SETUP")
    print("   Single permission layer for the entire platform")
    print("=" * 70)

    lf_client = boto3.client("lakeformation", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)

    # Step 1: Register admin
    print("\n--- Step 1: Lake Formation Admin ---")
    register_lake_formation_admin(lf_client)

    # Step 2: Remove IAMAllowedPrincipals
    print("\n--- Step 2: Remove Default Bypass ---")
    remove_iam_allowed_principals(lf_client, glue_client)

    # Step 3: Register S3 locations
    print("\n--- Step 3: Register S3 Locations ---")
    register_s3_locations(lf_client)

    # Step 4: ETL permissions
    print("\n--- Step 4: ETL Role Grants ---")
    grant_etl_permissions(lf_client)

    # Step 5: Analyst permissions
    print("\n--- Step 5: Analyst Role Grants ---")
    grant_analyst_permissions(lf_client)

    # Step 6: Redshift Spectrum permissions
    print("\n--- Step 6: Spectrum Role Grants ---")
    grant_redshift_spectrum_permissions(lf_client)

    # Step 7: Cross-account permissions
    print("\n--- Step 7: Cross-Account Grants ---")
    grant_cross_account_permissions(lf_client)

    # Audit
    audit_all_permissions(lf_client)

    print("\n✅ LAKE FORMATION GOVERNANCE COMPLETE")


if __name__ == "__main__":
    main()