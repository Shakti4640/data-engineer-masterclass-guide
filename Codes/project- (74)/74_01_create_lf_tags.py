# file: 74_01_create_lf_tags.py
# Account A (Orders Domain): Create LF-Tag definitions and assign to columns
# This is the FOUNDATION of the entire security model

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
CONSUMER_ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_lf_tags(lf_client):
    """
    Step 1: Define LF-Tag keys and allowed values
    
    Tags are account-level resources — created once, used everywhere
    Tag key + value combinations form the security classification system
    """
    tags_to_create = [
        {
            "TagKey": "sensitivity",
            "TagValues": ["public", "internal", "confidential", "restricted"],
            "description": "Data sensitivity classification (GDPR/CCPA)"
        },
        {
            "TagKey": "domain",
            "TagValues": ["orders", "customers", "products", "analytics", "platform"],
            "description": "Business domain ownership"
        },
        {
            "TagKey": "pii",
            "TagValues": ["true", "false"],
            "description": "Contains personally identifiable information"
        },
        {
            "TagKey": "layer",
            "TagValues": ["bronze", "silver", "gold", "derived"],
            "description": "Data lake layer (medallion architecture)"
        }
    ]

    print("🏷️  Creating LF-Tag definitions:")
    for tag_def in tags_to_create:
        try:
            lf_client.create_lf_tag(
                TagKey=tag_def["TagKey"],
                TagValues=tag_def["TagValues"]
            )
            print(f"   ✅ {tag_def['TagKey']}: {tag_def['TagValues']}")
        except lf_client.exceptions.EntityNotFoundException:
            # Tag exists — update values
            lf_client.update_lf_tag(
                TagKey=tag_def["TagKey"],
                TagValuesToAdd=tag_def["TagValues"]
            )
            print(f"   ℹ️  Updated {tag_def['TagKey']}: {tag_def['TagValues']}")
        except Exception as e:
            if "already exists" in str(e).lower() or "AlreadyExists" in str(e):
                print(f"   ℹ️  {tag_def['TagKey']}: already exists")
            else:
                print(f"   ⚠️  {tag_def['TagKey']}: {e}")


def assign_database_tags(lf_client):
    """
    Step 2: Tag databases with domain and layer
    
    Database-level tags:
    → Used for broad access patterns (all tables in a database)
    → Combined with column-level tags for fine-grained control
    """
    database_tags = [
        {
            "database": "orders_bronze",
            "tags": [
                {"TagKey": "domain", "TagValues": ["orders"]},
                {"TagKey": "layer", "TagValues": ["bronze"]}
            ]
        },
        {
            "database": "orders_silver",
            "tags": [
                {"TagKey": "domain", "TagValues": ["orders"]},
                {"TagKey": "layer", "TagValues": ["silver"]}
            ]
        },
        {
            "database": "orders_gold",
            "tags": [
                {"TagKey": "domain", "TagValues": ["orders"]},
                {"TagKey": "layer", "TagValues": ["gold"]}
            ]
        },
        {
            "database": "customers_gold",
            "tags": [
                {"TagKey": "domain", "TagValues": ["customers"]},
                {"TagKey": "layer", "TagValues": ["gold"]}
            ]
        },
        {
            "database": "analytics_derived",
            "tags": [
                {"TagKey": "domain", "TagValues": ["analytics"]},
                {"TagKey": "layer", "TagValues": ["derived"]}
            ]
        }
    ]

    print("\n📁 Tagging databases:")
    for db_tag in database_tags:
        try:
            lf_client.add_lf_tags_to_resource(
                Resource={
                    "Database": {"Name": db_tag["database"]}
                },
                LFTags=db_tag["tags"]
            )
            tag_str = ", ".join(f"{t['TagKey']}={t['TagValues']}" for t in db_tag["tags"])
            print(f"   ✅ {db_tag['database']}: {tag_str}")
        except Exception as e:
            print(f"   ⚠️  {db_tag['database']}: {e}")


def assign_column_tags(lf_client):
    """
    Step 3: Tag individual columns with sensitivity and PII classification
    
    THIS IS THE CORE OF COLUMN-LEVEL SECURITY
    → Each column gets a sensitivity tag
    → Each column gets a PII tag
    → These tags determine who can see each column
    
    COLUMN CLASSIFICATION GUIDE:
    → public: non-sensitive business data (order_id, status, product_name)
    → internal: business metrics (order_total, revenue, counts)
    → confidential: indirect PII references (customer_id, payment_method)
    → restricted: direct PII (email, phone, IP address, session_id)
    """

    # Define column classifications for ALL Gold tables
    table_column_tags = {
        "orders_gold": {
            "daily_orders": {
                "order_id":        {"sensitivity": "public",       "pii": "false"},
                "order_total":     {"sensitivity": "internal",     "pii": "false"},
                "item_count":      {"sensitivity": "internal",     "pii": "false"},
                "status":          {"sensitivity": "public",       "pii": "false"},
                "payment_method":  {"sensitivity": "confidential", "pii": "false"},
                "shipping_city":   {"sensitivity": "internal",     "pii": "false"},
                "shipping_country":{"sensitivity": "public",       "pii": "false"},
                "customer_id":     {"sensitivity": "confidential", "pii": "true"},
                "order_date":      {"sensitivity": "public",       "pii": "false"},
                "shipped_date":    {"sensitivity": "public",       "pii": "false"},
                "created_at":      {"sensitivity": "internal",     "pii": "false"},
                "updated_at":      {"sensitivity": "internal",     "pii": "false"},
                "_etl_loaded_at":  {"sensitivity": "internal",     "pii": "false"}
            },
            "clickstream_events": {
                "event_id":        {"sensitivity": "public",       "pii": "false"},
                "event_type":      {"sensitivity": "public",       "pii": "false"},
                "user_id":         {"sensitivity": "restricted",   "pii": "true"},
                "session_id":      {"sensitivity": "restricted",   "pii": "true"},
                "event_ts":        {"sensitivity": "internal",     "pii": "false"},
                "event_timestamp": {"sensitivity": "internal",     "pii": "false"},
                "page_url":        {"sensitivity": "internal",     "pii": "false"},
                "product_id":      {"sensitivity": "public",       "pii": "false"},
                "search_query":    {"sensitivity": "internal",     "pii": "false"},
                "device_type":     {"sensitivity": "public",       "pii": "false"},
                "browser":         {"sensitivity": "public",       "pii": "false"},
                "ip_country":      {"sensitivity": "internal",     "pii": "false"},
                "referrer":        {"sensitivity": "internal",     "pii": "false"},
                "cart_value":      {"sensitivity": "internal",     "pii": "false"},
                "items_in_cart":   {"sensitivity": "internal",     "pii": "false"},
                "event_date":      {"sensitivity": "public",       "pii": "false"},
                "event_hour":      {"sensitivity": "public",       "pii": "false"},
                "_processed_at":   {"sensitivity": "internal",     "pii": "false"}
            }
        },
        "customers_gold": {
            "customer_segments": {
                "customer_id":     {"sensitivity": "confidential", "pii": "true"},
                "name":            {"sensitivity": "restricted",   "pii": "true"},
                "email":           {"sensitivity": "restricted",   "pii": "true"},
                "city":            {"sensitivity": "internal",     "pii": "false"},
                "state":           {"sensitivity": "internal",     "pii": "false"},
                "tier":            {"sensitivity": "public",       "pii": "false"},
                "signup_date":     {"sensitivity": "internal",     "pii": "false"}
            }
        }
    }

    print("\n🔖 Tagging columns (sensitivity + PII):")
    total_tagged = 0

    for db_name, tables in table_column_tags.items():
        for table_name, columns in tables.items():
            print(f"\n   📋 {db_name}.{table_name}:")

            for col_name, tags in columns.items():
                try:
                    lf_tags = [
                        {"TagKey": "sensitivity", "TagValues": [tags["sensitivity"]]},
                        {"TagKey": "pii", "TagValues": [tags["pii"]]}
                    ]

                    lf_client.add_lf_tags_to_resource(
                        Resource={
                            "TableWithColumns": {
                                "DatabaseName": db_name,
                                "Name": table_name,
                                "ColumnNames": [col_name]
                            }
                        },
                        LFTags=lf_tags
                    )

                    icon = "🔴" if tags["sensitivity"] == "restricted" else \
                           "🟡" if tags["sensitivity"] == "confidential" else \
                           "🟢" if tags["sensitivity"] == "public" else "🔵"
                    pii_flag = " [PII]" if tags["pii"] == "true" else ""

                    print(f"      {icon} {col_name:<25} "
                          f"sensitivity={tags['sensitivity']:<15}{pii_flag}")
                    total_tagged += 1

                except Exception as e:
                    print(f"      ⚠️  {col_name}: {e}")

    print(f"\n   📊 Total columns tagged: {total_tagged}")
    return table_column_tags


def verify_tag_assignments(lf_client):
    """Verify all tags are correctly assigned"""
    print(f"\n{'=' * 70}")
    print("🔍 VERIFYING TAG ASSIGNMENTS")
    print(f"{'=' * 70}")

    # Check a specific table
    for db_name, table_name in [
        ("orders_gold", "daily_orders"),
        ("orders_gold", "clickstream_events")
    ]:
        try:
            response = lf_client.get_resource_lf_tags(
                Resource={
                    "Table": {
                        "DatabaseName": db_name,
                        "Name": table_name
                    }
                },
                ShowAssignedLFTags=True
            )

            print(f"\n   📋 {db_name}.{table_name}:")

            # Database tags
            for tag in response.get("LFTagOnDatabase", []):
                print(f"      DB tag: {tag['TagKey']}={tag['TagValues']}")

            # Table tags
            for tag in response.get("LFTagsOnTable", []):
                print(f"      Table tag: {tag['TagKey']}={tag['TagValues']}")

            # Column tags
            for col_tag in response.get("LFTagsOnColumns", []):
                col_name = col_tag["Name"]
                tags = col_tag["LFTags"]
                tag_str = ", ".join(f"{t['TagKey']}={t['TagValues']}" for t in tags)
                print(f"      Column: {col_name:<25} → {tag_str}")

        except Exception as e:
            print(f"   ⚠️  {db_name}.{table_name}: {e}")


def main():
    print("=" * 70)
    print("🏷️  LF-TAG CREATION & COLUMN CLASSIFICATION")
    print("   Foundation of column-level security")
    print("=" * 70)

    lf_client = boto3.client("lakeformation", region_name=REGION)

    # Step 1: Create tag definitions
    print("\n--- Step 1: Create LF-Tag Definitions ---")
    create_lf_tags(lf_client)

    # Step 2: Tag databases
    print("\n--- Step 2: Tag Databases ---")
    assign_database_tags(lf_client)

    # Step 3: Tag columns
    print("\n--- Step 3: Tag Columns ---")
    assign_column_tags(lf_client)

    # Step 4: Verify
    verify_tag_assignments(lf_client)

    print("\n" + "=" * 70)
    print("✅ TAG CLASSIFICATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()