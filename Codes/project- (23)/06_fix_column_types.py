# file: 06_fix_column_types.py
# Crawler often infers STRING for dates and BIGINT for small integers
# This script corrects column types after crawling
# Addresses Pitfall 2: "Column types are all STRING even for numeric data"

import boto3
import copy

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"


def update_column_types(table_name, column_type_overrides):
    """
    Override crawler-inferred column types in Glue Catalog
    
    WHY THIS IS NEEDED:
    → Crawler infers order_date as STRING (from CSV — no embedded type info)
    → Athena treats it as STRING → WHERE order_date > '2025-01-15' does string comparison
    → String comparison: '2025-01-02' > '2025-01-15' is FALSE (correct by accident)
    → But: '2025-1-2' > '2025-01-15' is TRUE (wrong — missing zero-padding)
    → Casting to DATE in catalog = Athena handles dates properly
    
    IMPORTANT: changing type in catalog does NOT change the actual data
    → If actual CSV value cannot be parsed as the new type → Athena query fails
    → Only override types when you are CERTAIN all values are valid
    
    column_type_overrides format:
    {
        "order_date": "date",
        "quantity": "int",
        "is_active": "boolean"
    }
    """
    glue_client = boto3.client("glue", region_name=REGION)

    # Get current table definition
    response = glue_client.get_table(
        DatabaseName=DATABASE_NAME,
        Name=table_name
    )
    table = response["Table"]

    # Deep copy storage descriptor to modify
    updated_sd = copy.deepcopy(table["StorageDescriptor"])

    changes_made = []

    for column in updated_sd["Columns"]:
        col_name = column["Name"]
        if col_name in column_type_overrides:
            old_type = column["Type"]
            new_type = column_type_overrides[col_name]
            if old_type != new_type:
                column["Type"] = new_type
                changes_made.append(f"   {col_name}: {old_type} → {new_type}")

    if not changes_made:
        print(f"ℹ️  No type changes needed for {table_name}")
        return

    # Build update input (must include all required fields)
    table_input = {
        "Name": table_name,
        "StorageDescriptor": updated_sd,
        "TableType": table.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table.get("Parameters", {}),
    }

    # Include partition keys if present
    if table.get("PartitionKeys"):
        table_input["PartitionKeys"] = table["PartitionKeys"]

    # Include SerDe info
    if "SerdeInfo" in updated_sd:
        pass  # Already in StorageDescriptor

    # Update table
    glue_client.update_table(
        DatabaseName=DATABASE_NAME,
        TableInput=table_input
    )

    print(f"✅ Updated column types for: {table_name}")
    for change in changes_made:
        print(change)


def fix_all_table_types():
    """
    Apply type corrections to all crawler-discovered tables
    
    CORRECTIONS BASED ON KNOWN DATA:
    → order_date: string → date (CSVs have YYYY-MM-DD format)
    → signup_date: string → date
    → quantity: bigint → int (never exceeds 2B)
    → stock_quantity: bigint → int
    → is_active: string → string (keep as-is — CSV has "true"/"false" strings)
       → NOTE: Athena CSV SerDe doesn't support boolean natively
       → Keep as string, cast in query: WHERE is_active = 'true'
    """
    print("=" * 60)
    print("🔧 FIXING CRAWLER-INFERRED COLUMN TYPES")
    print("=" * 60)

    # --- raw_orders ---
    update_column_types("raw_orders", {
        "order_date": "date",
        "quantity": "int"
    })

    # --- raw_customers ---
    update_column_types("raw_customers", {
        "signup_date": "date"
    })

    # --- raw_products ---
    update_column_types("raw_products", {
        "stock_quantity": "int"
    })

    # --- curated_orders_snapshot ---
    # Parquet tables usually have correct types already
    # But verify just in case
    print(f"\nℹ️  curated_orders_snapshot: Parquet — types usually correct")
    print(f"   Skipping type override (Parquet embeds schema)")

    print(f"\n{'=' * 60}")
    print("✅ All type corrections applied")
    print("   Re-run Athena queries to verify")


if __name__ == "__main__":
    fix_all_table_types()