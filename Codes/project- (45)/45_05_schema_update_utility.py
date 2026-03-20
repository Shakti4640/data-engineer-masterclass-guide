# file: 45_05_schema_update_utility.py
# Purpose: Data engineer uses this to update schema registry after reviewing drift
# Called AFTER receiving a drift alert and confirming the change is permanent

import boto3
import json
from schema_registry import get_active_schema, register_schema, display_all_schemas

REGION = "us-east-2"


def add_column_to_schema(entity_name, column_name, column_type,
                         nullable=True, required_for_silver=False, pii=False):
    """
    Add a new column to the active schema.
    Creates a new version with the additional column.
    
    USE CASE: Backend added 'shipping_address' — data engineer
    confirms it's permanent and adds to schema registry.
    """
    current = get_active_schema(entity_name)
    columns = current["columns"].copy()

    # Check if column already exists
    existing_names = {c["name"] for c in columns}
    if column_name in existing_names:
        print(f"ℹ️  Column '{column_name}' already exists in {entity_name} schema")
        return

    # Add new column
    columns.append({
        "name": column_name,
        "type": column_type,
        "nullable": nullable,
        "required_for_silver": required_for_silver,
        "pii": pii
    })

    # Register new version
    new_version = register_schema(
        entity_name,
        columns=columns,
        alias_map=current.get("alias_map", {})
    )

    print(f"✅ Added '{column_name}' ({column_type}) to {entity_name}")
    print(f"   New version: v{new_version}")


def add_alias_to_schema(entity_name, old_name, new_name):
    """
    Add a column alias (rename mapping) to the schema.
    
    USE CASE: Backend renamed 'status' to 'order_status'.
    Data engineer adds alias so pipeline auto-maps.
    
    Alias map: {canonical_name: source_name}
    → {"status": "order_status"} means:
      when we see "order_status" in source, treat it as "status"
    """
    current = get_active_schema(entity_name)
    alias_map = current.get("alias_map", {}).copy()

    alias_map[old_name] = new_name

    # Register new version with updated alias map
    new_version = register_schema(
        entity_name,
        columns=current["columns"],
        alias_map=alias_map
    )

    print(f"✅ Added alias: '{old_name}' → '{new_name}' for {entity_name}")
    print(f"   New version: v{new_version}")


def update_column_type(entity_name, column_name, new_type):
    """
    Update a column's type in the schema.
    
    USE CASE: unit_price changed from decimal(10,2) to decimal(10,4).
    Data engineer updates baseline to match new source type.
    """
    current = get_active_schema(entity_name)
    columns = current["columns"].copy()

    updated = False
    for col in columns:
        if col["name"] == column_name:
            old_type = col["type"]
            col["type"] = new_type
            updated = True
            break

    if not updated:
        print(f"❌ Column '{column_name}' not found in {entity_name} schema")
        return

    new_version = register_schema(
        entity_name,
        columns=columns,
        alias_map=current.get("alias_map", {})
    )

    print(f"✅ Updated type: {entity_name}.{column_name}: {old_type} → {new_type}")
    print(f"   New version: v{new_version}")


def remove_column_from_schema(entity_name, column_name):
    """
    Remove a column from the schema (mark as no longer expected).
    
    USE CASE: Backend permanently removed 'middle_name' column.
    After 90 days of all-NULL, data engineer removes from baseline.
    """
    current = get_active_schema(entity_name)
    columns = [c for c in current["columns"] if c["name"] != column_name]

    if len(columns) == len(current["columns"]):
        print(f"ℹ️  Column '{column_name}' not found in {entity_name} schema")
        return

    # Also remove from alias map if present
    alias_map = current.get("alias_map", {}).copy()
    alias_map.pop(column_name, None)

    new_version = register_schema(
        entity_name,
        columns=columns,
        alias_map=alias_map
    )

    print(f"✅ Removed '{column_name}' from {entity_name} schema")
    print(f"   New version: v{new_version}")


def apply_sprint47_changes():
    """
    Apply all schema changes from Sprint 47 (the scenario in this project).
    
    This is what the data engineer does AFTER receiving the drift alert
    and confirming with the backend team that changes are permanent.
    """
    print("=" * 60)
    print("🔧 APPLYING SPRINT 47 SCHEMA CHANGES")
    print("=" * 60)

    # Change 1: Add shipping_address column
    print(f"\n--- Change 1: New column ---")
    add_column_to_schema(
        "orders",
        column_name="shipping_address",
        column_type="string",
        nullable=True,
        required_for_silver=False,
        pii=True  # PII — contains physical address
    )

    # Change 2: Add alias for renamed column
    print(f"\n--- Change 2: Column rename ---")
    add_alias_to_schema(
        "orders",
        old_name="status",
        new_name="order_status"
    )

    # Change 3: Update type for unit_price
    print(f"\n--- Change 3: Type widening ---")
    update_column_type(
        "orders",
        column_name="unit_price",
        new_type="decimal(10,4)"
    )

    # Verify
    print(f"\n--- Updated Schema ---")
    display_all_schemas()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 45_05_schema_update_utility.py show")
        print("  python 45_05_schema_update_utility.py apply_sprint47")
        print("  python 45_05_schema_update_utility.py add_column orders shipping_address string")
        print("  python 45_05_schema_update_utility.py add_alias orders status order_status")
        print("  python 45_05_schema_update_utility.py update_type orders unit_price 'decimal(10,4)'")
        print("  python 45_05_schema_update_utility.py remove_column orders middle_name")
        sys.exit(0)

    action = sys.argv[1]

    if action == "show":
        display_all_schemas()

    elif action == "apply_sprint47":
        apply_sprint47_changes()

    elif action == "add_column":
        entity = sys.argv[2]
        col_name = sys.argv[3]
        col_type = sys.argv[4] if len(sys.argv) > 4 else "string"
        add_column_to_schema(entity, col_name, col_type)

    elif action == "add_alias":
        entity = sys.argv[2]
        old_name = sys.argv[3]
        new_name = sys.argv[4]
        add_alias_to_schema(entity, old_name, new_name)

    elif action == "update_type":
        entity = sys.argv[2]
        col_name = sys.argv[3]
        new_type = sys.argv[4]
        update_column_type(entity, col_name, new_type)

    elif action == "remove_column":
        entity = sys.argv[2]
        col_name = sys.argv[3]
        remove_column_from_schema(entity, col_name)

    else:
        print(f"Unknown action: {action}")