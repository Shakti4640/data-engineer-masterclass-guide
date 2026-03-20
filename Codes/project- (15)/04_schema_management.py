# file: 04_schema_management.py
# Purpose: Update table schemas when source data changes
# Handles: add column, change type, track schema versions

import boto3
import json
import copy
from datetime import datetime
from botocore.exceptions import ClientError

REGION = "us-east-2"


def load_config():
    with open("catalog_config.json", "r") as f:
        return json.load(f)


class SchemaManager:
    """
    Manages schema evolution in Glue Data Catalog
    
    SCENARIOS:
    → Source adds new column → add column to catalog table
    → Source changes column type → update column type
    → Source removes column → mark as deprecated (don't remove)
    → Track all changes for audit trail
    """
    
    def __init__(self, database_name, region=REGION):
        self.glue_client = boto3.client("glue", region_name=region)
        self.database_name = database_name
    
    def get_current_schema(self, table_name):
        """Get current table definition from catalog"""
        try:
            response = self.glue_client.get_table(
                DatabaseName=self.database_name,
                Name=table_name
            )
            return response["Table"]
        except ClientError as e:
            if "EntityNotFoundException" in str(e):
                return None
            raise
    
    def add_column(self, table_name, column_name, column_type, comment=""):
        """
        Add a new column to existing table
        
        WHY THIS HAPPENS:
        → MariaDB adds new column (ALTER TABLE ADD COLUMN)
        → CSV export now has extra column
        → Catalog must be updated to include it
        → Existing data files without the column: Athena returns NULL for it
        """
        print(f"\n📝 Adding column '{column_name}' to {self.database_name}.{table_name}")
        
        table = self.get_current_schema(table_name)
        if not table:
            print(f"   ❌ Table not found")
            return False
        
        sd = table["StorageDescriptor"]
        current_columns = sd["Columns"]
        
        # Check if column already exists
        existing_names = [col["Name"] for col in current_columns]
        if column_name in existing_names:
            print(f"   ℹ️  Column '{column_name}' already exists — skipping")
            return True
        
        # Add new column
        new_column = {
            "Name": column_name,
            "Type": column_type,
            "Comment": comment
        }
        current_columns.append(new_column)
        
        # Update version tracking
        params = table.get("Parameters", {})
        version = int(params.get("schema_version", "1"))
        params["schema_version"] = str(version + 1)
        params["last_schema_change"] = datetime.now().isoformat()
        params["last_change_type"] = f"add_column:{column_name}"
        
        # Build update input
        table_input = {
            "Name": table_name,
            "Description": table.get("Description", ""),
            "TableType": table.get("TableType", "EXTERNAL_TABLE"),
            "Parameters": params,
            "StorageDescriptor": sd,
            "PartitionKeys": table.get("PartitionKeys", [])
        }
        
        try:
            self.glue_client.update_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
            print(f"   ✅ Column added: {column_name} ({column_type})")
            print(f"   Schema version: {version} → {version + 1}")
            return True
        except ClientError as e:
            print(f"   ❌ Update failed: {e}")
            return False
    
    def update_column_type(self, table_name, column_name, new_type):
        """
        Change a column's data type
        
        ⚠️ CAUTION: changing types can break existing queries
        → string → int: works if all values are numeric
        → int → string: always works (widening)
        → double → int: may lose precision
        """
        print(f"\n📝 Updating column type: {table_name}.{column_name} → {new_type}")
        
        table = self.get_current_schema(table_name)
        if not table:
            print(f"   ❌ Table not found")
            return False
        
        sd = table["StorageDescriptor"]
        
        # Find and update the column
        found = False
        old_type = None
        for col in sd["Columns"]:
            if col["Name"] == column_name:
                old_type = col["Type"]
                col["Type"] = new_type
                found = True
                break
        
        if not found:
            print(f"   ❌ Column '{column_name}' not found")
            return False
        
        # Update version tracking
        params = table.get("Parameters", {})
        version = int(params.get("schema_version", "1"))
        params["schema_version"] = str(version + 1)
        params["last_schema_change"] = datetime.now().isoformat()
        params["last_change_type"] = f"change_type:{column_name}:{old_type}→{new_type}"
        
        table_input = {
            "Name": table_name,
            "Description": table.get("Description", ""),
            "TableType": table.get("TableType", "EXTERNAL_TABLE"),
            "Parameters": params,
            "StorageDescriptor": sd,
            "PartitionKeys": table.get("PartitionKeys", [])
        }
        
        try:
            self.glue_client.update_table(
                DatabaseName=self.database_name,
                TableInput=table_input
            )
            print(f"   ✅ Type changed: {old_type} → {new_type}")
            print(f"   Schema version: {version} → {version + 1}")
            return True
        except ClientError as e:
            print(f"   ❌ Update failed: {e}")
            return False
    
    def compare_schemas(self, table_name, expected_columns):
        """
        Compare catalog schema with expected schema
        Detects: missing columns, extra columns, type mismatches
        
        WHY: after MariaDB schema changes, verify catalog matches
        """
        print(f"\n🔍 Schema comparison: {self.database_name}.{table_name}")
        
        table = self.get_current_schema(table_name)
        if not table:
            print(f"   ❌ Table not found")
            return {"match": False, "diffs": ["table_not_found"]}
        
        catalog_cols = {
            col["Name"]: col["Type"] 
            for col in table["StorageDescriptor"]["Columns"]
        }
        expected_cols = {
            col["name"]: col["type"] 
            for col in expected_columns
        }
        
        diffs = []
        
        # Check for missing columns (in expected but not in catalog)
        for name, etype in expected_cols.items():
            if name not in catalog_cols:
                diffs.append(f"MISSING: {name} ({etype}) — not in catalog")
                print(f"   ❌ Missing: {name} ({etype})")
            elif catalog_cols[name] != etype:
                diffs.append(f"TYPE_MISMATCH: {name} — catalog={catalog_cols[name]}, expected={etype}")
                print(f"   ⚠️  Type mismatch: {name} — catalog={catalog_cols[name]}, expected={etype}")
            else:
                print(f"   ✅ Match: {name} ({etype})")
        
        # Check for extra columns (in catalog but not in expected)
        for name in catalog_cols:
            if name not in expected_cols:
                diffs.append(f"EXTRA: {name} ({catalog_cols[name]}) — in catalog but not expected")
                print(f"   ⚠️  Extra: {name} ({catalog_cols[name]})")
        
        is_match = len(diffs) == 0
        
        if is_match:
            print(f"\n   ✅ Schemas match perfectly")
        else:
            print(f"\n   ⚠️  {len(diffs)} difference(s) found")
        
        return {"match": is_match, "diffs": diffs}
    
    def get_schema_history(self, table_name):
        """
        Get table version history from Glue
        Glue stores up to 1,000,000 versions per table
        """
        print(f"\n📜 Schema history: {self.database_name}.{table_name}")
        
        try:
            response = self.glue_client.get_table_versions(
                DatabaseName=self.database_name,
                TableName=table_name,
                MaxResults=10
            )
            
            versions = response.get("TableVersions", [])
            
            if not versions:
                print(f"   (no version history)")
                return
            
            print(f"   {'Version':<10} {'Columns':<10} {'Updated':<25} {'Change'}")
            print(f"   {'─'*70}")
            
            for ver in versions:
                ver_id = ver.get("VersionId", "?")
                table = ver.get("Table", {})
                sd = table.get("StorageDescriptor", {})
                col_count = len(sd.get("Columns", []))
                updated = str(table.get("UpdateTime", "?"))[:19]
                params = table.get("Parameters", {})
                change = params.get("last_change_type", "initial")
                
                print(f"   {ver_id:<10} {col_count:<10} {updated:<25} {change}")
            
        except ClientError as e:
            print(f"   ❌ Error: {e}")


def run_schema_demo():
    """Demonstrate schema management operations"""
    print("=" * 65)
    print("📝 SCHEMA MANAGEMENT DEMONSTRATION")
    print("=" * 65)
    
    manager = SchemaManager("quickcart_raw_data")
    
    # Demo 1: Show current schema
    print(f"\n{'─'*65}")
    print(f"📋 Step 1: Current schema for 'orders' table")
    print(f"{'─'*65}")
    
    table = manager.get_current_schema("orders")
    if table:
        cols = table["StorageDescriptor"]["Columns"]
        print(f"   Columns ({len(cols)}):")
        for col in cols:
            print(f"      {col['Name']:<20} {col['Type']}")
    
    # Demo 2: Add a new column
    print(f"\n{'─'*65}")
    print(f"📋 Step 2: Add 'discount_code' column")
    print(f"{'─'*65}")
    
    manager.add_column(
        table_name="orders",
        column_name="discount_code",
        column_type="string",
        comment="Promotional discount code applied to order"
    )
    
    # Demo 3: Add another column
    print(f"\n{'─'*65}")
    print(f"📋 Step 3: Add 'shipping_amount' column")
    print(f"{'─'*65}")
    
    manager.add_column(
        table_name="orders",
        column_name="shipping_amount",
        column_type="double",
        comment="Shipping cost charged to customer"
    )
    
    # Demo 4: Compare with expected schema
    print(f"\n{'─'*65}")
    print(f"📋 Step 4: Compare catalog with expected schema")
    print(f"{'─'*65}")
    
    expected = [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "product_id", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "unit_price", "type": "double"},
        {"name": "total_amount", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "order_date", "type": "string"},
        {"name": "discount_code", "type": "string"},
        {"name": "shipping_amount", "type": "double"}
    ]
    
    manager.compare_schemas("orders", expected)
    
    # Demo 5: Version history
    print(f"\n{'─'*65}")
    print(f"📋 Step 5: Schema version history")
    print(f"{'─'*65}")
    
    manager.get_schema_history("orders")
    
    print(f"\n{'='*65}")
    print(f"✅ SCHEMA MANAGEMENT DEMO COMPLETE")
    print(f"{'='*65}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        run_schema_demo()
    elif len(sys.argv) > 1 and sys.argv[1] == "compare":
        # Quick compare: python 04_schema_management.py compare quickcart_raw_data orders
        if len(sys.argv) < 4:
            print("Usage: compare <database> <table>")
            sys.exit(1)
        manager = SchemaManager(sys.argv[2])
        table = manager.get_current_schema(sys.argv[3])
        if table:
            cols = table["StorageDescriptor"]["Columns"]
            print(f"Current columns in {sys.argv[2]}.{sys.argv[3]}:")
            for col in cols:
                print(f"   {col['Name']:<25} {col['Type']:<12} {col.get('Comment', '')[:40]}")
    elif len(sys.argv) > 1 and sys.argv[1] == "history":
        if len(sys.argv) < 4:
            print("Usage: history <database> <table>")
            sys.exit(1)
        manager = SchemaManager(sys.argv[2])
        manager.get_schema_history(sys.argv[3])
    else:
        print("Usage:")
        print("  python 04_schema_management.py demo                        # Run full demo")
        print("  python 04_schema_management.py compare <database> <table>  # Show current schema")
        print("  python 04_schema_management.py history <database> <table>  # Show version history")