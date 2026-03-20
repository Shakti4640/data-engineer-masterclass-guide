# file: 57_03_test_catalog_access.py
# Purpose: Verify cross-account Glue Catalog access from Account B

import boto3
import json
from datetime import datetime

REGION = "us-east-2"


def load_config():
    with open("/tmp/catalog_sharing_config.json", "r") as f:
        return json.load(f)


def test_cross_account_catalog():
    """
    Comprehensive test suite for cross-account Catalog access
    Tests both positive (should work) and negative (should fail) cases
    """
    config = load_config()
    glue_client = boto3.client("glue", region_name=REGION)
    account_a_id = config["account_a_id"]

    print("=" * 70)
    print("🧪 TESTING CROSS-ACCOUNT GLUE CATALOG ACCESS")
    print(f"   From Account B → Account A's Catalog ({account_a_id})")
    print("=" * 70)

    results = {}

    # ═══════════════════════════════════════
    # TEST 1: Get shared database
    # ═══════════════════════════════════════
    print(f"\n📌 Test 1: GetDatabase (quickcart_db) — should SUCCEED...")
    try:
        response = glue_client.get_database(
            CatalogId=account_a_id,
            Name="quickcart_db"
        )
        db = response["Database"]
        results["get_shared_db"] = "✅ PASS"
        print(f"  {results['get_shared_db']}")
        print(f"    Name: {db['Name']}")
        print(f"    Description: {db.get('Description', 'N/A')}")
        print(f"    Location: {db.get('LocationUri', 'N/A')}")
        print(f"    Created: {db.get('CreateTime', 'N/A')}")
    except Exception as e:
        results["get_shared_db"] = f"❌ FAIL: {e}"
        print(f"  {results['get_shared_db']}")

    # ═══════════════════════════════════════
    # TEST 2: List all databases (should see shared, not denied)
    # ═══════════════════════════════════════
    print(f"\n📌 Test 2: GetDatabases (list all) — should see ONLY shared...")
    try:
        response = glue_client.get_databases(
            CatalogId=account_a_id
        )
        databases = [db["Name"] for db in response["DatabaseList"]]
        results["list_databases"] = "✅ PASS"
        print(f"  {results['list_databases']}")
        print(f"    Visible databases: {databases}")

        # Verify denied databases are NOT visible
        for denied_db in config["denied_databases"]:
            if denied_db in databases:
                results["list_databases"] = f"❌ SECURITY ISSUE — {denied_db} visible!"
                print(f"  {results['list_databases']}")
                break
        else:
            print(f"    ✅ Denied databases correctly hidden")
    except Exception as e:
        results["list_databases"] = f"❌ FAIL: {e}"
        print(f"  {results['list_databases']}")

    # ═══════════════════════════════════════
    # TEST 3: Get tables in shared database
    # ═══════════════════════════════════════
    print(f"\n📌 Test 3: GetTables (quickcart_db) — should return tables...")
    try:
        response = glue_client.get_tables(
            CatalogId=account_a_id,
            DatabaseName="quickcart_db"
        )
        tables = response.get("TableList", [])
        results["get_tables"] = "✅ PASS"
        print(f"  {results['get_tables']}")
        print(f"    Tables found: {len(tables)}")
        for table in tables[:5]:
            columns = table.get("StorageDescriptor", {}).get("Columns", [])
            location = table.get("StorageDescriptor", {}).get("Location", "N/A")
            print(f"    → {table['Name']}")
            print(f"      Columns: {len(columns)}")
            print(f"      Location: {location[:60]}...")
        if len(tables) > 5:
            print(f"    ... and {len(tables) - 5} more tables")
    except Exception as e:
        results["get_tables"] = f"❌ FAIL: {e}"
        print(f"  {results['get_tables']}")

    # ═══════════════════════════════════════
    # TEST 4: Get specific table with full schema
    # ═══════════════════════════════════════
    print(f"\n📌 Test 4: GetTable (orders_gold) — should return full schema...")
    try:
        response = glue_client.get_table(
            CatalogId=account_a_id,
            DatabaseName="quickcart_db",
            Name="orders_gold"
        )
        table = response["Table"]
        columns = table.get("StorageDescriptor", {}).get("Columns", [])
        partition_keys = table.get("PartitionKeys", [])
        serde = table.get("StorageDescriptor", {}).get("SerdeInfo", {})
        location = table.get("StorageDescriptor", {}).get("Location", "N/A")

        results["get_specific_table"] = "✅ PASS"
        print(f"  {results['get_specific_table']}")
        print(f"    Table: {table['Name']}")
        print(f"    S3 Location: {location}")
        print(f"    SerDe: {serde.get('SerializationLibrary', 'N/A')}")
        print(f"    Columns ({len(columns)}):")
        for col in columns[:5]:
            print(f"      → {col['Name']}: {col['Type']}")
        if partition_keys:
            print(f"    Partition Keys: {[pk['Name'] for pk in partition_keys]}")
    except Exception as e:
        results["get_specific_table"] = f"❌ FAIL: {e}"
        print(f"  {results['get_specific_table']}")

    # ═══════════════════════════════════════
    # TEST 5: Get partitions (if table is partitioned)
    # ═══════════════════════════════════════
    print(f"\n📌 Test 5: GetPartitions — should return partition list...")
    try:
        response = glue_client.get_partitions(
            CatalogId=account_a_id,
            DatabaseName="quickcart_db",
            TableName="orders_gold",
            MaxResults=5
        )
        partitions = response.get("Partitions", [])
        results["get_partitions"] = "✅ PASS"
        print(f"  {results['get_partitions']}")
        print(f"    Partitions found: {len(partitions)}")
        for part in partitions[:3]:
            print(f"    → Values: {part['Values']}")
    except Exception as e:
        error_msg = str(e)
        if "EntityNotFoundException" in error_msg:
            results["get_partitions"] = "⚠️  SKIP — table not partitioned or not found"
        else:
            results["get_partitions"] = f"❌ FAIL: {e}"
        print(f"  {results['get_partitions']}")

    # ═══════════════════════════════════════
    # TEST 6: Access DENIED database (should fail)
    # ═══════════════════════════════════════
    print(f"\n📌 Test 6: GetDatabase (internal_db) — should be DENIED...")
    try:
        glue_client.get_database(
            CatalogId=account_a_id,
            Name="internal_db"
        )
        results["denied_database"] = "❌ SECURITY ISSUE — denied DB was accessible!"
        print(f"  {results['denied_database']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code in ["AccessDeniedException", "EntityNotFoundException"]:
            results["denied_database"] = "✅ PASS — correctly denied/hidden"
            print(f"  {results['denied_database']}")
        else:
            results["denied_database"] = f"⚠️  Unexpected: {e}"
            print(f"  {results['denied_database']}")

    # ═══════════════════════════════════════
    # TEST 7: Write operation (should fail)
    # ═══════════════════════════════════════
    print(f"\n📌 Test 7: CreateTable in Account A — should be DENIED...")
    try:
        glue_client.create_table(
            CatalogId=account_a_id,
            DatabaseName="quickcart_db",
            TableInput={
                "Name": "unauthorized_table",
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "int"}],
                    "Location": "s3://fake/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}
                }
            }
        )
        results["create_table_denied"] = "❌ SECURITY ISSUE — write was allowed!"
        print(f"  {results['create_table_denied']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDeniedException":
            results["create_table_denied"] = "✅ PASS — correctly denied"
            print(f"  {results['create_table_denied']}")
        else:
            results["create_table_denied"] = f"⚠️  Unexpected: {e}"
            print(f"  {results['create_table_denied']}")

    # ═══════════════════════════════════════
    # TEST 8: Delete operation (should fail)
    # ═══════════════════════════════════════
    print(f"\n📌 Test 8: DeleteTable in Account A — should be DENIED...")
    try:
        glue_client.delete_table(
            CatalogId=account_a_id,
            DatabaseName="quickcart_db",
            Name="orders_gold"
        )
        results["delete_table_denied"] = "❌ SECURITY ISSUE — delete was allowed!"
        print(f"  {results['delete_table_denied']}")
    except Exception as e:
        error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
        if error_code == "AccessDeniedException":
            results["delete_table_denied"] = "✅ PASS — correctly denied"
            print(f"  {results['delete_table_denied']}")
        else:
            results["delete_table_denied"] = f"⚠️  Unexpected: {e}"
            print(f"  {results['delete_table_denied']}")

    # ═══════════════════════════════════════
    # FINAL REPORT
    # ═══════════════════════════════════════
    passed = sum(1 for v in results.values() if v.startswith("✅"))
    failed = sum(1 for v in results.values() if v.startswith("❌"))
    warnings = sum(1 for v in results.values() if v.startswith("⚠️"))

    print(f"\n{'=' * 70}")
    print(f"📊 CROSS-ACCOUNT CATALOG TEST REPORT")
    print(f"{'=' * 70}")
    print(f"  Total tests:  {len(results)}")
    print(f"  ✅ Passed:    {passed}")
    print(f"  ❌ Failed:    {failed}")
    print(f"  ⚠️  Warnings: {warnings}")

    for test_name, result in results.items():
        print(f"  {result[:2]} {test_name}")

    if failed > 0:
        print(f"\n  🔴 SECURITY/ACCESS ISSUES DETECTED")
    elif warnings > 0:
        print(f"\n  🟡 MOSTLY GOOD — review warnings")
    else:
        print(f"\n  🟢 ALL TESTS PASSED")
    print(f"{'=' * 70}")

    return results


if __name__ == "__main__":
    test_cross_account_catalog()