# file: 03_catalog_explorer.py
# Purpose: Browse, search, and inspect catalog contents
# Operational tool for discovering available datasets

import boto3
import json
import sys
from botocore.exceptions import ClientError

REGION = "us-east-2"


def list_all_databases(glue_client):
    """List all databases with details"""
    print(f"\n📚 ALL DATABASES:")
    print(f"   {'─'*60}")
    
    paginator = glue_client.get_paginator("get_databases")
    
    db_count = 0
    for page in paginator.paginate():
        for db in page["DatabaseList"]:
            db_count += 1
            name = db["Name"]
            desc = db.get("Description", "No description")[:70]
            params = db.get("Parameters", {})
            layer = params.get("layer", "N/A")
            
            # Count tables in this database
            try:
                tables_resp = glue_client.get_tables(DatabaseName=name)
                table_count = len(tables_resp.get("TableList", []))
            except ClientError:
                table_count = "?"
            
            print(f"\n   📦 {name}")
            print(f"      Description: {desc}")
            print(f"      Layer:       {layer}")
            print(f"      Tables:      {table_count}")
    
    print(f"\n   Total databases: {db_count}")


def list_tables(glue_client, database_name):
    """List all tables in a database"""
    print(f"\n📋 TABLES IN '{database_name}':")
    print(f"   {'─'*60}")
    
    try:
        response = glue_client.get_tables(DatabaseName=database_name)
        tables = response.get("TableList", [])
        
        if not tables:
            print(f"   (no tables)")
            return
        
        for table in tables:
            name = table["Name"]
            desc = table.get("Description", "")[:50]
            sd = table["StorageDescriptor"]
            location = sd.get("Location", "N/A")
            col_count = len(sd.get("Columns", []))
            serde = sd.get("SerdeInfo", {}).get(
                "SerializationLibrary", "N/A").split(".")[-1]
            params = table.get("Parameters", {})
            classification = params.get("classification", "N/A")
            
            print(f"\n   📄 {name}")
            print(f"      Description: {desc}")
            print(f"      Location:    {location}")
            print(f"      Format:      {classification} (SerDe: {serde})")
            print(f"      Columns:     {col_count}")
            
    except ClientError as e:
        print(f"   ❌ Error: {e}")


def describe_table(glue_client, database_name, table_name):
    """Show complete table details"""
    print(f"\n🔍 TABLE DETAILS: {database_name}.{table_name}")
    print(f"   {'─'*60}")
    
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        
        table = response["Table"]
        sd = table["StorageDescriptor"]
        
        print(f"\n   General:")
        print(f"      Name:          {table['Name']}")
        print(f"      Database:      {database_name}")
        print(f"      Type:          {table.get('TableType', 'N/A')}")
        print(f"      Description:   {table.get('Description', 'N/A')}")
        print(f"      Created:       {table.get('CreateTime', 'N/A')}")
        print(f"      Updated:       {table.get('UpdateTime', 'N/A')}")
        
        print(f"\n   Storage:")
        print(f"      Location:      {sd.get('Location', 'N/A')}")
        print(f"      InputFormat:   {sd.get('InputFormat', 'N/A').split('.')[-1]}")
        print(f"      OutputFormat:  {sd.get('OutputFormat', 'N/A').split('.')[-1]}")
        print(f"      Compressed:    {sd.get('Compressed', False)}")
        
        serde = sd.get("SerdeInfo", {})
        print(f"\n   SerDe:")
        print(f"      Library:       {serde.get('SerializationLibrary', 'N/A').split('.')[-1]}")
        serde_params = serde.get("Parameters", {})
        for k, v in serde_params.items():
            # Show escape characters as readable
            display_v = repr(v) if len(v) == 1 else v
            print(f"      {k}: {display_v}")
        
        print(f"\n   Columns ({len(sd.get('Columns', []))}):")
        print(f"      {'Name':<25} {'Type':<12} {'Comment'}")
        print(f"      {'─'*70}")
        for col in sd.get("Columns", []):
            comment = col.get("Comment", "")[:40]
            print(f"      {col['Name']:<25} {col['Type']:<12} {comment}")
        
        # Partition keys
        part_keys = table.get("PartitionKeys", [])
        if part_keys:
            print(f"\n   Partition Keys ({len(part_keys)}):")
            for pk in part_keys:
                print(f"      {pk['Name']} ({pk['Type']})")
        else:
            print(f"\n   Partition Keys: none")
        
        # Properties
        params = table.get("Parameters", {})
        if params:
            print(f"\n   Properties:")
            for k, v in sorted(params.items()):
                print(f"      {k}: {v}")
        
        # Generate Athena query hint
        print(f"\n   💡 Query with Athena:")
        cols_sample = ", ".join(
            col["Name"] for col in sd.get("Columns", [])[:5]
        )
        print(f"      SELECT {cols_sample}")
        print(f"      FROM {database_name}.{table_name}")
        print(f"      LIMIT 10;")
        
    except ClientError as e:
        if "EntityNotFoundException" in str(e):
            print(f"   ❌ Table not found: {database_name}.{table_name}")
        else:
            print(f"   ❌ Error: {e}")


def search_tables(glue_client, search_term):
    """Search for tables across all databases"""
    print(f"\n🔎 SEARCHING FOR: '{search_term}'")
    print(f"   {'─'*60}")
    
    found = 0
    
    # Get all databases
    db_response = glue_client.get_databases()
    
    for db in db_response["DatabaseList"]:
        db_name = db["Name"]
        
        try:
            tables_response = glue_client.get_tables(DatabaseName=db_name)
            
            for table in tables_response.get("TableList", []):
                table_name = table["Name"]
                desc = table.get("Description", "")
                
                # Search in name and description
                if (search_term.lower() in table_name.lower() or
                    search_term.lower() in desc.lower()):
                    
                    found += 1
                    col_count = len(table["StorageDescriptor"].get("Columns", []))
                    print(f"\n   📄 {db_name}.{table_name}")
                    print(f"      Description: {desc[:60]}")
                    print(f"      Columns: {col_count}")
                    
        except ClientError:
            pass
    
    if found == 0:
        print(f"   No tables found matching '{search_term}'")
    else:
        print(f"\n   Found {found} table(s)")


def generate_athena_ddl(glue_client, database_name, table_name):
    """Generate the equivalent Athena CREATE TABLE statement"""
    print(f"\n📝 ATHENA DDL FOR: {database_name}.{table_name}")
    print(f"   {'─'*60}")
    
    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name
        )
        
        table = response["Table"]
        sd = table["StorageDescriptor"]
        serde = sd.get("SerdeInfo", {})
        params = table.get("Parameters", {})
        
        # Build column list
        columns = []
        for col in sd.get("Columns", []):
            col_def = f"  `{col['Name']}` {col['Type']}"
            if col.get("Comment"):
                col_def += f" COMMENT '{col['Comment']}'"
            columns.append(col_def)
        
        col_str = ",\n".join(columns)
        
        # Build DDL
        ddl = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
{col_str}
)
ROW FORMAT SERDE '{serde.get("SerializationLibrary", "")}'
WITH SERDEPROPERTIES (
  'separatorChar' = '{serde.get("Parameters", {}).get("separatorChar", ",")}',
  'quoteChar' = '{serde.get("Parameters", {}).get("quoteChar", '"')}'
)
STORED AS INPUTFORMAT '{sd.get("InputFormat", "")}'
OUTPUTFORMAT '{sd.get("OutputFormat", "")}'
LOCATION '{sd.get("Location", "")}'
TBLPROPERTIES (
  'skip.header.line.count' = '{params.get("skip.header.line.count", "0")}',
  'classification' = '{params.get("classification", "csv")}'
);"""
        
        print(f"\n{ddl}")
        print(f"\n   💡 This DDL produces the SAME table as the API-created one")
        print(f"   💡 Both approaches write to Glue Data Catalog")
        
    except ClientError as e:
        print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    glue_client = boto3.client("glue", region_name=REGION)
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python 03_catalog_explorer.py databases                    # List all databases")
        print("  python 03_catalog_explorer.py tables <database>            # List tables in database")
        print("  python 03_catalog_explorer.py describe <database> <table>  # Full table details")
        print("  python 03_catalog_explorer.py search <term>                # Search across all")
        print("  python 03_catalog_explorer.py ddl <database> <table>       # Generate Athena DDL")
        print("")
        print("Examples:")
        print("  python 03_catalog_explorer.py databases")
        print("  python 03_catalog_explorer.py tables quickcart_raw_data")
        print("  python 03_catalog_explorer.py describe quickcart_raw_data orders")
        print("  python 03_catalog_explorer.py search customer")
        print("  python 03_catalog_explorer.py ddl quickcart_raw_data orders")
        sys.exit(0)
    
    action = sys.argv[1].lower()
    
    if action == "databases":
        list_all_databases(glue_client)
    
    elif action == "tables":
        if len(sys.argv) < 3:
            print("Usage: tables <database_name>")
            sys.exit(1)
        list_tables(glue_client, sys.argv[2])
    
    elif action == "describe":
        if len(sys.argv) < 4:
            print("Usage: describe <database_name> <table_name>")
            sys.exit(1)
        describe_table(glue_client, sys.argv[2], sys.argv[3])
    
    elif action == "search":
        if len(sys.argv) < 3:
            print("Usage: search <search_term>")
            sys.exit(1)
        search_tables(glue_client, sys.argv[2])
    
    elif action == "ddl":
        if len(sys.argv) < 4:
            print("Usage: ddl <database_name> <table_name>")
            sys.exit(1)
        generate_athena_ddl(glue_client, sys.argv[2], sys.argv[3])
    
    else:
        print(f"❌ Unknown action: {action}")
        sys.exit(1)