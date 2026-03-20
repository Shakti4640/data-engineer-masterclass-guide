# file: 05_verify_discovered_tables.py
# Lists all tables created by crawlers and shows their schemas
# Validates that crawlers did their job correctly

import boto3

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"


def list_all_tables():
    """List all tables in the Glue database with full schema details"""
    glue_client = boto3.client("glue", region_name=REGION)

    print(f"📋 TABLES IN DATABASE: {DATABASE_NAME}")
    print("=" * 70)

    # Get all tables
    paginator = glue_client.get_paginator("get_tables")
    tables = []

    for page in paginator.paginate(DatabaseName=DATABASE_NAME):
        tables.extend(page["TableList"])

    if not tables:
        print("   ❌ No tables found — crawlers may not have run yet")
        return

    print(f"   Found {len(tables)} table(s)\n")

    for table in sorted(tables, key=lambda t: t["Name"]):
        print(f"{'─' * 70}")
        print(f"📊 TABLE: {table['Name']}")
        print(f"{'─' * 70}")

        # --- BASIC INFO ---
        print(f"   Description:    {table.get('Description', 'N/A')}")
        print(f"   Created:        {table.get('CreateTime', 'N/A')}")
        print(f"   Updated:        {table.get('UpdateTime', 'N/A')}")

        # --- STORAGE INFO ---
        sd = table.get("StorageDescriptor", {})
        location = sd.get("Location", "N/A")
        input_format = sd.get("InputFormat", "N/A")
        output_format = sd.get("OutputFormat", "N/A")
        serde = sd.get("SerdeInfo", {}).get("SerializationLibrary", "N/A")

        print(f"   S3 Location:    {location}")

        # Determine format from SerDe
        if "parquet" in serde.lower():
            fmt = "PARQUET"
        elif "lazy" in serde.lower() or "csv" in serde.lower():
            fmt = "CSV"
        elif "json" in serde.lower():
            fmt = "JSON"
        else:
            fmt = "UNKNOWN"
        print(f"   Format:         {fmt}")
        print(f"   SerDe:          {serde}")

        # --- COLUMNS ---
        columns = sd.get("Columns", [])
        print(f"\n   📋 Columns ({len(columns)}):")
        print(f"   {'Name':<30} {'Type':<15} {'Comment'}")
        print(f"   {'─' * 30} {'─' * 15} {'─' * 20}")
        for col in columns:
            name = col["Name"]
            dtype = col["Type"]
            comment = col.get("Comment", "")
            print(f"   {name:<30} {dtype:<15} {comment}")

        # --- PARTITION KEYS ---
        partition_keys = table.get("PartitionKeys", [])
        if partition_keys:
            print(f"\n   🔑 Partition Keys ({len(partition_keys)}):")
            for pk in partition_keys:
                print(f"   {pk['Name']:<30} {pk['Type']:<15}")
        else:
            print(f"\n   🔑 Partition Keys: None (non-partitioned)")

        # --- TABLE PARAMETERS ---
        params = table.get("Parameters", {})
        if "recordCount" in params:
            print(f"\n   📊 Statistics:")
            print(f"   Records:      {params.get('recordCount', 'N/A')}")
            print(f"   Avg Row Size: {params.get('averageRecordSize', 'N/A')} bytes")
            print(f"   Total Size:   {params.get('sizeKey', 'N/A')} bytes")
            print(f"   Classification: {params.get('classification', 'N/A')}")

        print()


def get_table_details(table_name):
    """Get detailed info for a single table"""
    glue_client = boto3.client("glue", region_name=REGION)

    response = glue_client.get_table(
        DatabaseName=DATABASE_NAME,
        Name=table_name
    )

    table = response["Table"]
    return table


def generate_athena_preview_queries():
    """Generate Athena SQL to test each discovered table"""
    glue_client = boto3.client("glue", region_name=REGION)

    paginator = glue_client.get_paginator("get_tables")
    tables = []
    for page in paginator.paginate(DatabaseName=DATABASE_NAME):
        tables.extend(page["TableList"])

    print(f"\n📋 ATHENA PREVIEW QUERIES")
    print(f"   Copy-paste these into Athena query editor:\n")

    for table in sorted(tables, key=lambda t: t["Name"]):
        name = table["Name"]
        print(f"-- Preview: {name}")
        print(f"SELECT * FROM {DATABASE_NAME}.{name} LIMIT 10;")
        print(f"SELECT COUNT(*) as row_count FROM {DATABASE_NAME}.{name};")
        print()


if __name__ == "__main__":
    list_all_tables()
    generate_athena_preview_queries()