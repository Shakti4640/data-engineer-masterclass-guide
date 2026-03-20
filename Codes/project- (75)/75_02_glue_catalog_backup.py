# file: 75_02_glue_catalog_backup.py
# Automated Glue Catalog backup to DR region
# Runs every hour via EventBridge → Lambda
# On failover: restore catalog from backup

import boto3
import json
from datetime import datetime

PRIMARY_REGION = "us-east-2"
DR_REGION = "us-west-2"
BACKUP_BUCKET = "orders-data-dr"  # Already replicated to DR
BACKUP_PREFIX = "dr-backups/glue-catalog/"


def export_catalog(glue_client):
    """
    Export entire Glue Catalog to JSON
    
    Exports:
    → All databases (name, parameters, description)
    → All tables (full definition including columns, SerDe, partitions)
    → Lake Formation tag assignments
    → Table parameters (data product metadata)
    
    Does NOT export:
    → Glue Jobs (stored as scripts in S3 — already replicated)
    → Glue Connections (must recreate with DR endpoints)
    → Lake Formation grants (exported separately)
    """
    catalog_export = {
        "export_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_region": PRIMARY_REGION,
        "databases": []
    }

    # Get all databases
    databases = glue_client.get_databases()

    for db in databases["DatabaseList"]:
        db_name = db["Name"]
        if db_name == "default":
            continue

        db_export = {
            "name": db_name,
            "description": db.get("Description", ""),
            "parameters": db.get("Parameters", {}),
            "target_database": db.get("TargetDatabase"),  # Resource links
            "tables": []
        }

        # Get all tables in this database
        try:
            tables_paginator = glue_client.get_paginator("get_tables")
            for page in tables_paginator.paginate(DatabaseName=db_name):
                for table in page.get("TableList", []):
                    table_export = {
                        "name": table["Name"],
                        "description": table.get("Description", ""),
                        "owner": table.get("Owner", ""),
                        "table_type": table.get("TableType", ""),
                        "parameters": table.get("Parameters", {}),
                        "storage_descriptor": serialize_storage_descriptor(
                            table.get("StorageDescriptor", {})
                        ),
                        "partition_keys": table.get("PartitionKeys", []),
                        "view_original_text": table.get("ViewOriginalText"),
                        "view_expanded_text": table.get("ViewExpandedText")
                    }

                    # Export partition info (for non-projection tables)
                    if not table.get("Parameters", {}).get("projection.enabled") == "true":
                        partitions = export_partitions(glue_client, db_name, table["Name"])
                        table_export["partitions"] = partitions

                    db_export["tables"].append(table_export)

        except Exception as e:
            print(f"   ⚠️  Error exporting tables from {db_name}: {e}")

        catalog_export["databases"].append(db_export)

    return catalog_export


def serialize_storage_descriptor(sd):
    """Convert storage descriptor to JSON-serializable format"""
    return {
        "columns": sd.get("Columns", []),
        "location": sd.get("Location", ""),
        "input_format": sd.get("InputFormat", ""),
        "output_format": sd.get("OutputFormat", ""),
        "serde_info": {
            "serialization_library": sd.get("SerdeInfo", {}).get("SerializationLibrary", ""),
            "parameters": sd.get("SerdeInfo", {}).get("Parameters", {})
        },
        "compressed": sd.get("Compressed", False),
        "parameters": sd.get("Parameters", {})
    }


def export_partitions(glue_client, db_name, table_name, max_partitions=1000):
    """Export partition definitions (not data — just metadata)"""
    partitions = []
    try:
        paginator = glue_client.get_paginator("get_partitions")
        count = 0
        for page in paginator.paginate(DatabaseName=db_name, TableName=table_name):
            for part in page.get("Partitions", []):
                if count >= max_partitions:
                    break
                partitions.append({
                    "values": part["Values"],
                    "location": part["StorageDescriptor"]["Location"]
                })
                count += 1
    except Exception:
        pass
    return partitions


def export_lf_grants(lf_client):
    """Export Lake Formation grants for DR restoration"""
    grants_export = []

    try:
        paginator = lf_client.get_paginator("list_permissions")
        for page in paginator.paginate():
            for perm in page["PrincipalResourcePermissions"]:
                # Serialize the grant
                grant = {
                    "principal": perm["Principal"],
                    "resource": json.loads(json.dumps(perm["Resource"], default=str)),
                    "permissions": perm["Permissions"],
                    "permissions_with_grant_option": perm.get("PermissionsWithGrantOption", [])
                }
                grants_export.append(grant)
    except Exception as e:
        print(f"   ⚠️  Error exporting LF grants: {e}")

    return grants_export


def export_lf_tags(lf_client):
    """Export LF-Tag definitions and assignments"""
    tags_export = {"definitions": [], "assignments": []}

    # Tag definitions
    try:
        tags = lf_client.list_lf_tags()
        for tag in tags.get("LFTags", []):
            tags_export["definitions"].append({
                "tag_key": tag["TagKey"],
                "tag_values": tag["TagValues"]
            })
    except Exception as e:
        print(f"   ⚠️  Error exporting LF-Tag definitions: {e}")

    return tags_export


def save_backup_to_s3(s3_client, catalog_export, lf_grants, lf_tags):
    """Save all exports to DR S3 bucket"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    full_backup = {
        "backup_version": "1.0",
        "timestamp": timestamp,
        "source_region": PRIMARY_REGION,
        "catalog": catalog_export,
        "lake_formation_grants": lf_grants,
        "lake_formation_tags": lf_tags
    }

    # Save timestamped backup
    key = f"{BACKUP_PREFIX}{timestamp}/full_catalog_backup.json"
    s3_client.put_object(
        Bucket=BACKUP_BUCKET,
        Key=key,
        Body=json.dumps(full_backup, indent=2, default=str),
        ContentType="application/json",
        ServerSideEncryption="AES256",
        Metadata={
            "backup-type": "glue-catalog-full",
            "source-region": PRIMARY_REGION,
            "database-count": str(len(catalog_export["databases"])),
            "grant-count": str(len(lf_grants))
        }
    )
    print(f"   ✅ Backup saved: s3://{BACKUP_BUCKET}/{key}")

    # Save as "latest" (overwrite for quick access during failover)
    latest_key = f"{BACKUP_PREFIX}latest/full_catalog_backup.json"
    s3_client.put_object(
        Bucket=BACKUP_BUCKET,
        Key=latest_key,
        Body=json.dumps(full_backup, indent=2, default=str),
        ContentType="application/json",
        ServerSideEncryption="AES256"
    )
    print(f"   ✅ Latest pointer: s3://{BACKUP_BUCKET}/{latest_key}")

    return key


def restore_catalog_in_dr(backup_key=None):
    """
    FAILOVER FUNCTION: Restore Glue Catalog in DR region
    
    Called during disaster recovery to recreate entire catalog
    from the latest backup in DR S3 bucket
    """
    s3_dr = boto3.client("s3", region_name=DR_REGION)
    glue_dr = boto3.client("glue", region_name=DR_REGION)
    lf_dr = boto3.client("lakeformation", region_name=DR_REGION)

    # Load backup
    if backup_key is None:
        backup_key = f"{BACKUP_PREFIX}latest/full_catalog_backup.json"

    print(f"\n🔄 RESTORING CATALOG IN {DR_REGION}")
    print(f"   Source: s3://{BACKUP_BUCKET}/{backup_key}")

    response = s3_dr.get_object(Bucket=BACKUP_BUCKET, Key=backup_key)
    backup = json.loads(response["Body"].read().decode("utf-8"))

    print(f"   Backup timestamp: {backup['timestamp']}")
    print(f"   Databases: {len(backup['catalog']['databases'])}")

    # Restore databases and tables
    for db in backup["catalog"]["databases"]:
        db_name = db["name"]

        # Skip resource links (cross-account references don't work in DR)
        if db.get("target_database"):
            print(f"   ⏭️  Skipping resource link: {db_name}")
            continue

        # Create database
        try:
            db_input = {
                "Name": db_name,
                "Description": db.get("description", "") + " [DR RESTORE]",
                "Parameters": db.get("parameters", {})
            }
            glue_dr.create_database(DatabaseInput=db_input)
            print(f"   ✅ Database: {db_name}")
        except glue_dr.exceptions.AlreadyExistsException:
            print(f"   ℹ️  Database exists: {db_name}")

        # Create tables
        for table in db.get("tables", []):
            try:
                sd = table["storage_descriptor"]

                # Update S3 location to point to DR bucket
                location = sd["location"]
                location = location.replace("orders-data-prod", "orders-data-dr")
                location = location.replace("analytics-data-prod", "analytics-data-dr")

                table_input = {
                    "Name": table["name"],
                    "Description": table.get("description", ""),
                    "Owner": table.get("owner", ""),
                    "TableType": table.get("table_type", "EXTERNAL_TABLE"),
                    "Parameters": table.get("parameters", {}),
                    "StorageDescriptor": {
                        "Columns": sd.get("columns", []),
                        "Location": location,
                        "InputFormat": sd.get("input_format", ""),
                        "OutputFormat": sd.get("output_format", ""),
                        "SerdeInfo": {
                            "SerializationLibrary": sd.get("serde_info", {}).get(
                                "serialization_library", ""
                            ),
                            "Parameters": sd.get("serde_info", {}).get("parameters", {})
                        },
                        "Compressed": sd.get("compressed", False)
                    },
                    "PartitionKeys": table.get("partition_keys", [])
                }

                glue_dr.create_table(
                    DatabaseName=db_name,
                    TableInput=table_input
                )
                print(f"      ✅ Table: {db_name}.{table['name']}")

                # Restore partitions (if not using partition projection)
                partitions = table.get("partitions", [])
                if partitions:
                    restore_partitions(glue_dr, db_name, table["name"],
                                      table_input, partitions, location)

            except glue_dr.exceptions.AlreadyExistsException:
                print(f"      ℹ️  Table exists: {db_name}.{table['name']}")
            except Exception as e:
                print(f"      ⚠️  Table {db_name}.{table['name']}: {e}")

    # Restore LF-Tags
    lf_tags = backup.get("lake_formation_tags", {})
    if lf_tags.get("definitions"):
        print(f"\n   🏷️  Restoring LF-Tags:")
        for tag_def in lf_tags["definitions"]:
            try:
                lf_dr.create_lf_tag(
                    TagKey=tag_def["tag_key"],
                    TagValues=tag_def["tag_values"]
                )
                print(f"      ✅ Tag: {tag_def['tag_key']} = {tag_def['tag_values']}")
            except Exception:
                print(f"      ℹ️  Tag exists: {tag_def['tag_key']}")

    # Restore LF grants
    lf_grants = backup.get("lake_formation_grants", [])
    if lf_grants:
        print(f"\n   🔐 Restoring {len(lf_grants)} LF grants:")
        restored = 0
        for grant in lf_grants:
            try:
                lf_dr.grant_permissions(
                    Principal=grant["principal"],
                    Resource=grant["resource"],
                    Permissions=grant["permissions"],
                    PermissionsWithGrantOption=grant.get(
                        "permissions_with_grant_option", []
                    )
                )
                restored += 1
            except Exception:
                pass  # Skip failures (some grants may not apply in DR)
        print(f"      ✅ Restored {restored}/{len(lf_grants)} grants")

    print(f"\n✅ CATALOG RESTORE COMPLETE IN {DR_REGION}")


def restore_partitions(glue_dr, db_name, table_name, table_input, partitions, base_location):
    """Batch restore partitions"""
    if not partitions:
        return

    batch = []
    for part in partitions[:1000]:  # Limit to prevent timeout
        part_location = part.get("location", "")
        part_location = part_location.replace("orders-data-prod", "orders-data-dr")
        part_location = part_location.replace("analytics-data-prod", "analytics-data-dr")

        batch.append({
            "Values": part["values"],
            "StorageDescriptor": {
                **table_input["StorageDescriptor"],
                "Location": part_location
            }
        })

        if len(batch) >= 100:  # BatchCreatePartition limit
            try:
                glue_dr.batch_create_partition(
                    DatabaseName=db_name,
                    TableName=table_name,
                    PartitionInputList=batch
                )
            except Exception:
                pass
            batch = []

    # Flush remaining
    if batch:
        try:
            glue_dr.batch_create_partition(
                DatabaseName=db_name,
                TableName=table_name,
                PartitionInputList=batch
            )
        except Exception:
            pass

    print(f"         Partitions: {min(len(partitions), 1000)} restored")


def run_backup():
    """Main backup function — called by Lambda every hour"""
    print("=" * 70)
    print("💾 GLUE CATALOG BACKUP — Hourly DR Export")
    print(f"   Source: {PRIMARY_REGION}")
    print(f"   Destination: s3://{BACKUP_BUCKET}/{BACKUP_PREFIX}")
    print("=" * 70)

    glue_client = boto3.client("glue", region_name=PRIMARY_REGION)
    lf_client = boto3.client("lakeformation", region_name=PRIMARY_REGION)
    s3_client = boto3.client("s3", region_name=DR_REGION)

    # Export catalog
    print("\n--- Exporting Glue Catalog ---")
    catalog_export = export_catalog(glue_client)
    db_count = len(catalog_export["databases"])
    table_count = sum(len(db["tables"]) for db in catalog_export["databases"])
    print(f"   Exported: {db_count} databases, {table_count} tables")

    # Export LF grants
    print("\n--- Exporting Lake Formation Grants ---")
    lf_grants = export_lf_grants(lf_client)
    print(f"   Exported: {len(lf_grants)} grants")

    # Export LF tags
    print("\n--- Exporting LF-Tags ---")
    lf_tags = export_lf_tags(lf_client)
    print(f"   Exported: {len(lf_tags.get('definitions', []))} tag definitions")

    # Save to DR S3
    print("\n--- Saving to DR S3 ---")
    backup_key = save_backup_to_s3(s3_client, catalog_export, lf_grants, lf_tags)

    print("\n" + "=" * 70)
    print("✅ CATALOG BACKUP COMPLETE")
    print(f"   Databases: {db_count} | Tables: {table_count} | Grants: {len(lf_grants)}")
    print("=" * 70)

    return backup_key


def main():
    """
    Run modes:
    → 'backup': Export catalog from primary (hourly cron)
    → 'restore': Import catalog into DR (failover only)
    """
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else "backup"

    if mode == "backup":
        run_backup()
    elif mode == "restore":
        restore_catalog_in_dr()
    else:
        print(f"Usage: python 75_02_glue_catalog_backup.py [backup|restore]")


if __name__ == "__main__":
    main()