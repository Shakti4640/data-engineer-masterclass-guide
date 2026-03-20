# file: migration/01_dms_infrastructure.py
# Purpose: Create DMS replication instance, source/target endpoints,
#          and replication task for full-load-and-cdc migration

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"  # Account A (Ingestion)

# Network config (VPN/DX already established)
VPC_ID = "vpc-0abc123def456"
SUBNET_IDS = ["subnet-0aaa111", "subnet-0bbb222"]
SECURITY_GROUP_ID = "sg-0ccc333"

# Source: On-prem MariaDB
SOURCE_CONFIG = {
    "server": "10.0.1.11",          # Read replica (not primary)
    "port": 3306,
    "username": "dms_replication",
    "password": "RETRIEVE_FROM_SECRETS_MANAGER",  # Project 70
    "databases": ["ecommerce", "payments", "analytics"]
}

# Target: S3 Bronze in Account A
TARGET_CONFIG = {
    "bucket": "quickcart-bronze-prod",
    "prefix": "raw",
    "service_role_arn": f"arn:aws:iam::{ACCOUNT_ID}:role/DMSTargetS3Role"
}


def create_replication_instance():
    """
    Create DMS replication instance
    
    SIZING RATIONALE:
    → dms.r5.2xlarge: 8 vCPU, 64GB RAM
    → Handles 500GB full load + CDC for 45 tables
    → Multi-AZ for reliability during long-running migration
    → Allocated storage: 256GB (for CDC caching during full load)
    """
    dms = boto3.client("dms", region_name=REGION)

    # Create replication subnet group
    dms.create_replication_subnet_group(
        ReplicationSubnetGroupIdentifier="quickcart-migration-subnet-group",
        ReplicationSubnetGroupDescription="Subnets for DMS migration",
        SubnetIds=SUBNET_IDS
    )
    print("✅ Replication subnet group created")

    # Create replication instance
    response = dms.create_replication_instance(
        ReplicationInstanceIdentifier="quickcart-migration-instance",
        ReplicationInstanceClass="dms.r5.2xlarge",
        AllocatedStorage=256,
        VpcSecurityGroupIds=[SECURITY_GROUP_ID],
        ReplicationSubnetGroupIdentifier="quickcart-migration-subnet-group",
        MultiAZ=True,
        EngineVersion="3.5.2",
        PubliclyAccessible=False,
        Tags=[
            {"Key": "Project", "Value": "QuickCart-Migration"},
            {"Key": "Phase", "Value": "migration"},
            {"Key": "CostCenter", "Value": "MIGRATION-2025"}
        ]
    )

    instance_arn = response["ReplicationInstance"]["ReplicationInstanceArn"]
    print(f"✅ Replication instance creating: {instance_arn}")

    # Wait for available
    print("⏳ Waiting for instance to become available (5-10 minutes)...")
    waiter = dms.get_waiter("replication_instance_available")
    waiter.wait(
        Filters=[{
            "Name": "replication-instance-id",
            "Values": ["quickcart-migration-instance"]
        }]
    )
    print("✅ Replication instance is AVAILABLE")
    return instance_arn


def create_source_endpoint():
    """Create DMS source endpoint pointing to on-prem MariaDB"""
    dms = boto3.client("dms", region_name=REGION)

    # Retrieve password from Secrets Manager (Project 70)
    secrets = boto3.client("secretsmanager", region_name=REGION)
    secret = secrets.get_secret_value(SecretId="onprem-mariadb-dms-credentials")
    creds = json.loads(secret["SecretString"])

    response = dms.create_endpoint(
        EndpointIdentifier="onprem-mariadb-source",
        EndpointType="source",
        EngineName="mariadb",
        ServerName=SOURCE_CONFIG["server"],
        Port=SOURCE_CONFIG["port"],
        Username=creds["username"],
        Password=[REDACTED:PASSWORD]["password"],
        DatabaseName="ecommerce",  # Primary DB — others added via extra connection attrs
        ExtraConnectionAttributes=(
            "initstmt=SET FOREIGN_KEY_CHECKS=0;"
            "afterConnectScript=SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;"
        ),
        Tags=[
            {"Key": "Project", "Value": "QuickCart-Migration"},
            {"Key": "Type", "Value": "Source"}
        ]
    )

    source_arn = response["Endpoint"]["EndpointArn"]
    print(f"✅ Source endpoint created: {source_arn}")
    return source_arn


def create_target_endpoint():
    """
    Create DMS target endpoint pointing to S3 Bronze
    
    KEY SETTINGS:
    → DataFormat: parquet (not CSV — better for downstream)
    → ParquetVersion: PARQUET_2_0
    → TimestampColumnName: _dms_ingestion_timestamp (audit column)
    → DatePartitionEnabled: true (auto-partition by date)
    → AddColumnName: true (include column names in Parquet)
    """
    dms = boto3.client("dms", region_name=REGION)

    response = dms.create_endpoint(
        EndpointIdentifier="s3-bronze-target",
        EndpointType="target",
        EngineName="s3",
        S3Settings={
            "BucketName": TARGET_CONFIG["bucket"],
            "BucketFolder": TARGET_CONFIG["prefix"],
            "ServiceAccessRoleArn": TARGET_CONFIG["service_role_arn"],
            "DataFormat": "parquet",
            "ParquetVersion": "PARQUET_2_0",
            "EnableStatistics": True,
            "IncludeOpForFullLoad": True,
            "TimestampColumnName": "_dms_ingestion_timestamp",
            "DatePartitionEnabled": True,
            "DatePartitionSequence": "YYYYMMDD",
            "DatePartitionDelimiter": "SLASH",
            "CdcPath": "CDC",
            "CdcMaxBatchInterval": 60,
            "CdcMinFileSize": 32000,
            "AddColumnName": True,
            "MaxFileSize": 65536,          # 64MB per file
            "ParquetTimestampInMillisecond": True,
            "PreserveTransactions": True
        },
        Tags=[
            {"Key": "Project", "Value": "QuickCart-Migration"},
            {"Key": "Type", "Value": "Target"}
        ]
    )

    target_arn = response["Endpoint"]["EndpointArn"]
    print(f"✅ Target endpoint created: {target_arn}")
    return target_arn


def create_replication_task(instance_arn, source_arn, target_arn):
    """
    Create DMS replication task for full-load-and-cdc
    
    TABLE MAPPINGS:
    → Include all tables from ecommerce, payments, analytics databases
    → Exclude system tables and temp tables
    → Add transformation: add schema name as column (for multi-DB support)
    
    TASK SETTINGS:
    → TargetMetadata.ParallelLoadThreads: 8
    → FullLoadSettings.MaxFullLoadSubTasks: 8
    → Logging: enabled for debugging
    → ValidationSettings: enabled for row count verification
    """
    dms = boto3.client("dms", region_name=REGION)

    table_mappings = {
        "rules": [
            # Include all tables from ecommerce database
            {
                "rule-type": "selection",
                "rule-id": "1",
                "rule-name": "include-ecommerce",
                "rule-action": "include",
                "object-locator": {
                    "schema-name": "ecommerce",
                    "table-name": "%"
                }
            },
            # Include all tables from payments database
            {
                "rule-type": "selection",
                "rule-id": "2",
                "rule-name": "include-payments",
                "rule-action": "include",
                "object-locator": {
                    "schema-name": "payments",
                    "table-name": "%"
                }
            },
            # Include all tables from analytics database
            {
                "rule-type": "selection",
                "rule-id": "3",
                "rule-name": "include-analytics",
                "rule-action": "include",
                "object-locator": {
                    "schema-name": "analytics",
                    "table-name": "%"
                }
            },
            # Exclude temp/staging tables
            {
                "rule-type": "selection",
                "rule-id": "4",
                "rule-name": "exclude-temp",
                "rule-action": "exclude",
                "object-locator": {
                    "schema-name": "%",
                    "table-name": "tmp_%"
                }
            },
            # Transformation: add source database name as column
            {
                "rule-type": "transformation",
                "rule-id": "10",
                "rule-name": "add-source-db-column",
                "rule-action": "add-column",
                "rule-target": "column",
                "object-locator": {
                    "schema-name": "%",
                    "table-name": "%"
                },
                "value": "source_database",
                "expression": "$SCHEMA_NAME",
                "data-type": {
                    "type": "string",
                    "length": 50
                }
            }
        ]
    }

    task_settings = {
        "TargetMetadata": {
            "ParallelLoadThreads": 8,
            "ParallelLoadBufferSize": 500,
            "ParallelLoadQueuesPerThread": 1
        },
        "FullLoadSettings": {
            "TargetTablePrepMode": "DO_NOTHING",
            "MaxFullLoadSubTasks": 8,
            "TransactionConsistencyTimeout": 600,
            "CreatePkAfterFullLoad": False,
            "StopTaskCachedChangesNotApplied": False,
            "CommitRate": 50000
        },
        "ChangeProcessingTuning": {
            "BatchApplyEnabled": True,
            "BatchApplyPreserveTransaction": True,
            "BatchSplitSize": 0,
            "MinTransactionSize": 1000,
            "CommitTimeout": 1,
            "MemoryLimitTotal": 1024,
            "MemoryKeepTime": 60,
            "StatementCacheSize": 50
        },
        "Logging": {
            "EnableLogging": True,
            "LogComponents": [
                {"Id": "TRANSFORMATION", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                {"Id": "SOURCE_UNLOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                {"Id": "TARGET_LOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                {"Id": "SOURCE_CAPTURE", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                {"Id": "TARGET_APPLY", "Severity": "LOGGER_SEVERITY_DEFAULT"}
            ]
        },
        "ValidationSettings": {
            "EnableValidation": True,
            "ThreadCount": 5,
            "ValidationMode": "ROW_LEVEL",
            "FailureMaxCount": 10000
        },
        "CharacterSetSettings": {
            "CharacterReplacements": [],
            "CharacterSetSupport": {
                "CharacterSet": "UTF8"
            }
        }
    }

    response = dms.create_replication_task(
        ReplicationTaskIdentifier="quickcart-full-migration",
        SourceEndpointArn=source_arn,
        TargetEndpointArn=target_arn,
        ReplicationInstanceArn=instance_arn,
        MigrationType="full-load-and-cdc",
        TableMappings=json.dumps(table_mappings),
        ReplicationTaskSettings=json.dumps(task_settings),
        Tags=[
            {"Key": "Project", "Value": "QuickCart-Migration"},
            {"Key": "Phase", "Value": "full-load-and-cdc"}
        ]
    )

    task_arn = response["ReplicationTask"]["ReplicationTaskArn"]
    print(f"✅ Replication task created: {task_arn}")
    return task_arn


def test_connections(instance_arn, source_arn, target_arn):
    """Test connectivity before starting migration"""
    dms = boto3.client("dms", region_name=REGION)

    print("\n🔌 Testing source connection...")
    dms.test_connection(
        ReplicationInstanceArn=instance_arn,
        EndpointArn=source_arn
    )

    print("🔌 Testing target connection...")
    dms.test_connection(
        ReplicationInstanceArn=instance_arn,
        EndpointArn=target_arn
    )

    # Wait for tests to complete
    time.sleep(30)

    connections = dms.describe_connections(
        Filters=[{
            "Name": "endpoint-arn",
            "Values": [source_arn, target_arn]
        }]
    )

    for conn in connections["Connections"]:
        status = conn["Status"]
        endpoint = conn["EndpointIdentifier"]
        if status == "successful":
            print(f"  ✅ {endpoint}: {status}")
        else:
            print(f"  ❌ {endpoint}: {status} — {conn.get('LastFailureMessage', '')}")
            raise RuntimeError(f"Connection test failed for {endpoint}")


def start_migration(task_arn):
    """Start the replication task"""
    dms = boto3.client("dms", region_name=REGION)

    print("\n🚀 STARTING MIGRATION...")
    dms.start_replication_task(
        ReplicationTaskArn=task_arn,
        StartReplicationTaskType="start-replication"
    )
    print("✅ Migration started — full load + CDC active")
    print("   Monitor in DMS console or via describe_replication_tasks")


if __name__ == "__main__":
    print("=" * 70)
    print("🚀 QUICKCART MIGRATION — DMS INFRASTRUCTURE SETUP")
    print("=" * 70)

    instance_arn = create_replication_instance()
    source_arn = create_source_endpoint()
    target_arn = create_target_endpoint()

    test_connections(instance_arn, source_arn, target_arn)

    task_arn = create_replication_task(instance_arn, source_arn, target_arn)

    print("\n🎯 DMS infrastructure ready")
    print(f"   To start migration: start_migration('{task_arn}')")
    print("   ⚠️  Verify all connections before starting!")