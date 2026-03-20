# file: 67_02_create_serverless.py
# Run from: Account B (222222222222)
# Purpose: Create Redshift Serverless namespace + workgroup

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Serverless configuration
NAMESPACE_NAME = "quickcart-analytics"
WORKGROUP_NAME = "quickcart-prod"
ADMIN_USER = "etl_writer"
ADMIN_PASS [REDACTED:PASSWORD]025"
DB_NAME = "analytics"

# Network (same VPC as provisioned cluster)
VPC_ID = "vpc-0bbb2222bbbb2222b"
SUBNET_IDS = ["subnet-0bbb2222sub1", "subnet-0bbb2222sub2", "subnet-0bbb2222sub3"]
SECURITY_GROUP_IDS = ["sg-0bbb2222redshift"]


def create_namespace():
    """
    Create Serverless namespace (data container)
    
    NAMESPACE:
    → Stores databases, schemas, tables, users
    → Equivalent to provisioned cluster's data storage
    → Persists independently of workgroup
    → Can be restored from provisioned cluster snapshot
    """
    rs_serverless = boto3.client("redshift-serverless", region_name=REGION)

    try:
        response = rs_serverless.create_namespace(
            namespaceName=NAMESPACE_NAME,
            adminUsername=ADMIN_USER,
            adminUserPassword=ADMIN_PASS,
            dbName=DB_NAME,
            defaultIamRoleArn=f"arn:aws:iam::{ACCOUNT_B_ID}:role/GlueCrossAccountETLRole",
            iamRoles=[
                f"arn:aws:iam::{ACCOUNT_B_ID}:role/GlueCrossAccountETLRole"
            ],
            tags=[
                {"key": "Environment", "value": "Production"},
                {"key": "ManagedBy", "value": "DataEngineering"}
            ]
        )
        ns = response["namespace"]
        print(f"✅ Namespace created: {ns['namespaceName']}")
        print(f"   Status: {ns['status']}")
        print(f"   Database: {DB_NAME}")
        return ns

    except rs_serverless.exceptions.ConflictException:
        print(f"ℹ️  Namespace already exists: {NAMESPACE_NAME}")
        desc = rs_serverless.get_namespace(namespaceName=NAMESPACE_NAME)
        return desc["namespace"]


def create_workgroup():
    """
    Create Serverless workgroup (compute container)
    
    WORKGROUP:
    → Provides RPU compute for running queries
    → Has its own network endpoint
    → Configurable: base RPU, max RPU, usage limits
    → Attached to a namespace (shares data)
    
    KEY SETTINGS:
    → baseCapacity: 8 RPU (minimum, cheapest idle cost)
    → maxCapacity: 128 RPU (auto-scales up to this)
    → Usage limits: prevent cost explosions
    """
    rs_serverless = boto3.client("redshift-serverless", region_name=REGION)

    try:
        response = rs_serverless.create_workgroup(
            workgroupName=WORKGROUP_NAME,
            namespaceName=NAMESPACE_NAME,
            baseCapacity=8,       # Minimum RPU (cheapest idle)
            maxCapacity=128,      # Max RPU for burst queries
            publiclyAccessible=False,
            subnetIds=SUBNET_IDS,
            securityGroupIds=SECURITY_GROUP_IDS,
            enhancedVpcRouting=True,
            configParameters=[
                {"parameterKey": "enable_case_sensitive_identifier", "parameterValue": "false"},
                {"parameterKey": "auto_mv", "parameterValue": "true"},
                {"parameterKey": "datestyle", "parameterValue": "ISO, MDY"},
                {"parameterKey": "search_path", "parameterValue": "quickcart_ops, public"}
            ],
            tags=[
                {"key": "Environment", "value": "Production"},
                {"key": "ManagedBy", "value": "DataEngineering"}
            ]
        )

        wg = response["workgroup"]
        print(f"✅ Workgroup created: {wg['workgroupName']}")
        print(f"   Status: {wg['status']}")
        print(f"   Base RPU: {wg['baseCapacity']}")
        print(f"   Max RPU: {wg.get('maxCapacity', 'default')}")

        # Wait for workgroup to become available
        print(f"   Waiting for AVAILABLE status...")
        while True:
            time.sleep(15)
            desc = rs_serverless.get_workgroup(workgroupName=WORKGROUP_NAME)
            status = desc["workgroup"]["status"]
            print(f"   Status: {status}")
            if status == "AVAILABLE":
                endpoint = desc["workgroup"]["endpoint"]
                print(f"   ✅ Workgroup available!")
                print(f"   Endpoint: {endpoint['address']}:{endpoint['port']}")
                return desc["workgroup"]
            elif status in ["CREATING", "MODIFYING"]:
                continue
            else:
                raise Exception(f"Unexpected workgroup status: {status}")

    except rs_serverless.exceptions.ConflictException:
        print(f"ℹ️  Workgroup already exists: {WORKGROUP_NAME}")
        desc = rs_serverless.get_workgroup(workgroupName=WORKGROUP_NAME)
        wg = desc["workgroup"]
        endpoint = wg.get("endpoint", {})
        print(f"   Endpoint: {endpoint.get('address', 'N/A')}:{endpoint.get('port', 'N/A')}")
        return wg


def set_usage_limits():
    """
    CRITICAL: Set usage limits to prevent cost explosions
    
    LIMITS:
    → Per-day RPU-hours: 200 (max ~$75/day)
    → Per-month RPU-hours: 3000 (max ~$1,125/month)
    → Action on breach: alert + deactivate user queries
    """
    rs_serverless = boto3.client("redshift-serverless", region_name=REGION)

    limits = [
        {
            "amount": 200,
            "period": "daily",
            "resourceArn": f"arn:aws:redshift-serverless:{REGION}:{ACCOUNT_B_ID}:workgroup/{WORKGROUP_NAME}",
            "usageType": "serverless-compute",
            "breachAction": "deactivate"
        },
        {
            "amount": 3000,
            "period": "monthly",
            "resourceArn": f"arn:aws:redshift-serverless:{REGION}:{ACCOUNT_B_ID}:workgroup/{WORKGROUP_NAME}",
            "usageType": "serverless-compute",
            "breachAction": "deactivate"
        }
    ]

    for limit in limits:
        try:
            rs_serverless.create_usage_limit(
                resourceArn=limit["resourceArn"],
                usageType=limit["usageType"],
                amount=limit["amount"],
                period=limit["period"],
                breachAction=limit["breachAction"]
            )
            print(f"✅ Usage limit set: {limit['amount']} RPU-hrs/{limit['period']} (action: {limit['breachAction']})")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Limit already exists: {limit['period']}")
            else:
                print(f"⚠️  Could not set limit: {str(e)[:100]}")


def restore_from_provisioned_snapshot():
    """
    Restore provisioned cluster snapshot to Serverless namespace
    
    FLOW:
    1. Create snapshot of provisioned cluster
    2. Restore into Serverless namespace
    3. All tables, schemas, users, stored procedures transferred
    4. MVs may need recreation
    """
    redshift = boto3.client("redshift", region_name=REGION)
    rs_serverless = boto3.client("redshift-serverless", region_name=REGION)

    snapshot_id = f"migration-snapshot-{int(time.time())}"

    # Step 1: Create snapshot
    print(f"\n📸 Creating snapshot of provisioned cluster...")
    redshift.create_cluster_snapshot(
        SnapshotIdentifier=snapshot_id,
        ClusterIdentifier="quickcart-analytics-cluster",
        Tags=[{"Key": "Purpose", "Value": "ServerlessMigration"}]
    )

    # Wait for snapshot
    print(f"   Waiting for snapshot completion...")
    waiter = redshift.get_waiter("snapshot_available")
    waiter.wait(
        SnapshotIdentifier=snapshot_id,
        WaiterConfig={"Delay": 15, "MaxAttempts": 60}
    )
    print(f"   ✅ Snapshot ready: {snapshot_id}")

    # Step 2: Restore to Serverless namespace
    print(f"\n📦 Restoring snapshot to Serverless namespace...")
    try:
        rs_serverless.restore_from_snapshot(
            namespaceName=NAMESPACE_NAME,
            workgroupName=WORKGROUP_NAME,
            snapshotName=snapshot_id,
            ownerAccount=ACCOUNT_B_ID
        )
        print(f"   ✅ Restore initiated")

        # Wait for restore
        print(f"   Waiting for restore completion...")
        while True:
            time.sleep(30)
            desc = rs_serverless.get_namespace(namespaceName=NAMESPACE_NAME)
            status = desc["namespace"]["status"]
            print(f"   Status: {status}")
            if status == "AVAILABLE":
                print(f"   ✅ Restore complete!")
                break
            elif status in ["MODIFYING"]:
                continue
            else:
                raise Exception(f"Unexpected status: {status}")

    except Exception as e:
        print(f"   ⚠️  Restore note: {str(e)[:200]}")
        print(f"   → If namespace already has data, use manual restore via console")

    return snapshot_id


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 CREATING REDSHIFT SERVERLESS ENVIRONMENT")
    print("=" * 60)

    ns = create_namespace()
    print()
    wg = create_workgroup()
    print()
    set_usage_limits()

    print(f"\n{'='*60}")
    print("✅ SERVERLESS ENVIRONMENT READY")
    endpoint = wg.get("endpoint", {})
    print(f"   Namespace: {NAMESPACE_NAME}")
    print(f"   Workgroup: {WORKGROUP_NAME}")
    print(f"   Endpoint: {endpoint.get('address', 'pending')}:{endpoint.get('port', '5439')}")
    print(f"   Base RPU: 8 | Max RPU: 128")
    print(f"   Usage limits: 200 RPU-hrs/day, 3000 RPU-hrs/month")
    print(f"\n   To restore from provisioned: run restore_from_provisioned_snapshot()")
    print(f"{'='*60}")