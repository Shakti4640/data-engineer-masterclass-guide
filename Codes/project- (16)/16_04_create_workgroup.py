# file: 16_04_create_workgroup.py
# Purpose: Create Athena Workgroup with cost controls
# WHY: Prevent runaway queries from scanning terabytes accidentally

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-2"
ATHENA_OUTPUT = "s3://quickcart-athena-results/"


def create_cost_controlled_workgroup():
    athena_client = boto3.client("athena", region_name=REGION)

    workgroup_name = "quickcart-analysts"

    try:
        athena_client.create_work_group(
            Name=workgroup_name,
            Configuration={
                # --- RESULT CONFIGURATION ---
                "ResultConfiguration": {
                    "OutputLocation": ATHENA_OUTPUT,
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    }
                },

                # --- ENFORCE WORKGROUP SETTINGS ---
                # If True: users CANNOT override output location
                # Ensures all results go to controlled bucket
                "EnforceWorkGroupConfiguration": True,

                # --- PER-QUERY DATA SCAN LIMIT ---
                # Kills any query that tries to scan > 1 GB
                # Protects against: SELECT * FROM huge_table (no WHERE)
                # Value in BYTES: 1 GB = 1,073,741,824
                "BytesScannedCutoffPerQuery": 1_073_741_824,

                # --- PUBLISH METRICS TO CLOUDWATCH ---
                # Enables monitoring: queries run, data scanned, errors
                # Used in Project 54 for alerting
                "PublishCloudWatchMetricsEnabled": True,

                # --- REQUESTER PAYS ---
                # If source bucket is requester-pays, this must be True
                "RequesterPaysEnabled": False
            },
            Description="Workgroup for QuickCart analysts with 1GB scan limit",
            Tags=[
                {"Key": "Team", "Value": "Analytics"},
                {"Key": "CostLimit", "Value": "1GB-per-query"}
            ]
        )
        print(f"✅ Workgroup '{workgroup_name}' created")
        print(f"   → Max scan per query: 1 GB")
        print(f"   → Output: {ATHENA_OUTPUT}")
        print(f"   → CloudWatch metrics: enabled")
        print(f"   → Settings enforced: yes")

    except ClientError as e:
        if "already exists" in str(e):
            print(f"ℹ️  Workgroup '{workgroup_name}' already exists")
        else:
            raise

    # --- SHOW HOW TO USE WORKGROUP IN QUERIES ---
    print("\n📝 To use this workgroup in queries:")
    print("   → Console: Select workgroup dropdown → 'quickcart-analysts'")
    print("   → boto3: start_query_execution(WorkGroup='quickcart-analysts')")
    print("   → If query scans > 1 GB → auto-cancelled with error")


def demonstrate_workgroup_query():
    """Run a query using the cost-controlled workgroup"""
    athena_client = boto3.client("athena", region_name=REGION)

    import time

    print("\n" + "=" * 70)
    print("Running query through cost-controlled workgroup")
    print("=" * 70)

    response = athena_client.start_query_execution(
        QueryString="SELECT COUNT(*) as total FROM quickcart_raw.orders",
        # --- KEY DIFFERENCE: WorkGroup parameter ---
        WorkGroup="quickcart-analysts"
        # NOTE: No ResultConfiguration needed — workgroup provides it
        # NOTE: Cannot override output location (EnforceWorkGroupConfiguration=True)
    )
    query_id = response["QueryExecutionId"]
    print(f"   Query ID: {query_id}")

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            stats = status["QueryExecution"]["Statistics"]
            print(f"   ✅ Scanned: {stats['DataScannedInBytes']:,} bytes")
            print(f"   ✅ Under 1 GB limit — query allowed")
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", ""
            )
            if "bytes scanned limit" in reason.lower():
                print(f"   🛑 Query KILLED — exceeded 1 GB scan limit")
                print(f"   → This is the workgroup protection working correctly")
            else:
                print(f"   ❌ {state}: {reason}")
            break
        time.sleep(1)


if __name__ == "__main__":
    create_cost_controlled_workgroup()
    demonstrate_workgroup_query()