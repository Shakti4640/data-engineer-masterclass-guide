# file: 67_03_update_connections.py
# Run from: Account B (222222222222)
# Purpose: Update all service connections from provisioned to Serverless endpoint

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"

# Old provisioned endpoint
OLD_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"

# New Serverless endpoint (get from workgroup description)
NEW_HOST = f"{WORKGROUP_NAME}.{ACCOUNT_B_ID}.{REGION}.redshift-serverless.amazonaws.com"
WORKGROUP_NAME = "quickcart-prod"
NEW_PORT = "5439"
DB_NAME = "analytics"
DB_USER = "etl_writer"
DB_PASS = "R3dsh1ftWr1t3r#2025"


def update_glue_connection():
    """
    Update Glue JDBC connection to point to Serverless endpoint
    
    KEY CHANGE: hostname in JDBC URL
    → Old: jdbc:redshift://cluster-name.xxxx.region.redshift.amazonaws.com:5439/db
    → New: jdbc:redshift://workgroup-name.account-id.region.redshift-serverless.amazonaws.com:5439/db
    """
    glue = boto3.client("glue", region_name=REGION)

    new_jdbc_url = f"jdbc:redshift://{NEW_HOST}:{NEW_PORT}/{DB_NAME}"

    try:
        glue.update_connection(
            Name="conn-redshift-analytics",
            ConnectionInput={
                "Name": "conn-redshift-analytics",
                "Description": "Redshift Serverless — quickcart-prod workgroup",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": new_jdbc_url,
                    "USERNAME": DB_USER,
                    "PASSWORD": DB_PASS
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": "subnet-0bbb2222glue",
                    "SecurityGroupIdList": ["sg-0bbb2222glueeni"],
                    "AvailabilityZone": "us-east-2a"
                }
            }
        )
        print(f"✅ Glue connection updated: conn-redshift-analytics")
        print(f"   New JDBC URL: {new_jdbc_url}")

    except Exception as e:
        print(f"❌ Glue connection update failed: {str(e)[:200]}")


def update_lambda_env_vars():
    """
    Update Lambda functions that use Redshift Data API
    → Change from ClusterIdentifier to WorkgroupName
    """
    lam = boto3.client("lambda", region_name=REGION)

    lambdas_to_update = [
        {
            "name": "refresh-materialized-views",
            "env_updates": {
                "REDSHIFT_WORKGROUP": WORKGROUP_NAME,
                "REDSHIFT_ENDPOINT_TYPE": "serverless"
                # Remove: CLUSTER_ID
            }
        },
        {
            "name": "pipeline-event-router",
            "env_updates": {
                "REDSHIFT_WORKGROUP": WORKGROUP_NAME,
                "REDSHIFT_ENDPOINT_TYPE": "serverless"
            }
        }
    ]

    for func in lambdas_to_update:
        try:
            # Get current config
            config = lam.get_function_configuration(FunctionName=func["name"])
            current_env = config.get("Environment", {}).get("Variables", {})

            # Merge updates
            current_env.update(func["env_updates"])

            # Remove old provisioned references
            current_env.pop("CLUSTER_ID", None)
            current_env.pop("CLUSTER_IDENTIFIER", None)

            lam.update_function_configuration(
                FunctionName=func["name"],
                Environment={"Variables": current_env}
            )
            print(f"✅ Lambda updated: {func['name']}")
            for k, v in func["env_updates"].items():
                print(f"   {k} = {v}")

        except lam.exceptions.ResourceNotFoundException:
            print(f"⚠️  Lambda not found: {func['name']} — skipping")
        except Exception as e:
            print(f"❌ Failed to update {func['name']}: {str(e)[:100]}")


def update_mv_refresh_lambda_code():
    """
    Update MV refresh Lambda to use Serverless Data API parameters
    
    KEY CHANGE:
    → Provisioned: execute_statement(ClusterIdentifier="cluster-name", Database="db", DbUser="user")
    → Serverless:  execute_statement(WorkgroupName="workgroup-name", Database="db")
    
    NOTE: Serverless Data API uses workgroup admin credentials automatically
    → No DbUser parameter needed
    → Authentication handled by namespace admin user
    """
    updated_lambda_code = '''
import boto3
import os
import time

REDSHIFT_CLIENT = boto3.client("redshift-data")
WORKGROUP_NAME = os.environ.get("REDSHIFT_WORKGROUP", "quickcart-prod")
DATABASE = "analytics"
SCHEMA = "quickcart_ops"
ENDPOINT_TYPE = os.environ.get("REDSHIFT_ENDPOINT_TYPE", "serverless")

MV_LIST = [
    "mv_daily_revenue", "mv_category_revenue", "mv_customer_tier_spend",
    "mv_hourly_volume", "mv_cumulative_revenue", "mv_top_customers",
    "mv_return_rate", "mv_new_vs_returning", "mv_weekly_aov",
    "mv_geo_revenue", "mv_order_funnel", "mv_forecast_vs_actual"
]


def execute_sql(sql):
    """
    Execute SQL against Redshift — handles both provisioned and Serverless
    """
    params = {
        "Database": DATABASE,
        "Sql": sql
    }

    if ENDPOINT_TYPE == "serverless":
        params["WorkgroupName"] = WORKGROUP_NAME
    else:
        params["ClusterIdentifier"] = WORKGROUP_NAME
        params["DbUser"] = "etl_writer"

    return REDSHIFT_CLIENT.execute_statement(**params)


def lambda_handler(event, context):
    results = {"success": [], "failed": []}
    statement_ids = {}

    # Submit all refresh statements
    for mv_name in MV_LIST:
        sql = f"REFRESH MATERIALIZED VIEW {SCHEMA}.{mv_name};"
        try:
            response = execute_sql(sql)
            statement_ids[mv_name] = response["Id"]
            print(f"Submitted: {mv_name} → {response['Id']}")
        except Exception as e:
            print(f"Submit failed: {mv_name} → {str(e)}")
            results["failed"].append({"mv": mv_name, "error": str(e)})

    # Poll for completion
    max_wait = 300
    elapsed = 0
    pending = dict(statement_ids)

    while pending and elapsed < max_wait:
        time.sleep(5)
        elapsed += 5

        completed = []
        for mv_name, stmt_id in pending.items():
            desc = REDSHIFT_CLIENT.describe_statement(Id=stmt_id)
            status = desc["Status"]

            if status == "FINISHED":
                duration = desc.get("Duration", 0) / 1000000
                print(f"✅ {mv_name}: refreshed in {duration:.2f}s")
                results["success"].append({"mv": mv_name, "duration_s": round(duration, 2)})
                completed.append(mv_name)
            elif status == "FAILED":
                error = desc.get("Error", "Unknown error")
                print(f"❌ {mv_name}: {error}")
                results["failed"].append({"mv": mv_name, "error": error})
                completed.append(mv_name)

        for mv in completed:
            del pending[mv]

    for mv_name in pending:
        results["failed"].append({"mv": mv_name, "error": f"Timeout after {max_wait}s"})

    return {
        "statusCode": 200 if len(results["failed"]) == 0 else 500,
        "refresh_results": results,
        "total_succeeded": len(results["success"]),
        "total_failed": len(results["failed"]),
        "all_success": len(results["failed"]) == 0,
        "endpoint_type": ENDPOINT_TYPE
    }
'''

    lam = boto3.client("lambda", region_name=REGION)

    import zipfile, io
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("lambda_function.py", updated_lambda_code)
    zip_buf.seek(0)

    try:
        lam.update_function_code(
            FunctionName="refresh-materialized-views",
            ZipFile=zip_buf.read()
        )
        print("✅ MV refresh Lambda code updated for Serverless compatibility")
    except Exception as e:
        print(f"❌ Failed to update Lambda code: {str(e)[:100]}")


def update_cdc_stored_procedure_call():
    """
    Update CDC Glue job's merge execution to use Serverless Data API
    
    CHANGE IN 65_03_cdc_glue_job.py execute_merge() function:
    → Old: ClusterIdentifier="quickcart-analytics-cluster"
    → New: WorkgroupName="quickcart-prod"
    
    This is a CODE CHANGE in the Glue script — upload new version
    """
    print("\n📝 CDC Glue job update required:")
    print("   In 65_03_cdc_glue_job.py → execute_merge() function:")
    print("   CHANGE:")
    print('     response = redshift_data.execute_statement(')
    print('         ClusterIdentifier="quickcart-analytics-cluster",')
    print("   TO:")
    print('     response = redshift_data.execute_statement(')
    print(f'         WorkgroupName="{WORKGROUP_NAME}",')
    print("")
    print("   Then re-upload script to S3:")
    print("   aws s3 cp 65_03_cdc_glue_job.py s3://quickcart-analytics-scripts/glue/cdc_mariadb_to_redshift.py")


def generate_connection_summary():
    """
    Print complete summary of all connection changes
    """
    print(f"\n{'='*70}")
    print("📋 CONNECTION UPDATE SUMMARY")
    print(f"{'='*70}")
    print(f"""
OLD PROVISIONED ENDPOINT:
  Host: {OLD_HOST}
  Port: 5439
  Database: analytics
  Auth: username/password

NEW SERVERLESS ENDPOINT:
  Host: {NEW_HOST}
  Port: 5439
  Database: analytics
  Auth: username/password (same credentials)

SERVICES TO UPDATE:
┌──────────────────────────────────┬────────────────────┬────────────────────┐
│ Service                          │ Config Location     │ Status             │
├──────────────────────────────────┼────────────────────┼────────────────────┤
│ Glue Connection                  │ conn-redshift-*     │ Updated ✅          │
│ MV Refresh Lambda                │ Env vars + code     │ Updated ✅          │
│ CDC Glue Job (Data API)          │ Script code         │ Manual update ⚠️    │
│ Step Function (Data API)         │ Lambda env vars     │ Updated ✅          │
│ Superset Dashboard               │ JDBC connection     │ Manual update ⚠️    │
│ Athena Federated (Redshift conn) │ Connector config    │ Manual update ⚠️    │
│ psycopg2 scripts (validation)    │ Python code         │ Manual update ⚠️    │
└──────────────────────────────────┴────────────────────┴────────────────────┘

IAM POLICY CHANGES:
  Old: redshift:GetClusterCredentials → arn:aws:redshift:*:*:cluster/*
  New: redshift-serverless:GetCredentials → arn:aws:redshift-serverless:*:*:workgroup/*

  Update IAM policies for:
  → GlueCrossAccountETLRole
  → LambdaMVRefreshRole
  → StepFunctionsEventDrivenPipelineRole
""")


def update_iam_policies():
    """
    Add Serverless permissions to existing IAM roles
    → Keep provisioned permissions during transition
    → Add Serverless permissions alongside
    """
    iam = boto3.client("iam")

    serverless_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "RedshiftServerlessDataAPI",
                "Effect": "Allow",
                "Action": [
                    "redshift-serverless:GetCredentials",
                    "redshift-serverless:GetWorkgroup",
                    "redshift-serverless:GetNamespace",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult",
                    "redshift-data:BatchExecuteStatement",
                    "redshift-data:CancelStatement"
                ],
                "Resource": [
                    f"arn:aws:redshift-serverless:{REGION}:{ACCOUNT_B_ID}:workgroup/{WORKGROUP_NAME}",
                    f"arn:aws:redshift-serverless:{REGION}:{ACCOUNT_B_ID}:namespace/{NAMESPACE_NAME}",
                    "*"
                ]
            }
        ]
    }

    roles_to_update = [
        "LambdaMVRefreshRole",
        "StepFunctionsEventDrivenPipelineRole",
        "GlueCrossAccountETLRole"
    ]

    for role_name in roles_to_update:
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName="RedshiftServerlessAccess",
                PolicyDocument=json.dumps(serverless_policy)
            )
            print(f"✅ IAM policy updated: {role_name} → Serverless permissions added")
        except iam.exceptions.NoSuchEntityException:
            print(f"⚠️  Role not found: {role_name} — skipping")
        except Exception as e:
            print(f"❌ Failed for {role_name}: {str(e)[:100]}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔄 UPDATING ALL CONNECTIONS FOR SERVERLESS")
    print("=" * 60)

    update_iam_policies()
    print()
    update_glue_connection()
    print()
    update_lambda_env_vars()
    print()
    update_mv_refresh_lambda_code()
    print()
    update_cdc_stored_procedure_call()
    print()
    generate_connection_summary()