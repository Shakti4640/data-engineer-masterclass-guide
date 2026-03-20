# file: 63_03_integrate_step_functions.py
# Run from: Account B
# Purpose: Add MV refresh step to existing ETL orchestration
# Builds on: Project 55 (Step Functions orchestration)

import boto3
import json

REGION = "us-east-2"
ACCOUNT_B_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_mv_refresh_lambda():
    """
    Lambda function that triggers MV refresh via Redshift Data API
    
    WHY LAMBDA + DATA API (not direct psycopg2):
    → Step Functions integrates natively with Lambda
    → Redshift Data API is serverless — no VPC/connection management
    → Async execution: submit SQL → poll for result
    → No psycopg2 dependency in Lambda
    → Better for orchestration than direct JDBC
    """
    lambda_code = '''
import boto3
import time

REDSHIFT_CLIENT = boto3.client("redshift-data")
CLUSTER_ID = "quickcart-analytics-cluster"
DATABASE = "analytics"
DB_USER = "etl_writer"
SCHEMA = "quickcart_ops"

MV_LIST = [
    "mv_daily_revenue",
    "mv_category_revenue",
    "mv_customer_tier_spend",
    "mv_hourly_volume",
    "mv_cumulative_revenue",
    "mv_top_customers",
    "mv_return_rate",
    "mv_new_vs_returning",
    "mv_weekly_aov",
    "mv_geo_revenue",
    "mv_order_funnel",
    "mv_forecast_vs_actual"
]


def lambda_handler(event, context):
    """
    Refresh all MVs using Redshift Data API
    
    FLOW:
    1. Submit REFRESH for each MV
    2. Collect statement IDs
    3. Poll until all complete
    4. Return results
    """
    results = {"success": [], "failed": []}
    statement_ids = {}
    
    # Submit all refresh statements (parallel execution)
    for mv_name in MV_LIST:
        sql = f"REFRESH MATERIALIZED VIEW {SCHEMA}.{mv_name};"
        
        try:
            response = REDSHIFT_CLIENT.execute_statement(
                ClusterIdentifier=CLUSTER_ID,
                Database=DATABASE,
                DbUser=DB_USER,
                Sql=sql
            )
            statement_ids[mv_name] = response["Id"]
            print(f"Submitted: {mv_name} → {response['Id']}")
        except Exception as e:
            print(f"Submit failed: {mv_name} → {str(e)}")
            results["failed"].append({"mv": mv_name, "error": str(e)})
    
    # Poll for completion
    max_wait = 300  # 5 minutes max
    poll_interval = 5
    elapsed = 0
    pending = dict(statement_ids)
    
    while pending and elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval
        
        completed = []
        for mv_name, stmt_id in pending.items():
            desc = REDSHIFT_CLIENT.describe_statement(Id=stmt_id)
            status = desc["Status"]
            
            if status == "FINISHED":
                duration = desc.get("Duration", 0) / 1000000  # nanosec to sec
                print(f"✅ {mv_name}: refreshed in {duration:.2f}s")
                results["success"].append({
                    "mv": mv_name,
                    "duration_s": round(duration, 2)
                })
                completed.append(mv_name)
                
            elif status == "FAILED":
                error = desc.get("Error", "Unknown error")
                print(f"❌ {mv_name}: {error}")
                results["failed"].append({
                    "mv": mv_name,
                    "error": error
                })
                completed.append(mv_name)
            
            # STARTED, PICKED, SUBMITTED = still running
        
        for mv in completed:
            del pending[mv]
        
        print(f"Elapsed: {elapsed}s — {len(pending)} still running")
    
    # Timeout remaining
    for mv_name in pending:
        results["failed"].append({
            "mv": mv_name,
            "error": f"Timeout after {max_wait}s"
        })
    
    # Determine overall status
    all_success = len(results["failed"]) == 0
    
    return {
        "statusCode": 200 if all_success else 500,
        "refresh_results": results,
        "total_succeeded": len(results["success"]),
        "total_failed": len(results["failed"]),
        "all_success": all_success
    }
'''

    # Create Lambda function
    lam = boto3.client("lambda", region_name=REGION)
    iam = boto3.client("iam")

    # IAM Role
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "redshift-data:ExecuteStatement",
                    "redshift-data:DescribeStatement",
                    "redshift-data:GetStatementResult"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": ["redshift:GetClusterCredentials"],
                "Resource": [
                    f"arn:aws:redshift:{REGION}:{ACCOUNT_B_ID}:cluster:quickcart-analytics-cluster",
                    f"arn:aws:redshift:{REGION}:{ACCOUNT_B_ID}:dbuser:quickcart-analytics-cluster/etl_writer",
                    f"arn:aws:redshift:{REGION}:{ACCOUNT_B_ID}:dbname:quickcart-analytics-cluster/analytics"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }

    role_name = "LambdaMVRefreshRole"
    try:
        iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust)
        )
    except iam.exceptions.EntityAlreadyExistsException:
        pass

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="MVRefreshPolicy",
        PolicyDocument=json.dumps(policy)
    )

    role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{role_name}"

    # Package and create Lambda
    import zipfile, io
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("lambda_function.py", lambda_code)
    zip_buf.seek(0)

    import time
    time.sleep(10)  # Wait for role propagation

    try:
        lam.create_function(
            FunctionName="refresh-materialized-views",
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_buf.read()},
            Timeout=300,
            MemorySize=128,
            Description="Refreshes all Redshift MVs via Data API"
        )
        print("✅ Lambda created: refresh-materialized-views")
    except lam.exceptions.ResourceConflictException:
        zip_buf.seek(0)
        lam.update_function_code(
            FunctionName="refresh-materialized-views",
            ZipFile=zip_buf.read()
        )
        print("✅ Lambda updated: refresh-materialized-views")


def create_enhanced_step_function():
    """
    Enhanced Step Function that includes MV refresh after ETL
    
    FLOW:
    1. Start Glue ETL Job (Project 61 — cross-account MariaDB → Redshift)
    2. Wait for Glue job completion
    3. Invoke MV Refresh Lambda
    4. Check refresh results
    5. If all success → SNS success notification
    6. If any failure → SNS failure notification + retry
    
    Builds on: Project 55 (Step Functions orchestration)
    """
    sfn = boto3.client("stepfunctions", region_name=REGION)

    state_machine_definition = {
        "Comment": "ETL Pipeline: MariaDB → Redshift → Refresh MVs → Notify",
        "StartAt": "StartGlueETL",
        "States": {
            "StartGlueETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "cross-account-mariadb-to-redshift",
                    "Arguments": {
                        "--target_schema": "quickcart_ops"
                    }
                },
                "ResultPath": "$.glue_result",
                "Next": "WaitForDataSettlement",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyETLFailure",
                    "ResultPath": "$.error"
                }]
            },
            "WaitForDataSettlement": {
                "Comment": "Brief pause to ensure Redshift commits are visible",
                "Type": "Wait",
                "Seconds": 30,
                "Next": "RefreshMaterializedViews"
            },
            "RefreshMaterializedViews": {
                "Type": "Task",
                "Resource": "arn:aws:states:::lambda:invoke",
                "Parameters": {
                    "FunctionName": "refresh-materialized-views",
                    "Payload": {
                        "trigger": "step_functions",
                        "etl_run_id.$": "$.glue_result.JobRunId"
                    }
                },
                "ResultPath": "$.mv_result",
                "ResultSelector": {
                    "statusCode.$": "$.Payload.statusCode",
                    "total_succeeded.$": "$.Payload.total_succeeded",
                    "total_failed.$": "$.Payload.total_failed",
                    "all_success.$": "$.Payload.all_success"
                },
                "Next": "CheckMVRefreshResult",
                "Retry": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 2,
                    "BackoffRate": 2.0
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "NotifyMVRefreshFailure",
                    "ResultPath": "$.error"
                }]
            },
            "CheckMVRefreshResult": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.mv_result.all_success",
                        "BooleanEquals": True,
                        "Next": "NotifySuccess"
                    }
                ],
                "Default": "NotifyMVRefreshFailure"
            },
            "NotifySuccess": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-2:222222222222:data-platform-alerts",
                    "Subject": "✅ Pipeline Complete — ETL + MV Refresh Succeeded",
                    "Message.$": "States.Format('ETL and MV refresh completed successfully.\nMVs refreshed: {}\nDashboards serving fresh data.', $.mv_result.total_succeeded)"
                },
                "End": True
            },
            "NotifyETLFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-2:222222222222:data-platform-alerts",
                    "Subject": "❌ Pipeline FAILED — Glue ETL Error",
                    "Message.$": "States.Format('Glue ETL job failed. MVs NOT refreshed.\nError: {}', $.error.Cause)"
                },
                "End": True
            },
            "NotifyMVRefreshFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": "arn:aws:sns:us-east-2:222222222222:data-platform-alerts",
                    "Subject": "⚠️ Pipeline PARTIAL — ETL OK but MV Refresh Failed",
                    "Message.$": "States.Format('ETL succeeded but {} MV(s) failed to refresh.\nDashboards may show STALE data.\nAction: manually refresh failed MVs.', $.mv_result.total_failed)"
                },
                "End": True
            }
        }
    }

    # Create IAM role for Step Functions
    iam = boto3.client("iam")
    sfn_role_name = "StepFunctionsETLMVPipelineRole"

    sfn_trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "states.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    sfn_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns",
                           "glue:BatchStopJobRun"],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_B_ID}:job/*"
            },
            {
                "Effect": "Allow",
                "Action": ["lambda:InvokeFunction"],
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT_B_ID}:function:refresh-materialized-views"
            },
            {
                "Effect": "Allow",
                "Action": ["sns:Publish"],
                "Resource": f"arn:aws:sns:{REGION}:{ACCOUNT_B_ID}:data-platform-alerts"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "events:PutTargets", "events:PutRule",
                    "events:DescribeRule"
                ],
                "Resource": f"arn:aws:events:{REGION}:{ACCOUNT_B_ID}:rule/*"
            }
        ]
    }

    try:
        iam.create_role(
            RoleName=sfn_role_name,
            AssumeRolePolicyDocument=json.dumps(sfn_trust)
        )
    except iam.exceptions.EntityAlreadyExistsException:
        pass

    iam.put_role_policy(
        RoleName=sfn_role_name,
        PolicyName="ETLMVPipelinePolicy",
        PolicyDocument=json.dumps(sfn_policy)
    )

    sfn_role_arn = f"arn:aws:iam::{ACCOUNT_B_ID}:role/{sfn_role_name}"

    import time
    time.sleep(10)

    # Create State Machine
    try:
        response = sfn.create_state_machine(
            name="etl-and-mv-refresh-pipeline",
            definition=json.dumps(state_machine_definition),
            roleArn=sfn_role_arn,
            type="STANDARD",
            tags=[
                {"key": "Environment", "value": "Production"},
                {"key": "Pipeline", "value": "ETL-MV-Refresh"}
            ]
        )
        print(f"✅ Step Function created: {response['stateMachineArn']}")
    except sfn.exceptions.StateMachineAlreadyExists:
        # Update existing
        sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_B_ID}:stateMachine:etl-and-mv-refresh-pipeline"
        sfn.update_state_machine(
            stateMachineArn=sm_arn,
            definition=json.dumps(state_machine_definition),
            roleArn=sfn_role_arn
        )
        print(f"✅ Step Function updated: {sm_arn}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 DEPLOYING MV REFRESH INTEGRATION")
    print("=" * 60)

    create_mv_refresh_lambda()
    print()
    create_enhanced_step_function()

    print(f"\n{'='*60}")
    print("✅ DEPLOYMENT COMPLETE")
    print("   Pipeline flow:")
    print("   EventBridge (2 AM) → Step Functions →")
    print("     → Glue ETL (MariaDB → Redshift) →")
    print("     → Wait 30s →")
    print("     → Lambda (Refresh 12 MVs) →")
    print("     → SNS Notification")
    print(f"{'='*60}")