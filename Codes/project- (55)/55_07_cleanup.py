# file: 55_07_cleanup.py
# Purpose: Remove all Step Functions infrastructure

import boto3
import json
import time

REGION = "us-east-2"
STATE_MACHINE_NAME = "quickcart-nightly-pipeline"
RULE_NAME = "quickcart-nightly-pipeline-trigger"
LAMBDA_NAME = "quickcart-dq-validator"
SF_ROLE_NAME = "QuickCart-StepFunctions-Pipeline-Role"
EB_ROLE_NAME = "QuickCart-EventBridge-StepFunctions-Role"
LAMBDA_ROLE_NAME = "QuickCart-DQ-Lambda-Role"


def cleanup_all():
    """Remove all Step Functions, EventBridge, Lambda, IAM resources"""
    sfn_client = boto3.client("stepfunctions", region_name=REGION)
    events_client = boto3.client("events", region_name=REGION)
    lambda_client = boto3.client("lambda", region_name=REGION)
    iam_client = boto3.client("iam")
    logs_client = boto3.client("logs", region_name=REGION)

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 55 — STEP FUNCTIONS PIPELINE")
    print("=" * 70)

    # --- Step 1: Remove EventBridge targets and rule ---
    print("\n📌 Step 1: Removing EventBridge schedule...")
    try:
        events_client.remove_targets(Rule=RULE_NAME, Ids=["StepFunctionsTarget"])
        events_client.delete_rule(Name=RULE_NAME)
        print(f"  ✅ EventBridge rule deleted: {RULE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 2: Stop running executions ---
    print("\n📌 Step 2: Stopping running executions...")
    try:
        response = sfn_client.list_state_machines()
        for sm in response["stateMachines"]:
            if sm["name"] == STATE_MACHINE_NAME:
                sm_arn = sm["stateMachineArn"]
                executions = sfn_client.list_executions(
                    stateMachineArn=sm_arn,
                    statusFilter="RUNNING"
                )
                for execution in executions.get("executions", []):
                    sfn_client.stop_execution(
                        executionArn=execution["executionArn"],
                        cause="Cleanup — stopping for resource deletion"
                    )
                    print(f"  ⏹️  Stopped: {execution['name']}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 3: Delete state machine ---
    print("\n📌 Step 3: Deleting state machine...")
    try:
        response = sfn_client.list_state_machines()
        for sm in response["stateMachines"]:
            if sm["name"] == STATE_MACHINE_NAME:
                sfn_client.delete_state_machine(
                    stateMachineArn=sm["stateMachineArn"]
                )
                print(f"  ✅ State machine deleted: {STATE_MACHINE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 4: Delete Lambda ---
    print("\n📌 Step 4: Deleting Lambda function...")
    try:
        lambda_client.delete_function(FunctionName=LAMBDA_NAME)
        print(f"  ✅ Lambda deleted: {LAMBDA_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 5: Delete IAM roles ---
    print("\n📌 Step 5: Deleting IAM roles...")
    for role_name, policy_name in [
        (SF_ROLE_NAME, "StepFunctionsPipelinePermissions"),
        (EB_ROLE_NAME, "InvokeStepFunctions"),
        (LAMBDA_ROLE_NAME, "DQLambdaPermissions")
    ]:
        try:
            iam_client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)
            iam_client.delete_role(RoleName=role_name)
            print(f"  ✅ Role deleted: {role_name}")
        except Exception as e:
            print(f"  ⚠️  {role_name}: {e}")

    # --- Step 6: Delete log groups ---
    print("\n📌 Step 6: Deleting log groups...")
    log_groups_to_delete = [
        f"/aws/stepfunctions/{STATE_MACHINE_NAME}",
        f"/aws/lambda/{LAMBDA_NAME}"
    ]
    for lg in log_groups_to_delete:
        try:
            logs_client.delete_log_group(logGroupName=lg)
            print(f"  ✅ Log group deleted: {lg}")
        except Exception as e:
            print(f"  ⚠️  {lg}: {e}")

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 55 RESOURCES CLEANED UP")
    print("=" * 70)


if __name__ == "__main__":
    cleanup_all()