# file: 41_09_cleanup.py
# Purpose: Remove all resources created in this project

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]


def cleanup():
    """Remove all Project 41 resources in reverse dependency order"""
    print("🧹 CLEANING UP PROJECT 41 RESOURCES")
    print("=" * 50)

    # 1. Delete EventBridge rule + targets
    events_client = boto3.client("events", region_name=REGION)
    rule_name = "quickcart-s3-file-arrival-trigger"
    try:
        events_client.remove_targets(Rule=rule_name, Ids=["StepFunctionsETLPipeline"])
        events_client.delete_rule(Name=rule_name)
        print(f"✅ Deleted EventBridge rule: {rule_name}")
    except Exception as e:
        print(f"⏭️  EventBridge rule: {e}")

    # 2. Delete Step Functions state machine
    sfn_client = boto3.client("stepfunctions", region_name=REGION)
    sm_arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-etl-pipeline"
    try:
        sfn_client.delete_state_machine(stateMachineArn=sm_arn)
        print(f"✅ Deleted State Machine: quickcart-etl-pipeline")
    except Exception as e:
        print(f"⏭️  State Machine: {e}")

    # 3. Delete Glue jobs and crawlers
    glue_client = boto3.client("glue", region_name=REGION)
    for entity in ["orders", "customers", "products"]:
        try:
            glue_client.delete_job(JobName=f"quickcart-{entity}-etl")
            print(f"✅ Deleted Glue Job: quickcart-{entity}-etl")
        except Exception as e:
            print(f"⏭️  Glue Job {entity}: {e}")

        try:
            glue_client.delete_crawler(Name=f"quickcart-{entity}-crawler")
            print(f"✅ Deleted Crawler: quickcart-{entity}-crawler")
        except Exception as e:
            print(f"⏭️  Crawler {entity}: {e}")

    # 4. Delete IAM roles (must delete inline policies first)
    iam_client = boto3.client("iam")
    roles = [
        "quickcart-eventbridge-to-stepfn-role",
        "quickcart-stepfn-etl-pipeline-role"
    ]
    for role_name in roles:
        try:
            # Delete inline policies
            policies = iam_client.list_role_policies(RoleName=role_name)
            for policy_name in policies["PolicyNames"]:
                iam_client.delete_role_policy(
                    RoleName=role_name,
                    PolicyName=policy_name
                )
            iam_client.delete_role(RoleName=role_name)
            print(f"✅ Deleted IAM Role: {role_name}")
        except Exception as e:
            print(f"⏭️  IAM Role {role_name}: {e}")

    # NOTE: NOT deleting S3 bucket, SNS topic, or Glue database
    # → These are shared with other projects

    print("\n✅ CLEANUP COMPLETE")


if __name__ == "__main__":
    cleanup()