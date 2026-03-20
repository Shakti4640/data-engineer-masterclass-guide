# file: 41_02_create_iam_roles.py
# Purpose: Create IAM roles for EventBridge and Step Functions
# Concepts from Project 7 (IAM Deep Dive) applied here

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

# --- ROLE NAMES ---
EVENTBRIDGE_ROLE_NAME = "quickcart-eventbridge-to-stepfn-role"
STEPFN_ROLE_NAME = "quickcart-stepfn-etl-pipeline-role"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-pipeline-alerts"
STATE_MACHINE_ARN_PATTERN = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:quickcart-etl-pipeline"


def create_eventbridge_role(iam_client):
    """
    EventBridge needs permission to START Step Functions executions.
    
    Trust Policy: allows events.amazonaws.com to assume this role
    Permission: states:StartExecution on our specific state machine
    """
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "events.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "states:StartExecution",
                "Resource": STATE_MACHINE_ARN_PATTERN
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=EVENTBRIDGE_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows EventBridge to trigger Step Functions ETL pipeline",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart-ETL"}
            ]
        )
        print(f"✅ Created role: {EVENTBRIDGE_ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Role already exists: {EVENTBRIDGE_ROLE_NAME}")

    iam_client.put_role_policy(
        RoleName=EVENTBRIDGE_ROLE_NAME,
        PolicyName="AllowStartStepFunction",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"   ✅ Inline policy attached")

    return f"arn:aws:iam::{ACCOUNT_ID}:role/{EVENTBRIDGE_ROLE_NAME}"


def create_stepfunctions_role(iam_client):
    """
    Step Functions needs permissions to:
    1. Start/monitor Glue Crawlers
    2. Start/monitor Glue ETL Jobs
    3. Invoke Lambda (for Redshift COPY step)
    4. Publish to SNS (for notifications)
    5. Write logs to CloudWatch
    6. Manage EventBridge events (for .sync pattern internal polling)
    """
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                # Glue Crawler operations
                "Effect": "Allow",
                "Action": [
                    "glue:StartCrawler",
                    "glue:GetCrawler"
                ],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:crawler/quickcart-*"
            },
            {
                # Glue ETL Job operations
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun"
                ],
                "Resource": f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/quickcart-*"
            },
            {
                # SNS for notifications
                "Effect": "Allow",
                "Action": "sns:Publish",
                "Resource": SNS_TOPIC_ARN
            },
            {
                # CloudWatch Logs for Step Functions logging
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups"
                ],
                "Resource": "*"
            },
            {
                # Required for .sync integration pattern
                # Step Functions uses EventBridge internally to track job completion
                "Effect": "Allow",
                "Action": [
                    "events:PutTargets",
                    "events:PutRule",
                    "events:DescribeRule"
                ],
                "Resource": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/StepFunctionsGetEventsForGlue*"
            },
            {
                # Lambda invocation (for Redshift COPY step)
                "Effect": "Allow",
                "Action": "lambda:InvokeFunction",
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:quickcart-*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=STEPFN_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Allows Step Functions to orchestrate Glue + SNS + Lambda",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Project", "Value": "QuickCart-ETL"}
            ]
        )
        print(f"✅ Created role: {STEPFN_ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Role already exists: {STEPFN_ROLE_NAME}")

    iam_client.put_role_policy(
        RoleName=STEPFN_ROLE_NAME,
        PolicyName="StepFnETLPipelinePermissions",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"   ✅ Inline policy attached")

    return f"arn:aws:iam::{ACCOUNT_ID}:role/{STEPFN_ROLE_NAME}"


if __name__ == "__main__":
    iam_client = boto3.client("iam")

    eb_role_arn = create_eventbridge_role(iam_client)
    sfn_role_arn = create_stepfunctions_role(iam_client)

    print(f"\n📋 Role ARNs:")
    print(f"   EventBridge Role: {eb_role_arn}")
    print(f"   StepFunctions Role: {sfn_role_arn}")

    print(f"\n⏳ Waiting 10 seconds for IAM propagation...")
    time.sleep(10)
    print(f"   ✅ Ready to create State Machine")