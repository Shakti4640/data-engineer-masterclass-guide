# file: 55_01_create_step_functions_role.py
# Purpose: Create IAM role that Step Functions uses to call other AWS services

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
ROLE_NAME = "QuickCart-StepFunctions-Pipeline-Role"


def create_step_functions_role():
    """
    Step Functions needs permission to:
    → Start and monitor Glue jobs/crawlers
    → Start and monitor Athena queries
    → Start and monitor DataBrew jobs
    → Publish to SNS topics
    → Invoke Lambda functions
    → Write to CloudWatch Logs
    """
    iam_client = boto3.client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "states.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "GlueAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                    "glue:StartCrawler",
                    "glue:GetCrawler"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AthenaAccess",
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution"
                ],
                "Resource": "*"
            },
            {
                "Sid": "AthenaS3Access",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:GetBucketLocation",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::quickcart-*",
                    f"arn:aws:s3:::quickcart-*/*"
                ]
            },
            {
                "Sid": "DataBrewAccess",
                "Effect": "Allow",
                "Action": [
                    "databrew:StartJobRun",
                    "databrew:DescribeJobRun",
                    "databrew:ListJobRuns"
                ],
                "Resource": "*"
            },
            {
                "Sid": "SNSPublish",
                "Effect": "Allow",
                "Action": [
                    "sns:Publish"
                ],
                "Resource": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:quickcart-*"
            },
            {
                "Sid": "LambdaInvoke",
                "Effect": "Allow",
                "Action": [
                    "lambda:InvokeFunction"
                ],
                "Resource": f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:quickcart-*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "*"
            },
            {
                "Sid": "EventBridgePutEvents",
                "Effect": "Allow",
                "Action": [
                    "events:PutTargets",
                    "events:PutRule",
                    "events:DescribeRule"
                ],
                "Resource": f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Step Functions to orchestrate QuickCart pipeline",
            Tags=[
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Purpose", "Value": "Pipeline-Orchestration"}
            ]
        )
        print(f"✅ IAM Role created: {ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM Role already exists: {ROLE_NAME}")

    iam_client.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="StepFunctionsPipelinePermissions",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached")

    role = iam_client.get_role(RoleName=ROLE_NAME)
    role_arn = role["Role"]["Arn"]
    print(f"✅ Role ARN: {role_arn}")

    time.sleep(10)
    return role_arn


if __name__ == "__main__":
    create_step_functions_role()