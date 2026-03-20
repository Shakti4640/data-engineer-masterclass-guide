# file: 52_01_create_worker_iam_role.py
# Purpose: Create IAM role that EC2 worker instances will assume
# Builds on Project 6: EC2 Instance Profile concept

import boto3
import json
import time

REGION = "us-east-2"
ROLE_NAME = "QuickCart-SQS-Worker-Role"
INSTANCE_PROFILE_NAME = "QuickCart-SQS-Worker-Profile"


def create_worker_role():
    """
    Create IAM role with permissions for:
    → SQS: receive, delete, get attributes (queue processing)
    → S3: get objects (download data for processing)
    → CloudWatch: put metrics (worker heartbeat)
    → Logs: put log events (centralized logging)
    
    Trust policy: only EC2 can assume this role
    (IAM fundamentals from Project 6 and Project 7)
    """
    iam_client = boto3.client("iam")

    # --- Trust Policy: Allow EC2 to assume this role ---
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # --- Permission Policy ---
    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "SQSProcessing",
                "Effect": "Allow",
                "Action": [
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:DeleteMessageBatch",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                    "sqs:ChangeMessageVisibility"
                ],
                "Resource": "arn:aws:sqs:*:*:quickcart-*"
            },
            {
                "Sid": "S3ReadAccess",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::quickcart-*",
                    "arn:aws:s3:::quickcart-*/*"
                ]
            },
            {
                "Sid": "CloudWatchMetrics",
                "Effect": "Allow",
                "Action": [
                    "cloudwatch:PutMetricData"
                ],
                "Resource": "*"
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": "arn:aws:logs:*:*:log-group:/quickcart/*"
            }
        ]
    }

    try:
        # Create role
        iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for QuickCart SQS worker EC2 instances",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"}
            ]
        )
        print(f"✅ IAM Role created: {ROLE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM Role already exists: {ROLE_NAME}")

    # Attach inline policy
    iam_client.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="SQSWorkerPermissions",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached")

    # Create instance profile
    try:
        iam_client.create_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME
        )
        print(f"✅ Instance profile created: {INSTANCE_PROFILE_NAME}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Instance profile already exists: {INSTANCE_PROFILE_NAME}")

    # Add role to instance profile
    try:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=INSTANCE_PROFILE_NAME,
            RoleName=ROLE_NAME
        )
        print(f"✅ Role added to instance profile")
    except iam_client.exceptions.LimitExceededException:
        print(f"ℹ️  Role already in instance profile")

    # Wait for propagation (IAM is eventually consistent)
    print("⏳ Waiting 10 seconds for IAM propagation...")
    time.sleep(10)

    # Get instance profile ARN
    response = iam_client.get_instance_profile(
        InstanceProfileName=INSTANCE_PROFILE_NAME
    )
    profile_arn = response["InstanceProfile"]["Arn"]
    print(f"✅ Instance Profile ARN: {profile_arn}")

    return profile_arn


if __name__ == "__main__":
    create_worker_role()