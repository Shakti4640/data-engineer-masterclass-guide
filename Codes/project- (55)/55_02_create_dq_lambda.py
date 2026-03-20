# file: 55_02_create_dq_lambda.py
# Purpose: Create Lambda function that validates data quality between ETL steps

import boto3
import json
import zipfile
import os
import time

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
FUNCTION_NAME = "quickcart-dq-validator"
ROLE_NAME = "QuickCart-DQ-Lambda-Role"


def create_lambda_role():
    """Create IAM role for DQ validation Lambda"""
    iam_client = boto3.client("iam")

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": ["arn:aws:s3:::quickcart-*", "arn:aws:s3:::quickcart-*/*"]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetDatabase"
                ],
                "Resource": "*"
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

    try:
        iam_client.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for DQ validation Lambda"
        )
    except iam_client.exceptions.EntityAlreadyExistsException:
        pass

    iam_client.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName="DQLambdaPermissions",
        PolicyDocument=json.dumps(permission_policy)
    )

    role = iam_client.get_role(RoleName=ROLE_NAME)
    time.sleep(10)
    return role["Role"]["Arn"]


def create_lambda_code():
    """Create Lambda function code as zip"""

    lambda_code = '''
import json
import boto3
import time

def lambda_handler(event, context):
    """
    Validate data quality after Glue ETL
    
    Called by Step Functions after ETL completes
    Returns: quality_passed (bool), details (dict)
    
    CHECKS:
    1. Row count >= minimum threshold
    2. Null percentage <= maximum allowed
    3. Duplicate count = 0
    4. Output files exist in S3
    """
    print(f"DQ Validator received event: {json.dumps(event)}")
    
    # Extract pipeline context
    pipeline = event.get("pipeline", "nightly-etl")
    execution_date = event.get("execution_date", "unknown")
    
    # Simulated DQ checks (in production: query Athena or read S3 stats)
    checks = {
        "row_count": {
            "actual": 245000,
            "threshold": 100000,
            "passed": True
        },
        "null_percentage": {
            "actual": 2.3,
            "threshold": 5.0,
            "passed": True
        },
        "duplicate_count": {
            "actual": 0,
            "threshold": 0,
            "passed": True
        },
        "output_files_exist": {
            "actual": True,
            "threshold": True,
            "passed": True
        }
    }
    
    all_passed = all(check["passed"] for check in checks.values())
    failed_checks = [name for name, check in checks.items() if not check["passed"]]
    
    result = {
        "quality_passed": all_passed,
        "pipeline": pipeline,
        "execution_date": execution_date,
        "checks": checks,
        "failed_checks": failed_checks,
        "summary": f"{sum(1 for c in checks.values() if c['passed'])}/{len(checks)} checks passed"
    }
    
    print(f"DQ Result: {json.dumps(result)}")
    return result
'''

    # Write to temp file and zip
    os.makedirs("/tmp/lambda_pkg", exist_ok=True)
    with open("/tmp/lambda_pkg/lambda_function.py", "w") as f:
        f.write(lambda_code)

    zip_path = "/tmp/dq_validator.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("/tmp/lambda_pkg/lambda_function.py", "lambda_function.py")

    with open(zip_path, "rb") as f:
        return f.read()


def create_dq_lambda():
    """Create the DQ validation Lambda function"""
    lambda_client = boto3.client("lambda", region_name=REGION)
    role_arn = create_lambda_role()
    code_bytes = create_lambda_code()

    print("=" * 70)
    print("🔧 CREATING DATA QUALITY VALIDATION LAMBDA")
    print("=" * 70)

    try:
        lambda_client.create_function(
            FunctionName=FUNCTION_NAME,
            Runtime="python3.12",
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": code_bytes},
            Timeout=300,
            MemorySize=256,
            Description="Validates data quality between pipeline steps",
            Tags={
                "Team": "Data-Engineering",
                "Pipeline": "nightly-etl"
            }
        )
        print(f"✅ Lambda created: {FUNCTION_NAME}")
    except lambda_client.exceptions.ResourceConflictException:
        lambda_client.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=code_bytes
        )
        print(f"ℹ️  Lambda updated: {FUNCTION_NAME}")

    # Get ARN
    func = lambda_client.get_function(FunctionName=FUNCTION_NAME)
    func_arn = func["Configuration"]["FunctionArn"]
    print(f"✅ Lambda ARN: {func_arn}")
    return func_arn


if __name__ == "__main__":
    create_dq_lambda()