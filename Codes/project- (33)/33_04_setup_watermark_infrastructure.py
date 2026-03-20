# file: 33_04_setup_watermark_infrastructure.py
# Purpose: Create DynamoDB watermark table + update Glue job configs
# Run from: Your local machine

import boto3
import json

AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

DDB_TABLE_NAME = "glue-job-watermarks"
S3_BUCKET = "quickcart-datalake-prod"
GLUE_ROLE_NAME = "GlueMariaDBExtractorRole"

dynamodb = boto3.client("dynamodb", region_name=AWS_REGION)
iam = boto3.client("iam")
glue = boto3.client("glue", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create DynamoDB Watermark Table
# ═══════════════════════════════════════════════════════════════════
def create_watermark_table():
    """
    Create DynamoDB table for storing job watermarks
    
    Schema:
    → Partition Key: job_name (String) — which Glue job
    → Sort Key: source_table (String) — which table within that job
    → Together: uniquely identify a watermark
    
    Example entries:
    ┌──────────────────────────┬──────────────┬─────────────────────┐
    │ job_name (PK)            │ source_table │ watermark_value     │
    ├──────────────────────────┼──────────────┼─────────────────────┤
    │ quickcart-mariadb-incr   │ orders       │ 2025-01-15 23:59:59 │
    │ quickcart-mariadb-incr   │ customers    │ 2025-01-15 18:30:00 │
    │ quickcart-mariadb-incr   │ products     │ 2025-01-15 06:00:00 │
    │ partner-feed-processor   │ partner_A    │ feed_20250115.csv   │
    └──────────────────────────┴──────────────┴─────────────────────┘
    
    BILLING MODE: PAY_PER_REQUEST
    → No provisioned capacity to manage
    → ~$0.25 per million read/write requests
    → For watermarks: < $0.01/month (a few reads/writes per day)
    """
    print("\n📊 Creating DynamoDB watermark table...")

    try:
        dynamodb.create_table(
            TableName=DDB_TABLE_NAME,
            KeySchema=[
                {
                    "AttributeName": "job_name",
                    "KeyType": "HASH"        # Partition key
                },
                {
                    "AttributeName": "source_table",
                    "KeyType": "RANGE"       # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "job_name",
                    "AttributeType": "S"     # String
                },
                {
                    "AttributeName": "source_table",
                    "AttributeType": "S"     # String
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[
                {"Key": "Environment", "Value": "Production"},
                {"Key": "Team", "Value": "Data-Engineering"},
                {"Key": "Purpose", "Value": "ETL-Watermark-Tracking"}
            ]
        )
        print(f"   ✅ Table created: {DDB_TABLE_NAME}")

        # Wait for table to be active
        waiter = dynamodb.get_waiter("table_exists")
        waiter.wait(TableName=DDB_TABLE_NAME)
        print(f"   ✅ Table is ACTIVE")

    except dynamodb.exceptions.ResourceInUseException:
        print(f"   ℹ️  Table already exists: {DDB_TABLE_NAME}")

    # Enable point-in-time recovery (backup)
    try:
        dynamodb.update_continuous_backups(
            TableName=DDB_TABLE_NAME,
            PointInTimeRecoverySpecification={
                "PointInTimeRecoveryEnabled": True
            }
        )
        print(f"   ✅ Point-in-time recovery enabled")
    except Exception:
        print(f"   ℹ️  PITR already enabled or not available")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Grant Glue IAM Role Access to DynamoDB
# ═══════════════════════════════════════════════════════════════════
def update_glue_role_for_dynamodb():
    """
    Add DynamoDB permissions to Glue IAM Role
    
    Glue job needs to:
    → GetItem: read current watermark
    → PutItem: update watermark after processing
    → Scan: (optional) list all watermarks for monitoring
    """
    print("\n🔐 Updating Glue IAM Role for DynamoDB access...")

    ddb_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DynamoDBWatermarkAccess",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:Scan",
                    "dynamodb:Query"
                ],
                "Resource": [
                    f"arn:aws:dynamodb:{AWS_REGION}:{ACCOUNT_ID}:table/{DDB_TABLE_NAME}"
                ]
            }
        ]
    }

    iam.put_role_policy(
        RoleName=GLUE_ROLE_NAME,
        PolicyName="GlueDynamoDBWatermarkAccess",
        PolicyDocument=json.dumps(ddb_policy)
    )
    print(f"   ✅ DynamoDB policy added to {GLUE_ROLE_NAME}")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Create S3 Watermark State Directory
# ═══════════════════════════════════════════════════════════════════
def create_s3_watermark_directory():
    """
    Create S3 prefix for S3-based watermark files
    (Alternative to DynamoDB — for simpler use cases)
    """
    print("\n📁 Creating S3 watermark state directory...")

    # S3 doesn't need directory creation — just document the path
    prefix = "state/watermarks/"

    # Create a README marker file
    readme_content = json.dumps({
        "purpose": "Stores ETL job watermark state files",
        "format": "JSON files per job/table combination",
        "path_pattern": "state/watermarks/{job_name}/{source_table}.json",
        "created_at": datetime.now().isoformat(),
        "managed_by": "Data Engineering team"
    }, indent=2)

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}_README.json",
        Body=readme_content,
        ContentType="application/json"
    )
    print(f"   ✅ Watermark directory: s3://{S3_BUCKET}/{prefix}")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Update Existing Glue Jobs to Enable Bookmarks
# ═══════════════════════════════════════════════════════════════════
def enable_bookmarks_on_existing_jobs():
    """
    Update existing Glue jobs from Projects 25, 31 to use bookmarks
    
    IMPORTANT: Enabling bookmarks on a running job
    → First run after enabling: processes ALL files (no previous state)
    → Second run: processes only new files
    → This is expected — bookmark has no history before being enabled
    """
    print("\n⚙️  Enabling bookmarks on existing Glue jobs...")

    # Jobs that should use bookmarks (S3 source jobs)
    bookmark_jobs = {
        # Job from Project 25 — CSV to Parquet conversion
        "quickcart-csv-to-parquet": {
            "--job-bookmark-option": "job-bookmark-enable"
        }
    }

    # Jobs that should use custom watermark (JDBC source jobs)
    watermark_jobs = {
        # Job from Project 31 — MariaDB extraction
        "quickcart-mariadb-to-s3-parquet": {
            "--job-bookmark-option": "job-bookmark-disable",
            "--watermark_table": DDB_TABLE_NAME,
            "--watermark_column": "updated_at"
        }
    }

    # Jobs that use date partition filtering (already working)
    partition_jobs = {
        # Job from Project 32 — S3 to Redshift
        "quickcart-s3-to-redshift": {
            "--job-bookmark-option": "job-bookmark-disable"
            # Already uses --extraction_date parameter
        }
    }

    for job_name, new_args in {**bookmark_jobs, **watermark_jobs, **partition_jobs}.items():
        try:
            # Get current job definition
            response = glue.get_job(JobName=job_name)
            current_args = response["Job"].get("DefaultArguments", {})

            # Merge new arguments
            current_args.update(new_args)

            # Update job
            glue.update_job(
                JobName=job_name,
                JobUpdate={
                    "DefaultArguments": current_args
                }
            )
            bookmark_status = new_args.get("--job-bookmark-option", "unchanged")
            print(f"   ✅ {job_name}: bookmark={bookmark_status}")

        except glue.exceptions.EntityNotFoundException:
            print(f"   ⏭️  {job_name}: job not found — skip")
        except Exception as e:
            print(f"   ⚠️  {job_name}: {e}")

    # Set max concurrent runs = 1 for bookmarked jobs
    for job_name in bookmark_jobs:
        try:
            response = glue.get_job(JobName=job_name)
            glue.update_job(
                JobName=job_name,
                JobUpdate={
                    "MaxRetries": 1,
                    "ExecutionProperty": {
                        "MaxConcurrentRuns": 1  # CRITICAL for bookmarks
                    }
                }
            )
            print(f"   ✅ {job_name}: MaxConcurrentRuns=1 (bookmark safety)")
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Create Bookmark Management Utility
# ═══════════════════════════════════════════════════════════════════
from datetime import datetime


def create_bookmark_utility():
    """Print commands for bookmark management"""
    print("\n📋 BOOKMARK MANAGEMENT COMMANDS:")
    print("=" * 60)
    print(f"""
# ── VIEW BOOKMARK STATE ──
aws glue get-job-bookmark \\
    --job-name "quickcart-csv-to-parquet" \\
    --region {AWS_REGION}

# ── RESET BOOKMARK (reprocess everything) ──
aws glue reset-job-bookmark \\
    --job-name "quickcart-csv-to-parquet" \\
    --region {AWS_REGION}

# ── VIEW DYNAMODB WATERMARKS ──
aws dynamodb scan \\
    --table-name {DDB_TABLE_NAME} \\
    --region {AWS_REGION}

# ── RESET SPECIFIC WATERMARK ──
aws dynamodb delete-item \\
    --table-name {DDB_TABLE_NAME} \\
    --key '{{"job_name": {{"S": "quickcart-mariadb-incr"}}, "source_table": {{"S": "orders"}}}}' \\
    --region {AWS_REGION}

# ── VIEW S3 WATERMARKS ──
aws s3 ls s3://{S3_BUCKET}/state/watermarks/ --recursive

# ── READ SPECIFIC S3 WATERMARK ──
aws s3 cp s3://{S3_BUCKET}/state/watermarks/my-job/orders.json -
""")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 SETTING UP INCREMENTAL PROCESSING INFRASTRUCTURE")
    print("=" * 60)

    create_watermark_table()
    update_glue_role_for_dynamodb()
    create_s3_watermark_directory()
    enable_bookmarks_on_existing_jobs()
    create_bookmark_utility()

    print("\n" + "=" * 60)
    print("✅ INCREMENTAL PROCESSING INFRASTRUCTURE READY")
    print("=" * 60)