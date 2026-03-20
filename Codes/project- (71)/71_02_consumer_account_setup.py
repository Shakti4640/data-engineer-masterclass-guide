# file: 71_02_consumer_account_setup.py
# Account C (333333333333): Analytics Domain — Consumer Account Setup
# Creates: IAM role for Glue/Athena, SQS queue for events, External Catalog reference

import boto3
import json

# --- CONFIGURATION ---
CONSUMER_ACCOUNT_ID = "333333333333"
PRODUCER_ORDERS_ACCOUNT_ID = "111111111111"
PRODUCER_CUSTOMER_ACCOUNT_ID = "222222222222"
GOVERNANCE_ACCOUNT_ID = "444444444444"
REGION = "us-east-2"
DOMAIN_NAME = "analytics"
EXTERNAL_ID = "mesh-analytics-2025"

# All producer domains this consumer subscribes to
SUBSCRIBED_DOMAINS = [
    {
        "domain": "orders",
        "account_id": PRODUCER_ORDERS_ACCOUNT_ID,
        "role_name": "MeshProducer-orders-CrossAcctReader",
        "glue_database": "orders_gold",
        "s3_bucket": "orders-data-prod"
    },
    {
        "domain": "customers",
        "account_id": PRODUCER_CUSTOMER_ACCOUNT_ID,
        "role_name": "MeshProducer-customers-CrossAcctReader",
        "glue_database": "customers_gold",
        "s3_bucket": "customers-data-prod"
    }
]


def create_consumer_role(iam_client):
    """
    Create the consumer's Glue/Athena execution role
    This role can:
    1. AssumeRole into each producer's cross-account reader role
    2. Read/write to own S3 bucket
    3. Access own Glue Catalog
    """
    role_name = "MeshDataConsumerRole"

    # Trust policy: Glue and Athena can assume this role
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": [
                        "glue.amazonaws.com",
                        "athena.amazonaws.com"
                    ]
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    # Permission: assume cross-account roles + local operations
    assume_role_resources = [
        f"arn:aws:iam::{sub['account_id']}:role/{sub['role_name']}"
        for sub in SUBSCRIBED_DOMAINS
    ]

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AssumeCrossAccountProducerRoles",
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": assume_role_resources
            },
            {
                "Sid": "ReadProducerS3Directly",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    item
                    for sub in SUBSCRIBED_DOMAINS
                    for item in [
                        f"arn:aws:s3:::{sub['s3_bucket']}",
                        f"arn:aws:s3:::{sub['s3_bucket']}/gold/*"
                    ]
                ]
            },
            {
                "Sid": "WriteOwnBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{DOMAIN_NAME}-data-prod",
                    f"arn:aws:s3:::{DOMAIN_NAME}-data-prod/*"
                ]
            },
            {
                "Sid": "AccessOwnGlueCatalog",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase*",
                    "glue:GetTable*",
                    "glue:GetPartition*",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{CONSUMER_ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{CONSUMER_ACCOUNT_ID}:database/*",
                    f"arn:aws:glue:{REGION}:{CONSUMER_ACCOUNT_ID}:table/*/*"
                ]
            },
            {
                "Sid": "ReadProducerGlueCatalogs",
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions"
                ],
                "Resource": [
                    item
                    for sub in SUBSCRIBED_DOMAINS
                    for item in [
                        f"arn:aws:glue:{REGION}:{sub['account_id']}:catalog",
                        f"arn:aws:glue:{REGION}:{sub['account_id']}:database/{sub['glue_database']}",
                        f"arn:aws:glue:{REGION}:{sub['account_id']}:table/{sub['glue_database']}/*"
                    ]
                ]
            },
            {
                "Sid": "CloudWatchLogs",
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
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Analytics domain consumer role for Data Mesh",
            Tags=[
                {"Key": "Domain", "Value": DOMAIN_NAME},
                {"Key": "MeshRole", "Value": "consumer"}
            ]
        )
        print(f"✅ Consumer role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  Consumer role exists, updating policies")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="mesh-consumer-access",
        PolicyDocument=json.dumps(permission_policy)
    )
    print(f"✅ Permission policy attached with {len(SUBSCRIBED_DOMAINS)} domain subscriptions")

    return f"arn:aws:iam::{CONSUMER_ACCOUNT_ID}:role/{role_name}"


def create_event_subscription_queue(sqs_client):
    """
    Create SQS queue to receive data product publish events
    from Governance Account's SNS topic (Project 59 pattern)
    """
    queue_name = f"mesh-{DOMAIN_NAME}-data-events"

    # Create the queue
    response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "VisibilityTimeout": "300",        # 5 min to process
            "MessageRetentionPeriod": "86400",  # 1 day retention
            "ReceiveMessageWaitTimeSeconds": "20"  # Long polling (Project 27)
        },
        tags={
            "Domain": DOMAIN_NAME,
            "MeshRole": "event-consumer",
            "Source": "governance-sns"
        }
    )
    queue_url = response["QueueUrl"]
    print(f"✅ SQS queue created: {queue_name}")

    # Get queue ARN
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )
    queue_arn = attrs["Attributes"]["QueueArn"]

    # Set queue policy: allow Governance SNS to send messages
    sns_topic_arn = f"arn:aws:sns:{REGION}:{GOVERNANCE_ACCOUNT_ID}:data-product-events"

    queue_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowGovernanceSNS",
                "Effect": "Allow",
                "Principal": {
                    "Service": "sns.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": sns_topic_arn
                    }
                }
            }
        ]
    }

    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(queue_policy)
        }
    )
    print(f"✅ Queue policy set: allows SNS from Governance account")

    # Create DLQ for failed processing (Project 50 pattern)
    dlq_response = sqs_client.create_queue(
        QueueName=f"{queue_name}-dlq",
        Attributes={
            "MessageRetentionPeriod": "1209600"  # 14 days for DLQ
        }
    )
    dlq_url = dlq_response["QueueUrl"]
    dlq_attrs = sqs_client.get_queue_attributes(
        QueueUrl=dlq_url,
        AttributeNames=["QueueArn"]
    )
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    # Attach DLQ to main queue
    sqs_client.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": 3
            })
        }
    )
    print(f"✅ DLQ attached: {queue_name}-dlq (maxReceiveCount=3)")

    return queue_url, queue_arn


def create_external_catalog_references(glue_client):
    """
    Create LOCAL Glue databases that REFERENCE producer catalogs
    
    WHY:
    → Athena/Glue in consumer account need a LOCAL database name
    → This database points to the EXTERNAL catalog (producer account)
    → When queried, Glue resolves to the remote catalog via CatalogId
    
    HOW IT WORKS INTERNALLY:
    → Athena SQL: SELECT * FROM orders_domain.daily_orders
    → "orders_domain" is a local DB with TargetDatabase pointing to Account A
    → Athena sees TargetDatabase → calls Glue GetTable on Account A's catalog
    → Account A's Glue Resource Policy allows this → returns schema
    → Athena reads S3 using cross-account permissions
    """
    for sub in SUBSCRIBED_DOMAINS:
        local_db_name = f"{sub['domain']}_domain"

        try:
            glue_client.create_database(
                DatabaseInput={
                    "Name": local_db_name,
                    "Description": (
                        f"Reference to {sub['domain']} domain Gold catalog "
                        f"in account {sub['account_id']}"
                    ),
                    "TargetDatabase": {
                        "CatalogId": sub["account_id"],
                        "DatabaseName": sub["glue_database"]
                    },
                    "Parameters": {
                        "mesh_source_domain": sub["domain"],
                        "mesh_source_account": sub["account_id"],
                        "mesh_role": "external_reference",
                        "created_by": "mesh-consumer-setup"
                    }
                }
            )
            print(f"✅ External catalog reference: {local_db_name} → "
                  f"{sub['account_id']}:{sub['glue_database']}")
        except glue_client.exceptions.AlreadyExistsException:
            print(f"ℹ️  External reference exists: {local_db_name}")


def create_consumer_output_bucket(s3_client):
    """
    Consumer's own S3 bucket for:
    → Athena query results
    → Derived datasets (joins across domains)
    → ML feature store outputs
    """
    bucket_name = f"{DOMAIN_NAME}-data-prod"

    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        print(f"✅ Consumer output bucket created: {bucket_name}")
    except s3_client.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Consumer bucket exists: {bucket_name}")

    # Standard security (Project 1 pattern — not repeated in detail)
    s3_client.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True
        }
    )

    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True
            }]
        }
    )

    # Create standard prefix structure
    for prefix in [
        "athena-results/",
        "derived/",
        "ml-features/",
        "temp/"
    ]:
        s3_client.put_object(Bucket=bucket_name, Key=prefix, Body=b"")

    print(f"✅ Consumer bucket configured with standard prefixes")
    return bucket_name


def create_athena_workgroup(athena_client, output_bucket):
    """
    Athena workgroup for this consumer domain
    → Isolates query history, cost tracking, result location
    → Enforces scan limits to prevent runaway costs
    """
    workgroup_name = f"mesh-{DOMAIN_NAME}"

    try:
        athena_client.create_work_group(
            Name=workgroup_name,
            Configuration={
                "ResultConfiguration": {
                    "OutputLocation": f"s3://{output_bucket}/athena-results/",
                    "EncryptionConfiguration": {
                        "EncryptionOption": "SSE_S3"
                    }
                },
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": 10 * 1024 * 1024 * 1024,  # 10 GB limit
                "EngineVersion": {
                    "SelectedEngineVersion": "Athena engine version 3"
                }
            },
            Description=f"Workgroup for {DOMAIN_NAME} domain — Data Mesh consumer",
            Tags=[
                {"Key": "Domain", "Value": DOMAIN_NAME},
                {"Key": "MeshRole", "Value": "consumer"}
            ]
        )
        print(f"✅ Athena workgroup created: {workgroup_name}")
        print(f"   → Scan limit: 10 GB per query")
        print(f"   → Results: s3://{output_bucket}/athena-results/")
    except athena_client.exceptions.InvalidRequestException as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️  Athena workgroup exists: {workgroup_name}")
        else:
            raise

    return workgroup_name


def main():
    print("=" * 70)
    print(f"🏗️  SETTING UP CONSUMER ACCOUNT: {DOMAIN_NAME} Domain")
    print(f"   Account: {CONSUMER_ACCOUNT_ID}")
    print(f"   Subscribing to: {[s['domain'] for s in SUBSCRIBED_DOMAINS]}")
    print("=" * 70)

    iam_client = boto3.client("iam", region_name=REGION)
    sqs_client = boto3.client("sqs", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)
    s3_client = boto3.client("s3", region_name=REGION)
    athena_client = boto3.client("athena", region_name=REGION)

    # Step 1: Consumer IAM Role
    print("\n--- Step 1: Consumer IAM Role ---")
    role_arn = create_consumer_role(iam_client)

    # Step 2: SQS Event Subscription
    print("\n--- Step 2: Event Subscription Queue ---")
    queue_url, queue_arn = create_event_subscription_queue(sqs_client)

    # Step 3: External Catalog References
    print("\n--- Step 3: External Catalog References ---")
    create_external_catalog_references(glue_client)

    # Step 4: Consumer Output Bucket
    print("\n--- Step 4: Consumer Output Bucket ---")
    output_bucket = create_consumer_output_bucket(s3_client)

    # Step 5: Athena Workgroup
    print("\n--- Step 5: Athena Workgroup ---")
    workgroup = create_athena_workgroup(athena_client, output_bucket)

    print("\n" + "=" * 70)
    print("✅ CONSUMER ACCOUNT SETUP COMPLETE")
    print(f"   Role ARN:        {role_arn}")
    print(f"   Event Queue:     {queue_url}")
    print(f"   Athena Workgroup: {workgroup}")
    print(f"   Output Bucket:   s3://{output_bucket}/")
    print("=" * 70)


if __name__ == "__main__":
    main()