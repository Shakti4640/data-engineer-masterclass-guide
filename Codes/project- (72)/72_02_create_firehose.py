# file: 72_02_create_firehose.py
# Account A: Firehose reads from Kinesis, converts to Parquet, writes to S3

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
STREAM_NAME = "clickstream-events"
FIREHOSE_NAME = "clickstream-delivery"
BUCKET_NAME = "orders-data-prod"  # Created in Project 71
GLUE_DB = "orders_bronze"
GLUE_TABLE = "clickstream_raw"


def create_glue_schema_table(glue_client):
    """
    Firehose needs a Glue Catalog table to define Parquet schema
    This table defines WHAT the Parquet columns look like
    """
    try:
        glue_client.create_database(
            DatabaseInput={
                "Name": GLUE_DB,
                "Description": "Bronze layer for Orders domain — raw ingestion"
            }
        )
        print(f"✅ Glue database: {GLUE_DB}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue database exists: {GLUE_DB}")

    try:
        glue_client.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                "Name": GLUE_TABLE,
                "Description": "Raw clickstream events — Firehose Parquet schema reference",
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "event_id", "Type": "string"},
                        {"Name": "event_type", "Type": "string"},
                        {"Name": "user_id", "Type": "string"},
                        {"Name": "session_id", "Type": "string"},
                        {"Name": "event_timestamp", "Type": "string"},
                        {"Name": "page_url", "Type": "string"},
                        {"Name": "product_id", "Type": "string"},
                        {"Name": "search_query", "Type": "string"},
                        {"Name": "device_type", "Type": "string"},
                        {"Name": "browser", "Type": "string"},
                        {"Name": "ip_country", "Type": "string"},
                        {"Name": "referrer", "Type": "string"},
                        {"Name": "cart_value", "Type": "double"},
                        {"Name": "items_in_cart", "Type": "int"}
                    ],
                    "Location": f"s3://{BUCKET_NAME}/bronze/clickstream/",
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                    }
                },
                "PartitionKeys": [
                    {"Name": "year", "Type": "string"},
                    {"Name": "month", "Type": "string"},
                    {"Name": "day", "Type": "string"},
                    {"Name": "hour", "Type": "string"}
                ],
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "classification": "parquet",
                    "pipeline": "clickstream-realtime"
                }
            }
        )
        print(f"✅ Glue table: {GLUE_DB}.{GLUE_TABLE}")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️  Glue table exists: {GLUE_DB}.{GLUE_TABLE}")


def create_firehose_role(iam_client):
    """
    IAM role for Firehose to:
    → Read from Kinesis Data Stream
    → Write Parquet to S3
    → Access Glue Catalog for schema
    → Write CloudWatch logs
    """
    role_name = "FirehoseClickstreamDeliveryRole"

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "firehose.amazonaws.com"},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {"sts:ExternalId": ACCOUNT_ID}
            }
        }]
    }

    permission_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ReadKinesisStream",
                "Effect": "Allow",
                "Action": [
                    "kinesis:DescribeStream",
                    "kinesis:GetShardIterator",
                    "kinesis:GetRecords",
                    "kinesis:ListShards"
                ],
                "Resource": f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{STREAM_NAME}"
            },
            {
                "Sid": "WriteS3",
                "Effect": "Allow",
                "Action": [
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET_NAME}",
                    f"arn:aws:s3:::{BUCKET_NAME}/*"
                ]
            },
            {
                "Sid": "GlueCatalogAccess",
                "Effect": "Allow",
                "Action": [
                    "glue:GetTable",
                    "glue:GetTableVersion",
                    "glue:GetTableVersions"
                ],
                "Resource": [
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{GLUE_DB}",
                    f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{GLUE_DB}/{GLUE_TABLE}"
                ]
            },
            {
                "Sid": "CloudWatchLogs",
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream"
                ],
                "Resource": "arn:aws:logs:*:*:*"
            }
        ]
    }

    try:
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Firehose role for clickstream delivery to S3"
        )
        print(f"✅ IAM role created: {role_name}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"ℹ️  IAM role exists: {role_name}")

    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName="firehose-clickstream-policy",
        PolicyDocument=json.dumps(permission_policy)
    )

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"
    print(f"   ARN: {role_arn}")

    # IAM role propagation delay
    print("   Waiting 10s for IAM propagation...", end="", flush=True)
    time.sleep(10)
    print(" ✅")

    return role_arn


def create_firehose_delivery_stream(firehose_client, role_arn):
    """
    Create Firehose delivery stream with:
    → Source: Kinesis Data Stream
    → Destination: S3 with Parquet conversion
    → Dynamic partitioning by year/month/day/hour
    → 60-second buffer interval
    """
    stream_arn = f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{STREAM_NAME}"

    try:
        firehose_client.create_delivery_stream(
            DeliveryStreamName=FIREHOSE_NAME,
            DeliveryStreamType="KinesisStreamAsSource",
            KinesisStreamSourceConfiguration={
                "KinesisStreamARN": stream_arn,
                "RoleARN": role_arn
            },
            ExtendedS3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{BUCKET_NAME}",

                # --- S3 PREFIX WITH DYNAMIC PARTITIONING ---
                "Prefix": (
                    "bronze/clickstream/"
                    "year=!{partitionKeyFromQuery:year}/"
                    "month=!{partitionKeyFromQuery:month}/"
                    "day=!{partitionKeyFromQuery:day}/"
                    "hour=!{partitionKeyFromQuery:hour}/"
                ),
                "ErrorOutputPrefix": (
                    "bronze/clickstream_errors/"
                    "year=!{timestamp:yyyy}/"
                    "month=!{timestamp:MM}/"
                    "day=!{timestamp:dd}/"
                    "hour=!{timestamp:HH}/"
                    "!{firehose:error-output-type}/"
                ),

                # --- BUFFERING ---
                "BufferingHints": {
                    "SizeInMBs": 64,         # Flush at 64 MB
                    "IntervalInSeconds": 60   # OR every 60 seconds
                },

                # --- COMPRESSION ---
                "CompressionFormat": "UNCOMPRESSED",
                # Compression handled by Parquet + Snappy below

                # --- FORMAT CONVERSION (JSON → Parquet) ---
                "DataFormatConversionConfiguration": {
                    "Enabled": True,
                    "SchemaConfiguration": {
                        "RoleARN": role_arn,
                        "DatabaseName": GLUE_DB,
                        "TableName": GLUE_TABLE,
                        "Region": REGION,
                        "VersionId": "LATEST"
                    },
                    "InputFormatConfiguration": {
                        "Deserializer": {
                            "OpenXJsonSerDe": {
                                "CaseInsensitive": True,
                                "ConvertDotsInJsonKeysToUnderscores": True
                            }
                        }
                    },
                    "OutputFormatConfiguration": {
                        "Serializer": {
                            "ParquetSerDe": {
                                "Compression": "SNAPPY",
                                "EnableDictionaryCompression": True,
                                "MaxPaddingBytes": 0,
                                "WriterVersion": "V2"
                            }
                        }
                    }
                },

                # --- DYNAMIC PARTITIONING ---
                "DynamicPartitioningConfiguration": {
                    "Enabled": True,
                    "RetryOptions": {
                        "DurationInSeconds": 300
                    }
                },

                "ProcessingConfiguration": {
                    "Enabled": True,
                    "Processors": [
                        {
                            "Type": "MetadataExtraction",
                            "Parameters": [
                                {
                                    "ParameterName": "MetadataExtractionQuery",
                                    "ParameterValue": (
                                        '{year: .event_timestamp[0:4], '
                                        'month: .event_timestamp[5:7], '
                                        'day: .event_timestamp[8:10], '
                                        'hour: .event_timestamp[11:13]}'
                                    )
                                },
                                {
                                    "ParameterName": "JsonParsingEngine",
                                    "ParameterValue": "JQ-1.6"
                                }
                            ]
                        }
                    ]
                },

                # --- CLOUDWATCH LOGGING ---
                "CloudWatchLoggingOptions": {
                    "Enabled": True,
                    "LogGroupName": f"/aws/firehose/{FIREHOSE_NAME}",
                    "LogStreamName": "S3Delivery"
                },

                # --- S3 BACKUP (optional: raw JSON backup) ---
                "S3BackupMode": "Disabled"
                # Enable for debugging: keeps original JSON alongside Parquet
            },
            Tags=[
                {"Key": "Domain", "Value": "orders"},
                {"Key": "Pipeline", "Value": "clickstream-realtime"},
                {"Key": "DataProduct", "Value": "clickstream_events"}
            ]
        )
        print(f"✅ Firehose delivery stream creating: {FIREHOSE_NAME}")

    except firehose_client.exceptions.ResourceInUseException:
        print(f"ℹ️  Firehose already exists: {FIREHOSE_NAME}")

    # Wait for Firehose to become ACTIVE
    print("   Waiting for ACTIVE status...", end="", flush=True)
    while True:
        desc = firehose_client.describe_delivery_stream(
            DeliveryStreamName=FIREHOSE_NAME
        )
        status = desc["DeliveryStreamDescription"]["DeliveryStreamStatus"]
        if status == "ACTIVE":
            print(f" ✅ ({status})")
            break
        elif status in ("CREATING_FAILED", "DELETING"):
            print(f" ❌ ({status})")
            raise Exception(f"Firehose creation failed: {status}")
        time.sleep(5)

    firehose_arn = desc["DeliveryStreamDescription"]["DeliveryStreamARN"]
    print(f"   ARN: {firehose_arn}")
    print(f"   Source: Kinesis stream '{STREAM_NAME}'")
    print(f"   Dest: s3://{BUCKET_NAME}/bronze/clickstream/")
    print(f"   Format: Parquet + Snappy")
    print(f"   Buffer: 60s or 64 MB")
    print(f"   Partitioning: year/month/day/hour")

    return firehose_arn


def main():
    print("=" * 70)
    print("🔥 CREATING FIREHOSE DELIVERY STREAM: Clickstream → S3 Parquet")
    print("=" * 70)

    iam_client = boto3.client("iam", region_name=REGION)
    glue_client = boto3.client("glue", region_name=REGION)
    firehose_client = boto3.client("firehose", region_name=REGION)

    # Step 1: Glue schema table (Firehose needs this for Parquet conversion)
    print("\n--- Step 1: Glue Schema Table ---")
    create_glue_schema_table(glue_client)

    # Step 2: IAM role for Firehose
    print("\n--- Step 2: IAM Role ---")
    role_arn = create_firehose_role(iam_client)

    # Step 3: Create Firehose delivery stream
    print("\n--- Step 3: Firehose Delivery Stream ---")
    firehose_arn = create_firehose_delivery_stream(firehose_client, role_arn)

    print("\n" + "=" * 70)
    print("✅ FIREHOSE SETUP COMPLETE")
    print(f"   Stream → Firehose → S3 Parquet pipeline is LIVE")
    print("=" * 70)


if __name__ == "__main__":
    main()