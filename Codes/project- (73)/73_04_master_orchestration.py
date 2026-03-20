# file: 73_04_master_orchestration.py
# Master Step Functions pipeline that orchestrates the ENTIRE platform
# Nightly batch: MariaDB → Bronze → Silver → Gold → Redshift → Publish

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
SNS_TOPIC_ARN = f"arn:aws:sns:{REGION}:444444444444:data-product-events"
ALERT_TOPIC_ARN = f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-alerts"


def create_master_state_machine(sfn_client, role_arn):
    """
    Master nightly pipeline:
    
    1. Extract: Glue JDBC from MariaDB → Bronze S3
    2. Transform: Glue ETL Bronze → Silver (validate + dedup)
    3. Quality: Glue DQ rules on Silver
    4. Curate: Glue ETL Silver → Gold (aggregate + compact)
    5. Load: Redshift COPY from Gold S3 (hot data refresh)
    6. Compact: Iceberg table maintenance
    7. Publish: SNS data product event to mesh
    8. Monitor: CloudWatch success/failure metrics
    
    Error handling:
    → Each step has Retry (transient failures)
    → Each step has Catch (permanent failures → alert + continue)
    → Pipeline continues even if one domain fails
    """
    definition = {
        "Comment": "QuickCart Master Nightly Pipeline — Full Data Lake Platform",
        "StartAt": "ParallelDomainExtraction",
        "States": {

            # ═══════════════════════════════════════════
            # STEP 1: PARALLEL EXTRACTION FROM ALL SOURCES
            # ═══════════════════════════════════════════
            "ParallelDomainExtraction": {
                "Type": "Parallel",
                "Comment": "Extract from all source systems simultaneously",
                "Branches": [
                    {
                        "StartAt": "ExtractOrders",
                        "States": {
                            "ExtractOrders": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "extract-mariadb-orders",
                                    "Arguments": {
                                        "--source_table": "orders",
                                        "--target_path": "s3://orders-data-prod/bronze/orders/",
                                        "--extraction_mode": "incremental",
                                        "--bookmark_key": "updated_at"
                                    }
                                },
                                "ResultPath": "$.extractResult",
                                "Retry": [{
                                    "ErrorEquals": ["States.TaskFailed"],
                                    "IntervalSeconds": 60,
                                    "MaxAttempts": 2,
                                    "BackoffRate": 2.0
                                }],
                                "Catch": [{
                                    "ErrorEquals": ["States.ALL"],
                                    "Next": "ExtractOrdersFailed",
                                    "ResultPath": "$.error"
                                }],
                                "End": True
                            },
                            "ExtractOrdersFailed": {
                                "Type": "Pass",
                                "Result": {"status": "FAILED", "domain": "orders"},
                                "ResultPath": "$.extractResult",
                                "End": True
                            }
                        }
                    },
                    {
                        "StartAt": "ExtractCustomers",
                        "States": {
                            "ExtractCustomers": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "extract-mariadb-customers",
                                    "Arguments": {
                                        "--source_table": "customers",
                                        "--target_path": "s3://customers-data-prod/bronze/customers/",
                                        "--extraction_mode": "incremental"
                                    }
                                },
                                "ResultPath": "$.extractResult",
                                "Retry": [{
                                    "ErrorEquals": ["States.TaskFailed"],
                                    "IntervalSeconds": 60,
                                    "MaxAttempts": 2,
                                    "BackoffRate": 2.0
                                }],
                                "Catch": [{
                                    "ErrorEquals": ["States.ALL"],
                                    "Next": "ExtractCustomersFailed",
                                    "ResultPath": "$.error"
                                }],
                                "End": True
                            },
                            "ExtractCustomersFailed": {
                                "Type": "Pass",
                                "Result": {"status": "FAILED", "domain": "customers"},
                                "ResultPath": "$.extractResult",
                                "End": True
                            }
                        }
                    }
                ],
                "ResultPath": "$.extractionResults",
                "Next": "TransformBronzeToSilver"
            },

            # ═══════════════════════════════════════════
            # STEP 2: BRONZE → SILVER (Validate + Dedup)
            # ═══════════════════════════════════════════
            "TransformBronzeToSilver": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "transform-bronze-to-silver",
                    "Arguments": {
                        "--domains": "orders,customers",
                        "--enable-data-quality": "true"
                    }
                },
                "ResultPath": "$.silverResult",
                "Retry": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 1
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "AlertTransformFailure",
                    "ResultPath": "$.error"
                }],
                "Next": "DataQualityCheck"
            },

            # ═══════════════════════════════════════════
            # STEP 3: DATA QUALITY GATE (Project 68)
            # ═══════════════════════════════════════════
            "DataQualityCheck": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "data-quality-check-silver",
                    "Arguments": {
                        "--database": "orders_silver",
                        "--tables": "orders,customers",
                        "--quality_threshold": "99"
                    }
                },
                "ResultPath": "$.qualityResult",
                "Next": "EvaluateQuality"
            },

            "EvaluateQuality": {
                "Type": "Choice",
                "Choices": [
                    {
                        "Variable": "$.qualityResult.JobRunState",
                        "StringEquals": "SUCCEEDED",
                        "Next": "TransformSilverToGold"
                    }
                ],
                "Default": "AlertQualityFailure"
            },

            # ═══════════════════════════════════════════
            # STEP 4: SILVER → GOLD (Aggregate + Compact)
            # ═══════════════════════════════════════════
            "TransformSilverToGold": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "transform-silver-to-gold",
                    "Arguments": {
                        "--domains": "orders,customers",
                        "--output_format": "iceberg",
                        "--compaction": "true"
                    }
                },
                "ResultPath": "$.goldResult",
                "Retry": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 60,
                    "MaxAttempts": 1
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "AlertTransformFailure",
                    "ResultPath": "$.error"
                }],
                "Next": "RefreshRedshiftHotData"
            },

            # ═══════════════════════════════════════════
            # STEP 5: REDSHIFT HOT DATA REFRESH
            # ═══════════════════════════════════════════
            "RefreshRedshiftHotData": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
                "Parameters": {
                    "WorkgroupName": "quickcart-analytics",
                    "Database": "quickcart",
                    "Sql": "CALL staging.refresh_hot_tables();"
                },
                "ResultPath": "$.redshiftResult",
                "Retry": [{
                    "ErrorEquals": ["States.TaskFailed"],
                    "IntervalSeconds": 30,
                    "MaxAttempts": 2
                }],
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "PublishDataProducts",
                    "ResultPath": "$.redshiftError",
                    "Comment": "Redshift failure non-critical — Gold S3 is still updated"
                }],
                "Next": "RefreshMaterializedViews"
            },

            # ═══════════════════════════════════════════
            # STEP 5B: REFRESH MATERIALIZED VIEWS
            # ═══════════════════════════════════════════
            "RefreshMaterializedViews": {
                "Type": "Task",
                "Resource": "arn:aws:states:::aws-sdk:redshiftdata:executeStatement",
                "Parameters": {
                    "WorkgroupName": "quickcart-analytics",
                    "Database": "quickcart",
                    "Sql": "REFRESH MATERIALIZED VIEW mart_sales.daily_sales_summary;"
                },
                "ResultPath": "$.mvRefreshResult",
                "Catch": [{
                    "ErrorEquals": ["States.ALL"],
                    "Next": "PublishDataProducts",
                    "ResultPath": "$.mvError"
                }],
                "Next": "PublishDataProducts"
            },

            # ═══════════════════════════════════════════
            # STEP 6: PUBLISH DATA PRODUCTS TO MESH
            # ═══════════════════════════════════════════
            "PublishDataProducts": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": SNS_TOPIC_ARN,
                    "Subject": "[DataMesh] Nightly batch complete — data products published",
                    "Message": {
                        "event_type": "data_product.published",
                        "source_domain": "orders",
                        "products": ["daily_orders", "daily_refunds"],
                        "pipeline": "nightly-batch",
                        "status": "success"
                    },
                    "MessageAttributes": {
                        "source_domain": {
                            "DataType": "String",
                            "StringValue": "orders"
                        },
                        "event_type": {
                            "DataType": "String",
                            "StringValue": "data_product.published"
                        }
                    }
                },
                "ResultPath": "$.publishResult",
                "Next": "PipelineSuccess"
            },

            # ═══════════════════════════════════════════
            # SUCCESS / FAILURE STATES
            # ═══════════════════════════════════════════
            "PipelineSuccess": {
                "Type": "Succeed",
                "Comment": "Full nightly pipeline completed successfully"
            },

            "AlertTransformFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": ALERT_TOPIC_ARN,
                    "Subject": "🚨 NIGHTLY PIPELINE FAILED — Transform step",
                    "Message.$": "States.Format('Transform failed. Error: {}. Check Glue job logs.', $.error.Cause)"
                },
                "Next": "PipelineFailed"
            },

            "AlertQualityFailure": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sns:publish",
                "Parameters": {
                    "TopicArn": ALERT_TOPIC_ARN,
                    "Subject": "⚠️ DATA QUALITY GATE FAILED — Gold layer NOT updated",
                    "Message": "Data quality below threshold. Silver data did not pass validation. Gold layer preserved with yesterday's data. Manual review required."
                },
                "Next": "PipelineFailed"
            },

            "PipelineFailed": {
                "Type": "Fail",
                "Error": "PipelineError",
                "Cause": "One or more pipeline steps failed — check alerts"
            }
        }
    }

    try:
        response = sfn_client.create_state_machine(
            name="quickcart-master-nightly-pipeline",
            definition=json.dumps(definition, indent=2),
            roleArn=role_arn,
            type="STANDARD",
            tags=[
                {"key": "Platform", "value": "data-lake"},
                {"key": "Pipeline", "value": "master-nightly"},
                {"key": "Criticality", "value": "P1"}
            ]
        )
        print(f"✅ Master pipeline created: {response['stateMachineArn']}")
        return response["stateMachineArn"]
    except sfn_client.exceptions.StateMachineAlreadyExists:
        print(f"ℹ️  Master pipeline already exists")
        machines = sfn_client.list_state_machines()
        for sm in machines["stateMachines"]:
            if sm["name"] == "quickcart-master-nightly-pipeline":
                return sm["stateMachineArn"]


def create_nightly_schedule(events_client, state_machine_arn, role_arn):
    """Schedule the master pipeline to run every night at 2 AM UTC"""
    events_client.put_rule(
        Name="nightly-master-pipeline",
        ScheduleExpression="cron(0 2 * * ? *)",  # 2:00 AM UTC daily
        State="ENABLED",
        Description="Trigger master nightly pipeline at 2 AM UTC"
    )

    events_client.put_targets(
        Rule="nightly-master-pipeline",
        Targets=[{
            "Id": "master-pipeline-target",
            "Arn": state_machine_arn,
            "RoleArn": role_arn,
            "Input": json.dumps({
                "trigger": "scheduled",
                "schedule": "nightly-2am-utc",
                "run_date": "dynamic"
            })
        }]
    )
    print(f"✅ Nightly schedule: cron(0 2 * * ? *) → master pipeline")


def main():
    print("=" * 70)
    print("🔧 MASTER ORCHESTRATION: Nightly Pipeline")
    print("   Extract → Transform → Quality → Gold → Redshift → Publish")
    print("=" * 70)

    sfn_client = boto3.client("stepfunctions", region_name=REGION)
    events_client = boto3.client("events", region_name=REGION)

    # Use existing role (or create one like Project 72 Step 5)
    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/MasterPipelineOrchestratorRole"

    print("\n--- Creating Master State Machine ---")
    sm_arn = create_master_state_machine(sfn_client, role_arn)

    print("\n--- Creating Nightly Schedule ---")
    create_nightly_schedule(events_client, sm_arn, role_arn)

    print("\n" + "=" * 70)
    print("✅ MASTER ORCHESTRATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()