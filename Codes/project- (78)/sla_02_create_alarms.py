# file: sla/02_create_alarms.py
# Purpose: Create CloudWatch alarms for every monitored pipeline component

import boto3
import json
from pipeline_registry import PIPELINE_REGISTRY, get_all_glue_jobs

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def create_glue_job_alarm(cloudwatch, job_name, pipeline_name, tier):
    """
    Create alarm that triggers when a Glue job fails
    
    METRIC: Glue publishes to CloudWatch automatically:
    → glue.driver.aggregate.numFailedTasks (job-level failure)
    
    We use Glue Job Metrics (enabled in job config):
    → Namespace: Glue
    → MetricName: glue.driver.aggregate.numFailedTasks
    → Dimensions: JobName, JobRunId, Type
    
    ALTERNATIVE: Use EventBridge Glue state change events (more reliable)
    → We use BOTH: alarm for dashboard, EventBridge for triggering recovery
    """
    alarm_name = f"sla-{pipeline_name}-{job_name}-failure"

    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=(
            f"Glue job '{job_name}' failed | "
            f"Pipeline: {pipeline_name} | Tier: {tier}"
        ),
        Namespace="Glue",
        MetricName="glue.driver.aggregate.numFailedTasks",
        Dimensions=[
            {"Name": "JobName", "Value": job_name},
            {"Name": "Type", "Value": "gauge"}
        ],
        Statistic="Maximum",
        Period=300,                         # 5 minutes
        EvaluationPeriods=1,                # 1 period
        Threshold=0,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",    # No data = no failure = OK
        ActionsEnabled=True,
        AlarmActions=[
            f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-info-alerts"
        ],
        OKActions=[
            f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-info-alerts"
        ],
        Tags=[
            {"Key": "Pipeline", "Value": pipeline_name},
            {"Key": "Tier", "Value": str(tier)},
            {"Key": "Component", "Value": "GlueJob"},
            {"Key": "ManagedBy", "Value": "SLA-System"}
        ]
    )
    print(f"  ✅ Alarm created: {alarm_name}")
    return alarm_name


def create_data_freshness_alarm(cloudwatch, pipeline_name, config):
    """
    Alarm on data staleness exceeding threshold
    
    Custom metric published by Lambda (Module 3):
    → Namespace: QuickCart/DataPlatform
    → MetricName: DataStalenessMinutes
    → Dimensions: Pipeline, Layer
    """
    alarm_name = f"sla-{pipeline_name}-data-stale"
    max_staleness = config["max_staleness_minutes"]

    cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        AlarmDescription=(
            f"Data in pipeline '{pipeline_name}' is stale > "
            f"{max_staleness} minutes"
        ),
        Namespace="QuickCart/DataPlatform",
        MetricName="DataStalenessMinutes",
        Dimensions=[
            {"Name": "Pipeline", "Value": pipeline_name},
            {"Name": "Layer", "Value": "gold"}
        ],
        Statistic="Maximum",
        Period=900,                         # 15 minutes
        EvaluationPeriods=1,
        Threshold=max_staleness,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="breaching",       # No data = check isn't running = bad
        ActionsEnabled=True,
        AlarmActions=[
            f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-warning-alerts"
        ],
        Tags=[
            {"Key": "Pipeline", "Value": pipeline_name},
            {"Key": "Tier", "Value": str(config["tier"])},
            {"Key": "Component", "Value": "DataFreshness"},
            {"Key": "ManagedBy", "Value": "SLA-System"}
        ]
    )
    print(f"  ✅ Alarm created: {alarm_name}")
    return alarm_name


def create_composite_alarm(cloudwatch, pipeline_name, child_alarm_names, config):
    """
    Composite alarm combining multiple signals
    
    Logic: ALARM when ANY child alarm is in ALARM state
    → This means: pipeline is unhealthy if ANY component fails
    
    COMPOSITE ALARM RULE SYNTAX:
    → ALARM("alarm1") OR ALARM("alarm2") OR ALARM("alarm3")
    → Can use AND, OR, NOT
    → Max 5 child alarms per composite
    """
    composite_name = f"sla-{pipeline_name}-composite-unhealthy"

    # Build alarm rule
    alarm_conditions = [f'ALARM("{name}")' for name in child_alarm_names[:5]]
    alarm_rule = " OR ".join(alarm_conditions)

    cloudwatch.put_composite_alarm(
        AlarmName=composite_name,
        AlarmDescription=(
            f"Composite health for pipeline '{pipeline_name}' | "
            f"Tier: {config['tier']} | "
            f"SLA: {config['sla_target_percent']}%"
        ),
        AlarmRule=alarm_rule,
        ActionsEnabled=True,
        AlarmActions=[
            f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-critical-alerts"
        ],
        OKActions=[
            f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-info-alerts"
        ],
        Tags=[
            {"Key": "Pipeline", "Value": pipeline_name},
            {"Key": "Tier", "Value": str(config["tier"])},
            {"Key": "Component", "Value": "Composite"},
            {"Key": "ManagedBy", "Value": "SLA-System"}
        ]
    )
    print(f"  ✅ Composite alarm created: {composite_name}")
    return composite_name


def create_all_alarms():
    """Create alarms for all registered pipelines"""
    cloudwatch = boto3.client("cloudwatch", region_name=REGION)

    print("=" * 70)
    print("🔔 CREATING SLA MONITORING ALARMS")
    print("=" * 70)

    all_composites = {}

    for pipeline_name, config in PIPELINE_REGISTRY.items():
        print(f"\n📋 Pipeline: {pipeline_name} (Tier {config['tier']})")
        print("-" * 50)

        child_alarms = []

        # Create Glue job alarms
        for job_name in config["glue_jobs"]:
            alarm_name = create_glue_job_alarm(
                cloudwatch, job_name, pipeline_name, config["tier"]
            )
            child_alarms.append(alarm_name)

        # Create data freshness alarm
        freshness_alarm = create_data_freshness_alarm(
            cloudwatch, pipeline_name, config
        )
        child_alarms.append(freshness_alarm)

        # Create composite alarm
        composite_name = create_composite_alarm(
            cloudwatch, pipeline_name, child_alarms, config
        )
        all_composites[pipeline_name] = composite_name

    print(f"\n{'=' * 70}")
    print(f"✅ All alarms created successfully")
    print(f"   Individual alarms: {sum(len(c['glue_jobs']) + 1 for c in PIPELINE_REGISTRY.values())}")
    print(f"   Composite alarms: {len(all_composites)}")
    return all_composites


if __name__ == "__main__":
    create_all_alarms()
