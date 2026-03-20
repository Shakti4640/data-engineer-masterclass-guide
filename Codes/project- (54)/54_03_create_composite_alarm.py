# file: 54_03_create_composite_alarm.py
# Purpose: Create composite alarm that aggregates individual component alarms

import boto3
import json

REGION = "us-east-2"


def load_config():
    with open("/tmp/monitoring_config.json", "r") as f:
        return json.load(f)


def create_composite_alarms():
    """
    Composite Alarms — aggregate individual alarms into meaningful signals
    
    DESIGN:
    → Composite 1: "Pipeline Data Flow Broken" (any CRITICAL component failed)
    → Composite 2: "Pipeline Degraded" (any WARNING component unhealthy)
    → Composite 3: "Pipeline SLA Breach" (data not ready by 6 AM)
    
    RULE SYNTAX:
    → ALARM("alarm-name"): true if alarm is in ALARM state
    → OK("alarm-name"): true if alarm is in OK state
    → AND, OR, NOT: boolean operators
    → Parentheses for grouping
    """
    config = load_config()
    cw_client = boto3.client("cloudwatch", region_name=REGION)

    critical_topic = config["topic_arns"]["CRITICAL"]
    sla_topic = config["topic_arns"]["SLA_BREACH"]
    warning_topic = config["topic_arns"]["WARNING"]

    print("=" * 70)
    print("🔗 CREATING COMPOSITE ALARMS")
    print("=" * 70)

    # ═══════════════════════════════════════════════════
    # COMPOSITE 1: Pipeline Data Flow Broken
    # Fires if ANY critical component has failed
    # ═══════════════════════════════════════════════════
    composite_rule_critical = (
        'ALARM("Pipeline-GlueETL-JobFailed") OR '
        'ALARM("Pipeline-SQS-DLQ-NotEmpty") OR '
        'ALARM("Pipeline-DataBrew-JobFailed") OR '
        'ALARM("Pipeline-S3Upload-NotReceived")'
    )

    cw_client.put_composite_alarm(
        AlarmName="Pipeline-CRITICAL-DataFlowBroken",
        AlarmDescription=json.dumps({
            "summary": "CRITICAL: One or more pipeline components have FAILED",
            "impact": "Data flow is interrupted. Downstream data may be stale or missing.",
            "action": "Check individual component alarms to identify which failed. "
                      "Dashboard: https://console.aws.amazon.com/.../dashboard/quickcart-pipeline",
            "severity": "CRITICAL",
            "escalation": "If not resolved in 30 min → escalate to engineering manager"
        }),
        AlarmRule=composite_rule_critical,
        AlarmActions=[critical_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Critical"},
            {"Key": "Type", "Value": "Composite"}
        ]
    )
    print(f"\n  ✅ Pipeline-CRITICAL-DataFlowBroken")
    print(f"     Rule: {composite_rule_critical}")
    print(f"     Action: → CRITICAL topic (email + SMS)")

    # ═══════════════════════════════════════════════════
    # COMPOSITE 2: Pipeline Degraded
    # Fires if any warning-level component is unhealthy
    # ═══════════════════════════════════════════════════
    composite_rule_degraded = (
        'ALARM("Pipeline-GlueCrawler-Failed") OR '
        'ALARM("Pipeline-Worker-HeartbeatMissing")'
    )

    cw_client.put_composite_alarm(
        AlarmName="Pipeline-WARNING-Degraded",
        AlarmDescription=json.dumps({
            "summary": "WARNING: Pipeline is degraded but not broken",
            "impact": "Some components unhealthy. May affect data freshness.",
            "action": "Review warning alarms. Fix during business hours.",
            "severity": "WARNING"
        }),
        AlarmRule=composite_rule_degraded,
        AlarmActions=[warning_topic],
        OKActions=[warning_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "Warning"},
            {"Key": "Type", "Value": "Composite"}
        ]
    )
    print(f"\n  ✅ Pipeline-WARNING-Degraded")
    print(f"     Rule: {composite_rule_degraded}")
    print(f"     Action: → WARNING topic (email only)")

    # ═══════════════════════════════════════════════════
    # COMPOSITE 3: SLA Breach
    # Fires if critical alarm is active AND it's past the SLA window
    # (Approximated: if critical alarm stays in ALARM for >2 hours)
    # ═══════════════════════════════════════════════════
    composite_rule_sla = (
        'ALARM("Pipeline-CRITICAL-DataFlowBroken")'
    )

    cw_client.put_composite_alarm(
        AlarmName="Pipeline-SLA-Breach",
        AlarmDescription=json.dumps({
            "summary": "SLA BREACH: Pipeline failure not resolved within SLA window",
            "impact": "Production dashboards showing stale data. "
                      "Executive reports may be incorrect.",
            "action": "IMMEDIATE: All hands on deck. "
                      "1. Identify root cause from component alarms. "
                      "2. Engage secondary on-call if primary stuck. "
                      "3. Notify CTO and stakeholders. "
                      "4. Post-incident review required.",
            "severity": "SLA_BREACH",
            "escalation": "Auto-escalates to engineering manager and CTO"
        }),
        AlarmRule=composite_rule_sla,
        AlarmActions=[sla_topic],
        OKActions=[critical_topic],
        Tags=[
            {"Key": "Pipeline", "Value": "nightly-etl"},
            {"Key": "Severity", "Value": "SLA-Breach"},
            {"Key": "Type", "Value": "Composite"}
        ]
    )
    print(f"\n  ✅ Pipeline-SLA-Breach")
    print(f"     Rule: {composite_rule_sla}")
    print(f"     Action: → SLA_BREACH topic (email + SMS + PagerDuty + CTO)")

    # --- ALARM HIERARCHY VISUALIZATION ---
    print(f"\n{'=' * 70}")
    print(f"📊 ALARM HIERARCHY")
    print(f"{'=' * 70}")
    print(f"""
    ┌─────────────────────────────────────────────────────┐
    │              Pipeline-SLA-Breach                     │
    │              (SLA topic: email+SMS+PD+CTO)           │
    │                        │                             │
    │                        ▼                             │
    │     ┌──────────────────────────────────┐             │
    │     │  Pipeline-CRITICAL-DataFlowBroken│             │
    │     │  (CRITICAL topic: email+SMS)     │             │
    │     │            │                      │             │
    │     │     ┌──────┼──────┬───────┐      │             │
    │     │     ▼      ▼      ▼       ▼      │             │
    │     │  GlueETL  DLQ  DataBrew  S3Upload│             │
    │     │  Failed   >0   Failed   Missing  │             │
    │     └──────────────────────────────────┘             │
    │                                                      │
    │     ┌──────────────────────────────────┐             │
    │     │  Pipeline-WARNING-Degraded       │             │
    │     │  (WARNING topic: email only)     │             │
    │     │            │                      │             │
    │     │     ┌──────┼──────┐              │             │
    │     │     ▼      ▼      │              │             │
    │     │  Crawler  Worker  │              │             │
    │     │  Failed   Missing │              │             │
    │     └──────────────────────────────────┘             │
    └─────────────────────────────────────────────────────┘
    """)

    # Save composite alarm names
    config = load_config()
    config["composite_alarms"] = [
        "Pipeline-CRITICAL-DataFlowBroken",
        "Pipeline-WARNING-Degraded",
        "Pipeline-SLA-Breach"
    ]
    with open("/tmp/monitoring_config.json", "w") as f:
        json.dump(config, f, indent=2)


if __name__ == "__main__":
    create_composite_alarms()