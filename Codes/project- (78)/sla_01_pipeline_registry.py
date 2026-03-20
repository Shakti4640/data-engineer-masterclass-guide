# file: sla/01_pipeline_registry.py
# Purpose: Define SLA requirements for each pipeline in machine-readable format
# This is the SINGLE SOURCE OF TRUTH for what pipelines exist and their SLAs

import json

PIPELINE_REGISTRY = {
    "finance_summary": {
        "tier": 1,
        "description": "CFO dashboard — daily revenue, costs, margins",
        "sla_target_percent": 99.9,
        "max_staleness_minutes": 120,
        "expected_completion_utc": "06:00",
        "glue_jobs": [
            "job_extract_orders",
            "job_extract_refunds",
            "job_clean_orders",
            "job_process_refunds",
            "job_build_finance_summary"
        ],
        "redshift_tables": ["gold.finance_summary"],
        "athena_views": ["silver.order_metrics"],
        "source_tables": [
            "mariadb.ecommerce.orders",
            "mariadb.payments.refunds"
        ],
        "recovery_config": {
            "max_retries": 3,
            "initial_backoff_seconds": 60,
            "backoff_multiplier": 2.0,
            "scale_up_on_retry": True,
            "initial_dpu": 10,
            "max_dpu": 40
        },
        "escalation_config": {
            "info_delay_minutes": 0,
            "warning_delay_minutes": 5,
            "critical_delay_minutes": 15,
            "emergency_delay_minutes": 30
        },
        "runbook_url": "https://wiki.quickcart.com/runbook/finance-summary",
        "owner_team": "data-platform",
        "oncall_schedule": "data-oncall-primary"
    },

    "customer_segments": {
        "tier": 2,
        "description": "Marketing segmentation — weekly refresh",
        "sla_target_percent": 99.0,
        "max_staleness_minutes": 1440,  # 24 hours
        "expected_completion_utc": "08:00",
        "glue_jobs": [
            "job_extract_customers",
            "job_segment_customers"
        ],
        "recovery_config": {
            "max_retries": 2,
            "initial_backoff_seconds": 120,
            "backoff_multiplier": 2.0,
            "scale_up_on_retry": False,
            "initial_dpu": 5,
            "max_dpu": 10
        },
        "escalation_config": {
            "info_delay_minutes": 0,
            "warning_delay_minutes": 15,
            "critical_delay_minutes": 60,
            "emergency_delay_minutes": None  # No emergency for Tier 2
        },
        "runbook_url": "https://wiki.quickcart.com/runbook/customer-segments",
        "owner_team": "data-platform",
        "oncall_schedule": "data-oncall-primary"
    }
}


def get_pipeline_config(pipeline_name):
    """Retrieve SLA config for a specific pipeline"""
    config = PIPELINE_REGISTRY.get(pipeline_name)
    if not config:
        raise ValueError(
            f"Pipeline '{pipeline_name}' not found in registry. "
            f"Available: {list(PIPELINE_REGISTRY.keys())}"
        )
    return config


def get_tier1_pipelines():
    """Get all Tier 1 (critical) pipelines"""
    return {
        name: config
        for name, config in PIPELINE_REGISTRY.items()
        if config["tier"] == 1
    }


def get_all_glue_jobs():
    """Get flat list of all monitored Glue jobs with their pipeline"""
    jobs = {}
    for pipeline_name, config in PIPELINE_REGISTRY.items():
        for job_name in config["glue_jobs"]:
            jobs[job_name] = {
                "pipeline": pipeline_name,
                "tier": config["tier"],
                "recovery_config": config["recovery_config"]
            }
    return jobs