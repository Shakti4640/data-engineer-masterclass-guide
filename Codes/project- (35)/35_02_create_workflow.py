# file: 35_02_create_workflow.py
# Purpose: Create complete Glue Workflow with all triggers
# This is the CORE of this project — the orchestration layer

import boto3
import json
import time
from datetime import datetime

AWS_REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]

glue = boto3.client("glue", region_name=AWS_REGION)

# ─── COMPONENT NAMES ────────────────────────────────────────────
WORKFLOW_NAME = "quickcart-nightly-pipeline"

# Jobs (from previous projects)
JOB_EXTRACT = "quickcart-mariadb-to-s3-parquet"         # Project 31
JOB_SILVER = "quickcart-silver-layer-transforms"          # Project 34
JOB_REDSHIFT = "quickcart-s3-to-redshift"                 # Project 32

# Crawlers (from Part 1)
CRAWLER_BRONZE = "crawl-bronze-mariadb"
CRAWLER_SILVER = "crawl-silver-quickcart"

# Trigger names
TRIGGER_START = "trigger-start-pipeline"
TRIGGER_AFTER_EXTRACT = "trigger-after-extract"
TRIGGER_AFTER_BRONZE_CRAWL = "trigger-after-bronze-crawl"
TRIGGER_AFTER_SILVER = "trigger-after-silver-transform"
TRIGGER_AFTER_SILVER_CRAWL = "trigger-after-silver-crawl"


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Create the Workflow
# ═══════════════════════════════════════════════════════════════════
def create_workflow():
    """
    Create the workflow container
    
    DefaultRunProperties: key-value pairs passed to ALL child jobs
    → All jobs receive --extraction_date with same value
    → Ensures consistency across the entire pipeline
    """
    print("\n📋 Creating Workflow...")

    today = datetime.now().strftime("%Y-%m-%d")

    try:
        glue.create_workflow(
            Name=WORKFLOW_NAME,
            Description=(
                "Nightly pipeline: MariaDB → Bronze → Silver → Redshift. "
                "Chains 3 ETL Jobs and 2 Crawlers with conditional triggers."
            ),
            DefaultRunProperties={
                "--extraction_date": today,
                "--pipeline_name": WORKFLOW_NAME,
                "--pipeline_version": "1.0"
            },
            MaxConcurrentRuns=1,  # Only one run at a time
            Tags={
                "Environment": "Production",
                "Team": "Data-Engineering",
                "Schedule": "Nightly-2AM-UTC"
            }
        )
        print(f"   ✅ Workflow created: {WORKFLOW_NAME}")
    except glue.exceptions.AlreadyExistsException:
        # Update existing workflow
        glue.update_workflow(
            Name=WORKFLOW_NAME,
            DefaultRunProperties={
                "--extraction_date": today,
                "--pipeline_name": WORKFLOW_NAME,
                "--pipeline_version": "1.0"
            },
            MaxConcurrentRuns=1
        )
        print(f"   ✅ Workflow updated: {WORKFLOW_NAME}")


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Create Trigger 1 — Schedule Start
# ═══════════════════════════════════════════════════════════════════
def create_trigger_start():
    """
    First trigger: SCHEDULED — fires every day at 2:00 AM UTC
    
    Starts: Job 1 (MariaDB → Bronze extraction)
    
    CRON FORMAT (Glue uses 6-field cron):
    cron(minutes hours day-of-month month day-of-week year)
    cron(0 2 * * ? *)  → 2:00 AM UTC every day
    
    NOTE: Use ON_DEMAND for testing, switch to SCHEDULED for production
    """
    print("\n⏰ Creating Trigger: Start (Scheduled)...")

    try:
        glue.create_trigger(
            Name=TRIGGER_START,
            WorkflowName=WORKFLOW_NAME,
            Type="SCHEDULED",
            Schedule="cron(0 2 * * ? *)",    # 2:00 AM UTC daily
            Description="Start nightly pipeline at 2:00 AM UTC",
            StartOnCreation=False,            # Don't start immediately
            Actions=[
                {
                    "JobName": JOB_EXTRACT,
                    "Arguments": {
                        "--extraction_date": "{{extraction_date}}"
                        # Note: workflow run properties substitute at runtime
                    },
                    "Timeout": 60  # 60 minute timeout for this job
                }
            ],
            Tags={"Step": "1-Extract"}
        )
        print(f"   ✅ Trigger created: {TRIGGER_START}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {TRIGGER_START}")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Create Trigger 2 — After Extract → Crawl Bronze
# ═══════════════════════════════════════════════════════════════════
def create_trigger_after_extract():
    """
    CONDITIONAL trigger: fires when Job 1 (Extract) SUCCEEDS
    
    Starts: Crawler A (crawl bronze/ for new partitions)
    
    PREDICATE EXPLAINED:
    → Conditions: list of {LogicalOperator, JobName, State}
    → Logical: "AND" — ALL conditions must be true
    → State: "SUCCEEDED" — job must have succeeded
    → If job FAILS or TIMES OUT → trigger never fires → pipeline stops
    """
    print("\n🔗 Creating Trigger: After Extract...")

    try:
        glue.create_trigger(
            Name=TRIGGER_AFTER_EXTRACT,
            WorkflowName=WORKFLOW_NAME,
            Type="CONDITIONAL",
            Description="Run bronze crawler after extraction succeeds",
            StartOnCreation=False,
            Predicate={
                "Logical": "AND",
                "Conditions": [
                    {
                        "LogicalOperator": "EQUALS",
                        "JobName": JOB_EXTRACT,
                        "State": "SUCCEEDED"
                    }
                ]
            },
            Actions=[
                {
                    "CrawlerName": CRAWLER_BRONZE
                    # Crawlers don't accept Arguments
                }
            ],
            Tags={"Step": "2-CrawlBronze"}
        )
        print(f"   ✅ Trigger created: {TRIGGER_AFTER_EXTRACT}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {TRIGGER_AFTER_EXTRACT}")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Create Trigger 3 — After Bronze Crawl → Silver Transform
# ═══════════════════════════════════════════════════════════════════
def create_trigger_after_bronze_crawl():
    """
    CONDITIONAL trigger: fires when Crawler A SUCCEEDS
    Starts: Job 2 (Silver layer transforms)
    """
    print("\n🔗 Creating Trigger: After Bronze Crawl...")

    try:
        glue.create_trigger(
            Name=TRIGGER_AFTER_BRONZE_CRAWL,
            WorkflowName=WORKFLOW_NAME,
            Type="CONDITIONAL",
            Description="Run silver transforms after bronze crawl succeeds",
            StartOnCreation=False,
            Predicate={
                "Logical": "AND",
                "Conditions": [
                    {
                        "LogicalOperator": "EQUALS",
                        "CrawlerName": CRAWLER_BRONZE,
                        "CrawlState": "SUCCEEDED"
                    }
                ]
            },
            Actions=[
                {
                    "JobName": JOB_SILVER,
                    "Arguments": {
                        "--extraction_date": "{{extraction_date}}"
                    },
                    "Timeout": 90
                }
            ],
            Tags={"Step": "3-SilverTransform"}
        )
        print(f"   ✅ Trigger created: {TRIGGER_AFTER_BRONZE_CRAWL}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {TRIGGER_AFTER_BRONZE_CRAWL}")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Create Trigger 4 — After Silver Transform → Crawl Silver
# ═══════════════════════════════════════════════════════════════════
def create_trigger_after_silver():
    """
    CONDITIONAL trigger: fires when Job 2 (Silver) SUCCEEDS
    Starts: Crawler B (crawl silver/ for new tables/partitions)
    """
    print("\n🔗 Creating Trigger: After Silver Transform...")

    try:
        glue.create_trigger(
            Name=TRIGGER_AFTER_SILVER,
            WorkflowName=WORKFLOW_NAME,
            Type="CONDITIONAL",
            Description="Run silver crawler after transforms succeed",
            StartOnCreation=False,
            Predicate={
                "Logical": "AND",
                "Conditions": [
                    {
                        "LogicalOperator": "EQUALS",
                        "JobName": JOB_SILVER,
                        "State": "SUCCEEDED"
                    }
                ]
            },
            Actions=[
                {
                    "CrawlerName": CRAWLER_SILVER
                }
            ],
            Tags={"Step": "4-CrawlSilver"}
        )
        print(f"   ✅ Trigger created: {TRIGGER_AFTER_SILVER}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {TRIGGER_AFTER_SILVER}")


# ═══════════════════════════════════════════════════════════════════
# STEP 6: Create Trigger 5 — After Silver Crawl → Load Redshift
# ═══════════════════════════════════════════════════════════════════
def create_trigger_after_silver_crawl():
    """
    CONDITIONAL trigger: fires when Crawler B SUCCEEDS
    Starts: Job 3 (Silver → Redshift load)
    This is the FINAL step in the pipeline
    """
    print("\n🔗 Creating Trigger: After Silver Crawl...")

    try:
        glue.create_trigger(
            Name=TRIGGER_AFTER_SILVER_CRAWL,
            WorkflowName=WORKFLOW_NAME,
            Type="CONDITIONAL",
            Description="Load Redshift after silver crawl succeeds",
            StartOnCreation=False,
            Predicate={
                "Logical": "AND",
                "Conditions": [
                    {
                        "LogicalOperator": "EQUALS",
                        "CrawlerName": CRAWLER_SILVER,
                        "CrawlState": "SUCCEEDED"
                    }
                ]
            },
            Actions=[
                {
                    "JobName": JOB_REDSHIFT,
                    "Arguments": {
                        "--source_s3_path": f"s3://{S3_BUCKET}/silver/",
                        "--target_tables": "order_details,daily_revenue_summary,customer_rfm",
                        "--extraction_date": "{{extraction_date}}"
                    },
                    "Timeout": 60
                }
            ],
            Tags={"Step": "5-LoadRedshift"}
        )
        print(f"   ✅ Trigger created: {TRIGGER_AFTER_SILVER_CRAWL}")
    except glue.exceptions.AlreadyExistsException:
        print(f"   ℹ️  Trigger already exists: {TRIGGER_AFTER_SILVER_CRAWL}")


S3_BUCKET = "quickcart-datalake-prod"


# ═══════════════════════════════════════════════════════════════════
# STEP 7: Activate All Triggers
# ═══════════════════════════════════════════════════════════════════
def activate_triggers():
    """
    CRITICAL: Triggers must be ACTIVATED to work
    
    After creation: trigger state = CREATED
    After activation: trigger state = ACTIVATED
    
    Only ACTIVATED triggers evaluate their predicates
    CREATED triggers are dormant — they exist but do nothing
    """
    print("\n🔌 Activating all triggers...")

    triggers = [
        TRIGGER_START,
        TRIGGER_AFTER_EXTRACT,
        TRIGGER_AFTER_BRONZE_CRAWL,
        TRIGGER_AFTER_SILVER,
        TRIGGER_AFTER_SILVER_CRAWL
    ]

    for trigger_name in triggers:
        try:
            # Check current state
            response = glue.get_trigger(Name=trigger_name)
            current_state = response["Trigger"]["State"]

            if current_state == "ACTIVATED":
                print(f"   ℹ️  Already active: {trigger_name}")
            else:
                glue.start_trigger(Name=trigger_name)
                print(f"   ✅ Activated: {trigger_name}")
        except Exception as e:
            print(f"   ⚠️  {trigger_name}: {e}")

    # Wait for activation to propagate
    time.sleep(5)


# ═══════════════════════════════════════════════════════════════════
# STEP 8: Verify Workflow Configuration
# ═══════════════════════════════════════════════════════════════════
def verify_workflow():
    """Print the complete workflow graph for verification"""
    print("\n🔍 Verifying Workflow Configuration...")
    print("-" * 60)

    response = glue.get_workflow(Name=WORKFLOW_NAME)
    workflow = response["Workflow"]

    print(f"   Name: {workflow['Name']}")
    print(f"   Status: {workflow.get('LastRun', {}).get('Status', 'Never run')}")
    print(f"   MaxConcurrentRuns: {workflow.get('MaxConcurrentRuns', 'N/A')}")

    # Get workflow graph
    print(f"\n   EXECUTION GRAPH:")
    print(f"   ================")

    graph = workflow.get("Graph", {})
    nodes = graph.get("Nodes", [])
    edges = graph.get("Edges", [])

    if nodes:
        for node in nodes:
            node_type = node.get("Type", "Unknown")
            node_name = node.get("Name", "Unknown")
            trigger_details = node.get("TriggerDetails", {})
            job_details = node.get("JobDetails", [])
            crawler_details = node.get("CrawlerDetails", [])

            if node_type == "TRIGGER":
                trigger_type = trigger_details.get("Trigger", {}).get("Type", "")
                print(f"   🔹 TRIGGER: {node_name} ({trigger_type})")
            elif node_type == "JOB":
                for jd in job_details:
                    job_name = jd.get("JobRuns", [{}])[0].get("JobName", node_name) if jd.get("JobRuns") else node_name
                    print(f"   🟢 JOB: {node_name}")
            elif node_type == "CRAWLER":
                print(f"   🕷️  CRAWLER: {node_name}")

        print(f"\n   EDGES (dependencies):")
        for edge in edges:
            src = edge.get("SourceId", "?")
            dst = edge.get("DestinationId", "?")
            print(f"   {src} → {dst}")
    else:
        print(f"   (graph not yet populated — run workflow once)")

    print("-" * 60)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("🚀 CREATING GLUE WORKFLOW")
    print("=" * 60)

    # Create workflow container
    create_workflow()

    # Create triggers (in execution order)
    create_trigger_start()
    create_trigger_after_extract()
    create_trigger_after_bronze_crawl()
    create_trigger_after_silver()
    create_trigger_after_silver_crawl()

    # Activate triggers
    activate_triggers()

    # Verify
    verify_workflow()

    print("\n" + "=" * 60)
    print("✅ WORKFLOW CREATED AND ACTIVATED")
    print(f"   Name: {WORKFLOW_NAME}")
    print(f"   Schedule: cron(0 2 * * ? *)  (2:00 AM UTC daily)")
    print(f"   Steps: 3 Jobs + 2 Crawlers")
    print(f"   Triggers: 5 (1 scheduled + 4 conditional)")
    print("=" * 60)