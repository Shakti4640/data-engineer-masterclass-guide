# file: 35_04_workflow_management.py
# Purpose: Manage workflow lifecycle — pause, resume, delete, modify
# Run from: Your local machine for operational tasks

import boto3
import time

AWS_REGION = "us-east-2"
glue = boto3.client("glue", region_name=AWS_REGION)

WORKFLOW_NAME = "quickcart-nightly-pipeline"

TRIGGER_START = "trigger-start-pipeline"
TRIGGER_AFTER_EXTRACT = "trigger-after-extract"
TRIGGER_AFTER_BRONZE_CRAWL = "trigger-after-bronze-crawl"
TRIGGER_AFTER_SILVER = "trigger-after-silver-transform"
TRIGGER_AFTER_SILVER_CRAWL = "trigger-after-silver-crawl"

ALL_TRIGGERS = [
    TRIGGER_START,
    TRIGGER_AFTER_EXTRACT,
    TRIGGER_AFTER_BRONZE_CRAWL,
    TRIGGER_AFTER_SILVER,
    TRIGGER_AFTER_SILVER_CRAWL
]


# ═══════════════════════════════════════════════════════════════════
# Pause Workflow (Deactivate Schedule)
# ═══════════════════════════════════════════════════════════════════
def pause_workflow():
    """
    Pause the nightly schedule WITHOUT deleting anything
    
    USE CASE:
    → Maintenance window on MariaDB → pause pipeline
    → Resume after maintenance completes
    → All configuration preserved
    
    HOW:
    → Deactivate the start trigger → schedule stops firing
    → Conditional triggers stay active but never fire (no predecessor)
    """
    print(f"\n⏸️  Pausing workflow: {WORKFLOW_NAME}")

    try:
        glue.stop_trigger(Name=TRIGGER_START)
        print(f"   ✅ Schedule trigger deactivated: {TRIGGER_START}")
        print(f"   → Nightly 2AM runs will NOT fire")
        print(f"   → Manual runs still possible via API")
    except Exception as e:
        print(f"   ❌ Error: {e}")


def resume_workflow():
    """Resume the nightly schedule"""
    print(f"\n▶️  Resuming workflow: {WORKFLOW_NAME}")

    try:
        glue.start_trigger(Name=TRIGGER_START)
        print(f"   ✅ Schedule trigger reactivated: {TRIGGER_START}")
        print(f"   → Next run at 2:00 AM UTC")
    except Exception as e:
        print(f"   ❌ Error: {e}")


# ═══════════════════════════════════════════════════════════════════
# Switch Between Scheduled and On-Demand
# ═══════════════════════════════════════════════════════════════════
def switch_to_on_demand():
    """
    Convert start trigger from SCHEDULED to ON_DEMAND
    Used during development/testing
    """
    print(f"\n🔧 Switching start trigger to ON_DEMAND...")

    # Delete scheduled trigger
    try:
        glue.stop_trigger(Name=TRIGGER_START)
        time.sleep(2)
        glue.delete_trigger(Name=TRIGGER_START)
        print(f"   Deleted scheduled trigger")
    except Exception:
        pass

    # Recreate as on-demand
    glue.create_trigger(
        Name=TRIGGER_START,
        WorkflowName=WORKFLOW_NAME,
        Type="ON_DEMAND",
        Description="Manual start trigger (development mode)",
        Actions=[
            {
                "JobName": "quickcart-mariadb-to-s3-parquet",
                "Timeout": 60
            }
        ]
    )
    glue.start_trigger(Name=TRIGGER_START)
    print(f"   ✅ Start trigger is now ON_DEMAND")
    print(f"   → Start manually: glue.start_workflow_run(Name='{WORKFLOW_NAME}')")


def switch_to_scheduled(cron_expression="cron(0 2 * * ? *)"):
    """Convert start trigger back to SCHEDULED"""
    print(f"\n🔧 Switching start trigger to SCHEDULED...")
    print(f"   Cron: {cron_expression}")

    try:
        glue.stop_trigger(Name=TRIGGER_START)
        time.sleep(2)
        glue.delete_trigger(Name=TRIGGER_START)
    except Exception:
        pass

    glue.create_trigger(
        Name=TRIGGER_START,
        WorkflowName=WORKFLOW_NAME,
        Type="SCHEDULED",
        Schedule=cron_expression,
        Description=f"Nightly pipeline - {cron_expression}",
        StartOnCreation=True,
        Actions=[
            {
                "JobName": "quickcart-mariadb-to-s3-parquet",
                "Timeout": 60
            }
        ]
    )
    print(f"   ✅ Start trigger is now SCHEDULED: {cron_expression}")


# ═══════════════════════════════════════════════════════════════════
# Delete Entire Workflow (Clean Teardown)
# ═══════════════════════════════════════════════════════════════════
def delete_workflow():
    """
    Completely remove workflow and all triggers
    
    ORDER MATTERS:
    1. Stop all triggers (deactivate)
    2. Delete all triggers
    3. Delete workflow
    
    Jobs and Crawlers are NOT deleted — they exist independently
    """
    print(f"\n🗑️  Deleting workflow: {WORKFLOW_NAME}")
    print(f"   WARNING: This removes the workflow and all triggers")

    # Step 1: Stop all triggers
    print(f"\n   Stopping triggers...")
    for trigger_name in ALL_TRIGGERS:
        try:
            glue.stop_trigger(Name=trigger_name)
            print(f"   ⏹️  Stopped: {trigger_name}")
        except Exception:
            pass

    time.sleep(3)

    # Step 2: Delete all triggers
    print(f"\n   Deleting triggers...")
    for trigger_name in ALL_TRIGGERS:
        try:
            glue.delete_trigger(Name=trigger_name)
            print(f"   🗑️  Deleted: {trigger_name}")
        except Exception:
            pass

    time.sleep(2)

    # Step 3: Delete workflow
    try:
        glue.delete_workflow(Name=WORKFLOW_NAME)
        print(f"\n   ✅ Workflow deleted: {WORKFLOW_NAME}")
    except Exception as e:
        print(f"\n   ❌ Failed to delete workflow: {e}")

    print(f"\n   NOTE: Jobs and Crawlers still exist — delete separately if needed")


# ═══════════════════════════════════════════════════════════════════
# Print Workflow Status Summary
# ═══════════════════════════════════════════════════════════════════
def print_workflow_status():
    """Comprehensive status dump"""
    print(f"\n📊 WORKFLOW STATUS: {WORKFLOW_NAME}")
    print("=" * 70)

    # Workflow info
    try:
        response = glue.get_workflow(Name=WORKFLOW_NAME)
        wf = response["Workflow"]
        print(f"   Name: {wf['Name']}")
        print(f"   Created: {wf.get('CreatedOn', 'N/A')}")
        print(f"   Modified: {wf.get('LastModifiedOn', 'N/A')}")
        print(f"   MaxConcurrentRuns: {wf.get('MaxConcurrentRuns', 'N/A')}")

        last_run = wf.get("LastRun", {})
        if last_run:
            print(f"   Last Run Status: {last_run.get('Status', 'N/A')}")
            print(f"   Last Run Started: {last_run.get('StartedOn', 'N/A')}")
    except glue.exceptions.EntityNotFoundException:
        print(f"   ❌ Workflow not found: {WORKFLOW_NAME}")
        return

    # Trigger statuses
    print(f"\n   TRIGGERS:")
    for trigger_name in ALL_TRIGGERS:
        try:
            response = glue.get_trigger(Name=trigger_name)
            trigger = response["Trigger"]
            state = trigger.get("State", "UNKNOWN")
            trigger_type = trigger.get("Type", "UNKNOWN")
            schedule = trigger.get("Schedule", "")

            icon = "🟢" if state == "ACTIVATED" else "🔴" if state == "DEACTIVATED" else "⚪"
            schedule_str = f" [{schedule}]" if schedule else ""
            print(f"   {icon} {trigger_name}: {state} ({trigger_type}){schedule_str}")
        except glue.exceptions.EntityNotFoundException:
            print(f"   ❌ {trigger_name}: NOT FOUND")

    # Job statuses
    print(f"\n   JOBS:")
    job_names = [
        "quickcart-mariadb-to-s3-parquet",
        "quickcart-silver-layer-transforms",
        "quickcart-s3-to-redshift"
    ]
    for job_name in job_names:
        try:
            runs = glue.get_job_runs(JobName=job_name, MaxResults=1)
            if runs["JobRuns"]:
                last = runs["JobRuns"][0]
                state = last["JobRunState"]
                duration = last.get("ExecutionTime", 0)
                icon = "✅" if state == "SUCCEEDED" else "❌"
                print(f"   {icon} {job_name}: {state} (last run: {duration}s)")
            else:
                print(f"   ⚪ {job_name}: Never run")
        except Exception:
            print(f"   ❌ {job_name}: Error fetching status")

    # Crawler statuses
    print(f"\n   CRAWLERS:")
    crawler_names = ["crawl-bronze-mariadb", "crawl-silver-quickcart"]
    for crawler_name in crawler_names:
        try:
            response = glue.get_crawler(Name=crawler_name)
            crawler = response["Crawler"]
            state = crawler.get("State", "UNKNOWN")
            last_crawl = crawler.get("LastCrawl", {})
            crawl_status = last_crawl.get("Status", "Never run")
            icon = "✅" if crawl_status == "SUCCEEDED" else "❌" if crawl_status == "FAILED" else "⚪"
            print(f"   {icon} {crawler_name}: {state} (last crawl: {crawl_status})")
        except Exception:
            print(f"   ❌ {crawler_name}: Error fetching status")

    print("=" * 70)


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    action = sys.argv[1] if len(sys.argv) > 1 else "status"

    if action == "status":
        print_workflow_status()
    elif action == "pause":
        pause_workflow()
    elif action == "resume":
        resume_workflow()
    elif action == "on-demand":
        switch_to_on_demand()
    elif action == "scheduled":
        cron = sys.argv[2] if len(sys.argv) > 2 else "cron(0 2 * * ? *)"
        switch_to_scheduled(cron)
    elif action == "delete":
        confirm = input("Type 'DELETE' to confirm: ")
        if confirm == "DELETE":
            delete_workflow()
        else:
            print("Cancelled.")
    else:
        print(f"""
USAGE:
    python 35_04_workflow_management.py status
    python 35_04_workflow_management.py pause
    python 35_04_workflow_management.py resume
    python 35_04_workflow_management.py on-demand
    python 35_04_workflow_management.py scheduled ["cron(0 3 * * ? *)"]
    python 35_04_workflow_management.py delete
        """)