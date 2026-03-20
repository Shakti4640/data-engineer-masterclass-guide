# file: 35_03_run_and_monitor_workflow.py
# Purpose: Manually trigger workflow and monitor execution
# Use for: Testing, on-demand runs, re-processing specific dates

import boto3
import time
from datetime import datetime

AWS_REGION = "us-east-2"
glue = boto3.client("glue", region_name=AWS_REGION)

WORKFLOW_NAME = "quickcart-nightly-pipeline"


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Start Workflow Run
# ═══════════════════════════════════════════════════════════════════
def start_workflow(extraction_date=None):
    """
    Manually start a workflow run with custom properties
    
    RunProperties override DefaultRunProperties for this specific run
    Use case: re-process a specific date without changing default
    """
    if extraction_date is None:
        extraction_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🚀 Starting workflow: {WORKFLOW_NAME}")
    print(f"   Extraction date: {extraction_date}")

    response = glue.start_workflow_run(
        Name=WORKFLOW_NAME,
        RunProperties={
            "--extraction_date": extraction_date,
            "--run_type": "manual",
            "--triggered_by": "35_03_run_and_monitor_workflow.py"
        }
    )

    run_id = response["RunId"]
    print(f"   ✅ Workflow Run started: {run_id}")

    return run_id


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Monitor Workflow Run
# ═══════════════════════════════════════════════════════════════════
def monitor_workflow(run_id):
    """
    Monitor workflow execution — shows real-time status of each node
    
    WORKFLOW STATES:
    → RUNNING: at least one node is executing
    → COMPLETED: all triggers evaluated (but jobs may have FAILED)
    → STOPPING: cancel requested
    → STOPPED: cancelled
    → ERROR: infrastructure error
    
    NODE STATES (Jobs):
    → STARTING → RUNNING → SUCCEEDED / FAILED / TIMEOUT
    
    NODE STATES (Crawlers):
    → RUNNING → SUCCEEDED / FAILED / CANCELLED
    
    IMPORTANT: COMPLETED != all jobs succeeded
    → Must check individual node states
    """
    print(f"\n📊 Monitoring Workflow Run: {run_id}")
    print("=" * 80)

    completed_nodes = set()
    terminal_workflow_states = {"COMPLETED", "STOPPED", "ERROR"}

    while True:
        response = glue.get_workflow_run(
            Name=WORKFLOW_NAME,
            RunId=run_id
        )
        run = response["Run"]
        workflow_status = run.get("Status", "UNKNOWN")
        started_on = run.get("StartedOn", "")

        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n   [{ts}] Workflow: {workflow_status}")

        # Get detailed graph with node statuses
        graph = run.get("Graph", {})
        nodes = graph.get("Nodes", [])

        all_succeeded = True
        any_failed = False

        for node in nodes:
            node_type = node.get("Type", "")
            node_name = ""
            node_state = ""

            if node_type == "JOB":
                job_details = node.get("JobDetails", [])
                for jd in job_details:
                    runs = jd.get("JobRuns", [])
                    if runs:
                        latest = runs[0]
                        node_name = latest.get("JobName", "?")
                        node_state = latest.get("JobRunState", "PENDING")
                        duration = latest.get("ExecutionTime", 0)

                        # Print status change
                        node_key = f"JOB:{node_name}"
                        if node_state in ("SUCCEEDED", "FAILED", "TIMEOUT"):
                            if node_key not in completed_nodes:
                                icon = "✅" if node_state == "SUCCEEDED" else "❌"
                                print(f"   {icon} JOB: {node_name} → {node_state} ({duration}s)")
                                completed_nodes.add(node_key)
                        elif node_state == "RUNNING":
                            print(f"   🔄 JOB: {node_name} → RUNNING ({duration}s)")

                        if node_state == "FAILED":
                            any_failed = True
                        if node_state != "SUCCEEDED":
                            all_succeeded = False

            elif node_type == "CRAWLER":
                crawler_details = node.get("CrawlerDetails", [])
                for cd in crawler_details:
                    crawls = cd.get("Crawls", [])
                    if crawls:
                        latest = crawls[0]
                        node_name = latest.get("CrawlerName", "?")  # Fixed key name
                        node_state = latest.get("State", "PENDING")

                        node_key = f"CRAWLER:{node_name}"
                        if node_state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                            if node_key not in completed_nodes:
                                icon = "✅" if node_state == "SUCCEEDED" else "❌"
                                print(f"   {icon} CRAWLER: {node_name} → {node_state}")
                                completed_nodes.add(node_key)
                        elif node_state == "RUNNING":
                            print(f"   🕷️  CRAWLER: {node_name} → RUNNING")
                        if node_state == "FAILED":
                            any_failed = True
                        if node_state != "SUCCEEDED":
                            all_succeeded = False

            elif node_type == "TRIGGER":
                trigger_details = node.get("TriggerDetails", {})
                trigger = trigger_details.get("Trigger", {})
                trigger_name = trigger.get("Name", "?")
                trigger_state = trigger.get("State", "?")
                # Triggers don't need status printing — they fire silently

        # Check if workflow is done
        if workflow_status in terminal_workflow_states:
            break

        time.sleep(15)

    # ── FINAL STATUS ──
    print("\n" + "=" * 80)

    # Re-fetch final state
    final = glue.get_workflow_run(Name=WORKFLOW_NAME, RunId=run_id)
    final_run = final["Run"]
    final_status = final_run.get("Status", "UNKNOWN")
    started = final_run.get("StartedOn", "")
    completed = final_run.get("CompletedOn", "")

    if started and completed:
        duration = (completed - started).total_seconds()
    else:
        duration = 0

    # Count node results
    final_graph = final_run.get("Graph", {})
    final_nodes = final_graph.get("Nodes", [])

    succeeded_jobs = 0
    failed_jobs = 0
    succeeded_crawlers = 0
    failed_crawlers = 0
    not_started = 0

    for node in final_nodes:
        node_type = node.get("Type", "")
        if node_type == "JOB":
            for jd in node.get("JobDetails", []):
                runs = jd.get("JobRuns", [])
                if runs:
                    state = runs[0].get("JobRunState", "")
                    if state == "SUCCEEDED":
                        succeeded_jobs += 1
                    elif state in ("FAILED", "TIMEOUT", "ERROR"):
                        failed_jobs += 1
                    else:
                        not_started += 1
                else:
                    not_started += 1
        elif node_type == "CRAWLER":
            for cd in node.get("CrawlerDetails", []):
                crawls = cd.get("Crawls", [])
                if crawls:
                    state = crawls[0].get("State", "")
                    if state == "SUCCEEDED":
                        succeeded_crawlers += 1
                    elif state in ("FAILED", "CANCELLED"):
                        failed_crawlers += 1
                    else:
                        not_started += 1
                else:
                    not_started += 1

    print(f"   WORKFLOW: {final_status}")
    print(f"   Duration: {duration:.0f}s ({duration/60:.1f} min)")
    print(f"   Jobs:     {succeeded_jobs} succeeded, {failed_jobs} failed")
    print(f"   Crawlers: {succeeded_crawlers} succeeded, {failed_crawlers} failed")
    print(f"   Not started: {not_started} (downstream of failed step)")

    if failed_jobs > 0 or failed_crawlers > 0:
        print(f"\n   ❌ PIPELINE HAD FAILURES")
        print(f"   → Check individual job/crawler logs in Glue Console")
        print(f"   → Glue → Workflows → {WORKFLOW_NAME} → History → Click run")
    elif not_started > 0:
        print(f"\n   ⚠️  PIPELINE INCOMPLETE — {not_started} steps not started")
        print(f"   → A predecessor step failed, blocking downstream steps")
    else:
        print(f"\n   ✅ PIPELINE FULLY SUCCEEDED — all 5 steps completed")

    print("=" * 80)

    return {
        "workflow_status": final_status,
        "duration_seconds": duration,
        "succeeded_jobs": succeeded_jobs,
        "failed_jobs": failed_jobs,
        "succeeded_crawlers": succeeded_crawlers,
        "failed_crawlers": failed_crawlers,
        "not_started": not_started
    }


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Get Workflow Run History
# ═══════════════════════════════════════════════════════════════════
def get_workflow_history(max_results=10):
    """
    Show recent workflow run history
    Useful for: tracking pipeline reliability, finding failure patterns
    """
    print(f"\n📜 WORKFLOW RUN HISTORY (last {max_results})")
    print("-" * 80)
    print(f"   {'Run ID':<20} {'Status':<12} {'Started':<22} {'Duration':>10}")
    print(f"   {'-'*20} {'-'*12} {'-'*22} {'-'*10}")

    response = glue.get_workflow_runs(
        Name=WORKFLOW_NAME,
        MaxResults=max_results
    )

    total_runs = 0
    successful_runs = 0

    for run in response.get("Runs", []):
        run_id = run.get("WorkflowRunId", "?")[:18]
        status = run.get("Status", "?")
        started = run.get("StartedOn", "")
        completed = run.get("CompletedOn", "")

        if started and completed:
            duration = f"{(completed - started).total_seconds():.0f}s"
        elif started:
            duration = "Running..."
        else:
            duration = "N/A"

        started_str = started.strftime("%Y-%m-%d %H:%M") if started else "N/A"
        icon = "✅" if status == "COMPLETED" else "❌" if status in ("STOPPED", "ERROR") else "🔄"

        print(f"   {icon} {run_id:<18} {status:<12} {started_str:<22} {duration:>10}")

        total_runs += 1
        if status == "COMPLETED":
            successful_runs += 1

    if total_runs > 0:
        success_rate = round(successful_runs / total_runs * 100, 1)
        print(f"\n   Success rate: {successful_runs}/{total_runs} ({success_rate}%)")


# ═══════════════════════════════════════════════════════════════════
# STEP 4: Stop Running Workflow
# ═══════════════════════════════════════════════════════════════════
def stop_workflow(run_id):
    """
    Emergency stop — cancel a running workflow
    
    WHAT HAPPENS:
    → Currently running job/crawler is NOT immediately killed
    → It finishes its current operation, then stops
    → Pending triggers are cancelled — downstream steps won't start
    → Workflow status changes to STOPPING → STOPPED
    """
    print(f"\n🛑 Stopping workflow run: {run_id}")

    try:
        glue.stop_workflow_run(
            Name=WORKFLOW_NAME,
            RunId=run_id
        )
        print(f"   ✅ Stop requested — workflow will stop after current step completes")
    except Exception as e:
        print(f"   ❌ Failed to stop: {e}")


# ═══════════════════════════════════════════════════════════════════
# STEP 5: Re-Run Specific Date (Common Operation)
# ═══════════════════════════════════════════════════════════════════
def rerun_for_date(target_date):
    """
    Re-run entire pipeline for a specific historical date
    
    USE CASE:
    → Data quality issue found in Jan 10 data
    → Fix source data → re-run pipeline for 2025-01-10
    → Overwrites bronze/silver/Redshift data for that date
    → Historical partitions untouched
    """
    print(f"\n🔄 RE-RUNNING PIPELINE FOR DATE: {target_date}")
    print(f"   This will reprocess: extract → transform → load for {target_date}")

    run_id = start_workflow(extraction_date=target_date)
    result = monitor_workflow(run_id)

    return result


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("🔧 WORKFLOW MANAGEMENT")
    print("=" * 60)

    action = sys.argv[1] if len(sys.argv) > 1 else "run"

    if action == "run":
        # Run for today
        date = sys.argv[2] if len(sys.argv) > 2 else None
        run_id = start_workflow(extraction_date=date)
        monitor_workflow(run_id)

    elif action == "history":
        get_workflow_history()

    elif action == "rerun":
        if len(sys.argv) < 3:
            print("Usage: python 35_03_... rerun YYYY-MM-DD")
            sys.exit(1)
        rerun_for_date(sys.argv[2])

    elif action == "stop":
        if len(sys.argv) < 3:
            print("Usage: python 35_03_... stop <run_id>")
            sys.exit(1)
        stop_workflow(sys.argv[2])

    else:
        print(f"""
USAGE:
    python 35_03_run_and_monitor_workflow.py run [YYYY-MM-DD]
    python 35_03_run_and_monitor_workflow.py history
    python 35_03_run_and_monitor_workflow.py rerun YYYY-MM-DD
    python 35_03_run_and_monitor_workflow.py stop <run_id>
        """)

    print("\n🏁 DONE")