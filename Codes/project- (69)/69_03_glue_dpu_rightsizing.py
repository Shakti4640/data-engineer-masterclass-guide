# file: 69_03_glue_dpu_rightsizing.py
# Run from: Account[REDACTED:BANK_ACCOUNT_NUMBER]2222)
# Purpose: Analyze and optimize Glue job DPU allocation
# SAVINGS: ~$135/month

import boto3
from datetime import datetime, timedelta

REGION = "us-east-2"
glue = boto3.client("glue", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def analyze_glue_job_usage():
    """
    Analyze each Glue job's actual DPU usage vs allocated
    
    METRICS:
    → glue.driver.aggregate.elapsedTime: total runtime
    → glue.ALL.system.cpuSystemLoad: CPU utilization
    → glue.driver.aggregate.numCompletedTasks: task count
    → DPUSeconds from job run details: actual DPU consumed
    """
    print("=" * 70)
    print("📊 GLUE JOB DPU ANALYSIS")
    print("=" * 70)

    # List all jobs
    response = glue.get_jobs()
    jobs = response.get("Jobs", [])

    print(f"\n{'Job Name':<45} {'Workers':>8} {'Type':>6} {'Avg Min':>8} {'DPU-Hr':>8} {'$/Run':>8}")
    print("-" * 90)

    recommendations = []

    for job_def in jobs:
        job_name = job_def["Name"]
        workers = job_def.get("NumberOfWorkers", job_def.get("MaxCapacity", "?"))
        worker_type = job_def.get("WorkerType", "Standard")

        # Get last 30 days of job runs
        runs_response = glue.get_job_runs(JobName=job_name, MaxResults=30)
        runs = runs_response.get("JobRuns", [])

        if not runs:
            print(f"  {job_name:<45} {workers:>8} {worker_type:>6} {'no runs':>8}")
            continue

        # Analyze runs
        successful_runs = [r for r in runs if r.get("JobRunState") == "SUCCEEDED"]
        if not successful_runs:
            print(f"  {job_name:<45} {workers:>8} {worker_type:>6} {'no success':>8}")
            continue

        durations = [r.get("ExecutionTime", 0) for r in successful_runs]
        dpu_seconds_list = [r.get("DPUSeconds", 0) for r in successful_runs]

        avg_duration_min = (sum(durations) / len(durations)) / 60
        avg_dpu_seconds = sum(dpu_seconds_list) / len(dpu_seconds_list)
        avg_dpu_hours = avg_dpu_seconds / 3600
        cost_per_run = avg_dpu_hours * 0.44  # $0.44/DPU-hour

        print(f"  {job_name:<45} {workers:>8} {worker_type:>6} {avg_duration_min:>7.1f}m {avg_dpu_hours:>7.2f} ${cost_per_run:>6.2f}")

        # Recommendation: if DPU utilization is low
        if isinstance(workers, int):
            allocated_dpu_hours = workers * (avg_duration_min / 60)
            utilization = avg_dpu_hours / max(allocated_dpu_hours, 0.01) * 100

            if utilization < 50:
                new_workers = max(2, int(workers * utilization / 70))
                old_cost = cost_per_run
                new_cost = (new_workers * (avg_duration_min / 60)) * 0.44
                savings_per_run = old_cost - new_cost

                recommendations.append({
                    "job": job_name,
                    "current_workers": workers,
                    "recommended_workers": new_workers,
                    "utilization": round(utilization, 1),
                    "savings_per_run": round(savings_per_run, 2),
                    "runs_per_month": len(successful_runs)
                })

    # Print recommendations
    if recommendations:
        print(f"\n{'='*70}")
        print(f"💡 OPTIMIZATION RECOMMENDATIONS")
        print(f"{'='*70}")

        total_monthly_savings = 0

        for rec in recommendations:
            monthly_savings = rec["savings_per_run"] * rec["runs_per_month"]
            total_monthly_savings += monthly_savings

            print(f"\n   📋 {rec['job']}:")
            print(f"      Current workers: {rec['current_workers']}")
            print(f"      DPU utilization: {rec['utilization']}%")
            print(f"      Recommended workers: {rec['recommended_workers']}")
            print(f"      Savings/run: ${rec['savings_per_run']:.2f}")
            print(f"      Runs/month: ~{rec['runs_per_month']}")
            print(f"      Monthly savings: ${monthly_savings:.2f}")

        print(f"\n   💰 TOTAL MONTHLY SAVINGS: ${total_monthly_savings:.2f}")

    return recommendations


def apply_dpu_optimizations(recommendations, auto_apply=False):
    """
    Apply DPU optimizations to Glue jobs
    
    CAUTION: reduce gradually and monitor
    → First: reduce by 30% → run 7 days → check duration/failures
    → Then: reduce to target if stable
    """
    if not auto_apply:
        print(f"\n⚠️  Auto-apply is OFF. To apply optimizations:")
        print(f"   Call apply_dpu_optimizations(recommendations, auto_apply=True)")
        return

    for rec in recommendations:
        job_name = rec["job"]
        new_workers = rec["recommended_workers"]

        try:
            # Get current job definition
            job_def = glue.get_job(JobName=job_name)["Job"]

            # Update worker count
            job_update = {
                "Role": job_def["Role"],
                "Command": job_def["Command"],
                "NumberOfWorkers": new_workers,
                "WorkerType": job_def.get("WorkerType", "G.1X"),
                "GlueVersion": job_def.get("GlueVersion", "4.0")
            }

            # Preserve other settings
            for key in ["DefaultArguments", "Connections", "Timeout", "MaxRetries"]:
                if key in job_def:
                    job_update[key] = job_def[key]

            glue.update_job(JobName=job_name, JobUpdate=job_update)
            print(f"✅ {job_name}: workers {rec['current_workers']} → {new_workers}")

        except Exception as e:
            print(f"❌ {job_name}: {str(e)[:100]}")


if __name__ == "__main__":
    recommendations = analyze_glue_job_usage()
    apply_dpu_optimizations(recommendations, auto_apply=False)