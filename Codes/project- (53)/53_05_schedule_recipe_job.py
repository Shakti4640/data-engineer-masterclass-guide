# file: 53_05_schedule_recipe_job.py
# Purpose: Schedule DataBrew recipe job to run every night at 2 AM

import boto3
import json

REGION = "us-east-2"
RECIPE_JOB_NAME = "quickcart-customers-clean-job"
SCHEDULE_NAME = "quickcart-customers-nightly-clean"


def create_schedule():
    """
    Schedule the recipe job to run automatically
    
    TIMING RATIONALE:
    → Source file lands at midnight (Project 29 cron)
    → Buffer: 1 hour for upload to complete
    → DataBrew runs at 2 AM
    → Downstream Glue ETL runs at 4 AM (Project 35 workflow)
    → Redshift COPY at 5 AM
    → Dashboard ready by 6 AM
    
    CRON FORMAT (DataBrew uses standard cron):
    → cron(minutes hours day-of-month month day-of-week year)
    → cron(0 2 * * ? *) = every day at 2:00 AM UTC
    """
    databrew_client = boto3.client("databrew", region_name=REGION)

    try:
        databrew_client.create_schedule(
            Name=SCHEDULE_NAME,
            CronExpression="cron(0 2 * * ? *)",    # 2 AM UTC daily
            JobNames=[RECIPE_JOB_NAME],
            Tags={
                "Team": "Analytics",
                "Schedule": "Nightly",
                "Owner": "Data-Engineering"
            }
        )
        print(f"✅ Schedule created: {SCHEDULE_NAME}")
        print(f"   Cron: cron(0 2 * * ? *) → Every day at 2:00 AM UTC")
        print(f"   Job: {RECIPE_JOB_NAME}")

    except databrew_client.exceptions.ConflictException:
        print(f"ℹ️  Schedule already exists: {SCHEDULE_NAME}")

    # Also show alternative schedules for reference
    print(f"\n📋 COMMON SCHEDULE PATTERNS:")
    print(f"   Every day at 2 AM:        cron(0 2 * * ? *)")
    print(f"   Weekdays only at 6 AM:    cron(0 6 ? * MON-FRI *)")
    print(f"   Every 6 hours:            cron(0 0/6 * * ? *)")
    print(f"   First of month at 1 AM:   cron(0 1 1 * ? *)")
    print(f"   Every Monday at midnight:  cron(0 0 ? * MON *)")


def list_schedules():
    """List all DataBrew schedules"""
    databrew_client = boto3.client("databrew", region_name=REGION)

    response = databrew_client.list_schedules()

    print(f"\n📋 ALL DATABREW SCHEDULES:")
    print(f"{'─' * 60}")

    for schedule in response.get("Schedules", []):
        name = schedule.get("Name", "Unknown")
        cron = schedule.get("CronExpression", "N/A")
        jobs = schedule.get("JobNames", [])
        print(f"  {name}")
        print(f"    Cron: {cron}")
        print(f"    Jobs: {', '.join(jobs)}")
        print()


def update_schedule_to_weekdays_only():
    """
    Update schedule — example: run only on weekdays
    
    WHY:
    → Weekends have no new data files
    → Running job on empty input wastes money
    → Weekday-only saves ~28% of scheduled job costs
    """
    databrew_client = boto3.client("databrew", region_name=REGION)

    databrew_client.update_schedule(
        Name=SCHEDULE_NAME,
        CronExpression="cron(0 2 ? * MON-FRI *)"   # Weekdays at 2 AM
    )
    print(f"✅ Schedule updated to weekdays only: cron(0 2 ? * MON-FRI *)")


if __name__ == "__main__":
    create_schedule()
    list_schedules()