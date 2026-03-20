# file: 53_07_list_all_resources.py
# Purpose: Inventory all DataBrew resources for documentation and auditing

import boto3
import json

REGION = "us-east-2"


def list_all_databrew_resources():
    """
    List every DataBrew resource — useful for:
    → Auditing: what's deployed
    → Cost tracking: what's running
    → Cleanup: what to delete
    → Documentation: inventory for team
    """
    databrew_client = boto3.client("databrew", region_name=REGION)

    print("=" * 70)
    print("📋 COMPLETE DATABREW RESOURCE INVENTORY")
    print("=" * 70)

    # --- DATASETS ---
    print(f"\n📊 DATASETS:")
    datasets = databrew_client.list_datasets()
    for ds in datasets.get("Datasets", []):
        source = ds.get("Input", {})
        s3_input = source.get("S3InputDefinition", {})
        print(f"  → {ds['Name']}")
        print(f"    Format: {ds.get('Format', 'N/A')}")
        print(f"    Source: s3://{s3_input.get('Bucket', '')}/{s3_input.get('Key', '')}")
        print(f"    Created: {ds.get('CreateDate', 'N/A')}")
        print()

    # --- RECIPES ---
    print(f"📝 RECIPES:")
    recipes = databrew_client.list_recipes()
    for recipe in recipes.get("Recipes", []):
        print(f"  → {recipe['Name']}")
        print(f"    Version: {recipe.get('RecipeVersion', 'N/A')}")
        print(f"    Steps: {len(recipe.get('Steps', []))}")
        print(f"    Created: {recipe.get('CreateDate', 'N/A')}")
        print()

    # --- PROJECTS ---
    print(f"🔬 PROJECTS:")
    projects = databrew_client.list_projects()
    for project in projects.get("Projects", []):
        print(f"  → {project['Name']}")
        print(f"    Dataset: {project.get('DatasetName', 'N/A')}")
        print(f"    Recipe: {project.get('RecipeName', 'N/A')}")
        print(f"    Status: {project.get('OpenedBy', 'Not open')}")
        print()

    # --- JOBS ---
    print(f"⚙️  JOBS:")
    jobs = databrew_client.list_jobs()
    for job in jobs.get("Jobs", []):
        print(f"  → {job['Name']} ({job.get('Type', 'N/A')})")
        print(f"    Dataset: {job.get('DatasetName', 'N/A')}")
        print(f"    Max Capacity: {job.get('MaxCapacity', 'N/A')}")
        print(f"    Timeout: {job.get('Timeout', 'N/A')} min")
        print(f"    Created: {job.get('CreateDate', 'N/A')}")

        # Last run status
        try:
            runs = databrew_client.list_job_runs(Name=job["Name"], MaxResults=1)
            if runs.get("JobRuns"):
                last_run = runs["JobRuns"][0]
                print(f"    Last Run: {last_run.get('State', 'N/A')} "
                      f"({last_run.get('StartedOn', 'N/A')})")
        except Exception:
            pass
        print()

    # --- SCHEDULES ---
    print(f"📅 SCHEDULES:")
    schedules = databrew_client.list_schedules()
    for schedule in schedules.get("Schedules", []):
        print(f"  → {schedule['Name']}")
        print(f"    Cron: {schedule.get('CronExpression', 'N/A')}")
        print(f"    Jobs: {', '.join(schedule.get('JobNames', []))}")
        print()

    print("=" * 70)


if __name__ == "__main__":
    list_all_databrew_resources()