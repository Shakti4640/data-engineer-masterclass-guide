# file: 53_08_cleanup.py
# Purpose: Remove all DataBrew resources created in this project

import boto3
import json
import time

REGION = "us-east-2"

DATASET_NAME = "quickcart-customers-raw"
PROJECT_NAME = "quickcart-customers-cleaning"
RECIPE_NAME = "quickcart-customers-recipe"
PROFILE_JOB_NAME = "quickcart-customers-profile"
RECIPE_JOB_NAME = "quickcart-customers-clean-job"
SCHEDULE_NAME = "quickcart-customers-nightly-clean"
ROLE_NAME = "QuickCart-DataBrew-Role"


def cleanup_all():
    """
    Delete all DataBrew resources in correct ORDER
    
    ORDER MATTERS:
    1. Schedules first (removes automated triggers)
    2. Jobs next (can't delete if running)
    3. Projects (references datasets and recipes)
    4. Recipes (may be referenced by projects)
    5. Datasets (may be referenced by projects/jobs)
    6. IAM role (last — nothing references it from DataBrew)
    """
    databrew_client = boto3.client("databrew", region_name=REGION)
    iam_client = boto3.client("iam")

    print("=" * 70)
    print("🧹 CLEANING UP PROJECT 53 — DATABREW RESOURCES")
    print("=" * 70)

    # --- Step 1: Delete Schedule ---
    print("\n📌 Step 1: Deleting schedule...")
    try:
        databrew_client.delete_schedule(Name=SCHEDULE_NAME)
        print(f"  ✅ Schedule deleted: {SCHEDULE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 2: Delete Jobs ---
    print("\n📌 Step 2: Deleting jobs...")
    for job_name in [RECIPE_JOB_NAME, PROFILE_JOB_NAME]:
        try:
            # Stop if running
            try:
                runs = databrew_client.list_job_runs(Name=job_name, MaxResults=1)
                for run in runs.get("JobRuns", []):
                    if run.get("State") in ["STARTING", "RUNNING"]:
                        databrew_client.stop_job_run(
                            Name=job_name,
                            RunId=run["RunId"]
                        )
                        print(f"  ⏹️  Stopped running job: {job_name}")
                        time.sleep(5)
            except Exception:
                pass

            databrew_client.delete_job(Name=job_name)
            print(f"  ✅ Job deleted: {job_name}")
        except Exception as e:
            print(f"  ⚠️  {job_name}: {e}")

    # --- Step 3: Delete Project ---
    print("\n📌 Step 3: Deleting project...")
    try:
        databrew_client.delete_project(Name=PROJECT_NAME)
        print(f"  ✅ Project deleted: {PROJECT_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 4: Delete Recipe ---
    print("\n📌 Step 4: Deleting recipe...")
    try:
        # Must delete all versions
        # First delete the latest working version
        databrew_client.delete_recipe_version(
            Name=RECIPE_NAME,
            RecipeVersion="LATEST_WORKING"
        )
        print(f"  ✅ Recipe working version deleted")
    except Exception as e:
        print(f"  ⚠️  Working version: {e}")

    try:
        # Then delete published version(s)
        databrew_client.delete_recipe_version(
            Name=RECIPE_NAME,
            RecipeVersion="1.0"
        )
        print(f"  ✅ Recipe v1.0 deleted")
    except Exception as e:
        print(f"  ⚠️  Published version: {e}")

    # --- Step 5: Delete Dataset ---
    print("\n📌 Step 5: Deleting dataset...")
    try:
        databrew_client.delete_dataset(Name=DATASET_NAME)
        print(f"  ✅ Dataset deleted: {DATASET_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 6: Delete IAM Role ---
    print("\n📌 Step 6: Deleting IAM role...")
    try:
        iam_client.delete_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="DataBrewS3Access"
        )
        iam_client.delete_role(RoleName=ROLE_NAME)
        print(f"  ✅ IAM Role deleted: {ROLE_NAME}")
    except Exception as e:
        print(f"  ⚠️  {e}")

    # --- Step 7: Clean S3 output (optional) ---
    print("\n📌 Step 7: Cleaning S3 output (optional)...")
    s3_client = boto3.client("s3", region_name=REGION)
    prefixes_to_clean = [
        "cleaned/customers/",
        "databrew-profiles/customers/"
    ]

    for prefix in prefixes_to_clean:
        try:
            response = s3_client.list_objects_v2(
                Bucket="quickcart-raw-data-prod",
                Prefix=prefix
            )
            if "Contents" in response:
                objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
                s3_client.delete_objects(
                    Bucket="quickcart-raw-data-prod",
                    Delete={"Objects": objects}
                )
                print(f"  ✅ Cleaned S3 prefix: {prefix} ({len(objects)} objects)")
            else:
                print(f"  ℹ️  No objects at: {prefix}")
        except Exception as e:
            print(f"  ⚠️  {prefix}: {e}")

    print("\n" + "=" * 70)
    print("🎉 ALL PROJECT 53 RESOURCES CLEANED UP")
    print("=" * 70)


if __name__ == "__main__":
    cleanup_all()