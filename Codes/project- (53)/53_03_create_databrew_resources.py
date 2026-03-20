# file: 53_03_create_databrew_resources.py
# Purpose: Set up DataBrew Dataset, Project, and Profile Job via API

import boto3
import json
import time

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
ROLE_NAME = "QuickCart-DataBrew-Role"
DATASET_NAME = "quickcart-customers-raw"
PROJECT_NAME = "quickcart-customers-cleaning"
PROFILE_JOB_NAME = "quickcart-customers-profile"
RECIPE_NAME = "quickcart-customers-recipe"


def get_role_arn():
    iam_client = boto3.client("iam")
    role = iam_client.get_role(RoleName=ROLE_NAME)
    return role["Role"]["Arn"]


def create_dataset(databrew_client, role_arn):
    """
    Create DataBrew Dataset pointing to customer CSVs in S3
    
    Dataset = logical pointer to data, NOT a copy
    DataBrew reads from this location when running jobs
    """
    try:
        databrew_client.create_dataset(
            Name=DATASET_NAME,
            Format="CSV",
            FormatOptions={
                "Csv": {
                    "Delimiter": ",",
                    "HeaderRow": True
                }
            },
            Input={
                "S3InputDefinition": {
                    "Bucket": BUCKET_NAME,
                    "Key": "daily_exports/customers/"
                    # Reads ALL CSVs under this prefix
                    # For specific file: use full key path
                }
            },
            Tags={
                "Team": "Analytics",
                "Owner": "Sarah",
                "DataZone": "Bronze"
            }
        )
        print(f"✅ Dataset created: {DATASET_NAME}")
        print(f"   Source: s3://{BUCKET_NAME}/daily_exports/customers/")
    except databrew_client.exceptions.ConflictException:
        print(f"ℹ️  Dataset already exists: {DATASET_NAME}")

    return DATASET_NAME


def create_profile_job(databrew_client, role_arn):
    """
    Create Profile Job — analyzes data quality BEFORE cleaning
    
    Sarah uses profile results to understand:
    → Which columns have NULLs and how many
    → Which columns have duplicates
    → What formats exist (phone patterns, state variations)
    → Statistical distribution of each column
    """
    try:
        databrew_client.create_profile_job(
            Name=PROFILE_JOB_NAME,
            DatasetName=DATASET_NAME,
            RoleArn=role_arn,
            OutputLocation={
                "Bucket": BUCKET_NAME,
                "Key": "databrew-profiles/customers/"
            },
            Configuration={
                "DatasetStatisticsConfiguration": {
                    "IncludedStatistics": [
                        "DUPLICATE_ROWS_COUNT"
                    ]
                },
                "ColumnStatisticsConfigurations": [
                    {
                        "Selectors": [
                            {"Regex": ".*"}  # Profile ALL columns
                        ],
                        "Statistics": {
                            "IncludedStatistics": [
                                "UNIQUE_VALUES_COUNT",
                                "MISSING_VALUES_COUNT",
                                "MOST_COMMON_VALUE",
                                "MINIMUM",
                                "MAXIMUM"
                            ]
                        }
                    }
                ],
                "ProfileColumns": [
                    {"Regex": ".*"}  # Include all columns
                ]
            },
            MaxCapacity=5,      # Max 5 DataBrew nodes (cost control)
            MaxRetries=1,
            Timeout=60,         # 60 minutes max
            Tags={
                "Team": "Analytics",
                "JobType": "Profile"
            }
        )
        print(f"✅ Profile Job created: {PROFILE_JOB_NAME}")
        print(f"   Output: s3://{BUCKET_NAME}/databrew-profiles/customers/")
    except databrew_client.exceptions.ConflictException:
        print(f"ℹ️  Profile Job already exists: {PROFILE_JOB_NAME}")


def create_recipe(databrew_client):
    """
    Create recipe with transformation steps via API
    
    IN PRACTICE:
    → Sarah builds this visually in DataBrew console
    → We create it via API for reproducibility and demonstration
    → Each step is a JSON action that DataBrew understands
    """
    recipe_steps = [
        # ═══════════════════════════════════════
        # STEP 1: Remove duplicate rows
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "REMOVE_DUPLICATES",
                "Parameters": {
                    "duplicateRowsCondition": "IDENTICAL_ROWS"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 2: Trim whitespace from all string columns
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "TRIM",
                "Parameters": {
                    "sourceColumn": "name"
                }
            }
        },
        {
            "Action": {
                "Operation": "TRIM",
                "Parameters": {
                    "sourceColumn": "email"
                }
            }
        },
        {
            "Action": {
                "Operation": "TRIM",
                "Parameters": {
                    "sourceColumn": "state"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 3: Standardize name to PROPER CASE
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "PROPER_CASE",
                "Parameters": {
                    "sourceColumn": "name"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 4: Lowercase email
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "LOWER_CASE",
                "Parameters": {
                    "sourceColumn": "email"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 5: Remove spaces from email
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "REMOVE_WHITESPACE",
                "Parameters": {
                    "sourceColumn": "email"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 6: Standardize tier to lowercase
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "LOWER_CASE",
                "Parameters": {
                    "sourceColumn": "tier"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 7: Uppercase state (normalize toward code)
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "UPPER_CASE",
                "Parameters": {
                    "sourceColumn": "state"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 8: Map full state names to 2-letter codes
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "CALIFORNIA",
                    "replaceWith": "CA"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "CALIF.",
                    "replaceWith": "CA"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "NEW YORK",
                    "replaceWith": "NY"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "N.Y.",
                    "replaceWith": "NY"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "TEXAS",
                    "replaceWith": "TX"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "TEX.",
                    "replaceWith": "TX"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "FLORIDA",
                    "replaceWith": "FL"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "FLA.",
                    "replaceWith": "FL"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "ILLINOIS",
                    "replaceWith": "IL"
                }
            }
        },
        {
            "Action": {
                "Operation": "REPLACE_VALUE",
                "Parameters": {
                    "sourceColumn": "state",
                    "value": "ILL.",
                    "replaceWith": "IL"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 9: Fill missing city with "Unknown"
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "FILL_WITH_VALUE",
                "Parameters": {
                    "sourceColumn": "city",
                    "value": "Unknown"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 10: Fill missing signup_date with today
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "FILL_WITH_VALUE",
                "Parameters": {
                    "sourceColumn": "signup_date",
                    "value": "2025-01-15"
                }
            }
        },

        # ═══════════════════════════════════════
        # STEP 11: Remove non-digit characters from phone
        # (standardize to digits only)
        # ═══════════════════════════════════════
        {
            "Action": {
                "Operation": "REMOVE_SPECIAL",
                "Parameters": {
                    "sourceColumn": "phone",
                    "customSpecialCharacters": "()-+. "
                }
            }
        }
    ]

    try:
        databrew_client.create_recipe(
            Name=RECIPE_NAME,
            Steps=recipe_steps,
            Description="Customer data cleaning recipe — standardize formats, fix nulls, remove duplicates",
            Tags={
                "Team": "Analytics",
                "Owner": "Sarah",
                "Version": "1"
            }
        )
        print(f"✅ Recipe created: {RECIPE_NAME}")
        print(f"   Steps: {len(recipe_steps)}")
        print(f"   Transforms: dedup, trim, case, state mapping, null fill, phone clean")
    except databrew_client.exceptions.ConflictException:
        print(f"ℹ️  Recipe already exists: {RECIPE_NAME}")

    return RECIPE_NAME


def create_project(databrew_client, role_arn):
    """
    Create DataBrew Project — interactive workspace for Sarah
    
    When Sarah opens this in the console:
    → She sees the data in a spreadsheet view
    → She can apply/modify recipe steps visually
    → Changes are saved to the recipe
    """
    try:
        databrew_client.create_project(
            Name=PROJECT_NAME,
            DatasetName=DATASET_NAME,
            RecipeName=RECIPE_NAME,
            RoleArn=role_arn,
            Sample={
                "Size": 500,        # Show first 500 rows in interactive view
                "Type": "FIRST_N"
            },
            Tags={
                "Team": "Analytics",
                "Owner": "Sarah"
            }
        )
        print(f"✅ Project created: {PROJECT_NAME}")
        print(f"   Dataset: {DATASET_NAME}")
        print(f"   Recipe: {RECIPE_NAME}")
        print(f"   Sample: first 500 rows")
        print(f"   → Sarah can now open this in DataBrew console")
    except databrew_client.exceptions.ConflictException:
        print(f"ℹ️  Project already exists: {PROJECT_NAME}")


def setup_all_databrew_resources():
    """Master orchestrator"""
    databrew_client = boto3.client("databrew", region_name=REGION)
    role_arn = get_role_arn()

    print("=" * 70)
    print("🚀 SETTING UP GLUE DATABREW FOR SARAH")
    print("=" * 70)

    print("\n📌 Step 1: Creating Dataset...")
    create_dataset(databrew_client, role_arn)

    print("\n📌 Step 2: Creating Profile Job...")
    create_profile_job(databrew_client, role_arn)

    print("\n📌 Step 3: Creating Recipe...")
    create_recipe(databrew_client)

    print("\n📌 Step 4: Creating Project...")
    create_project(databrew_client, role_arn)

    print("\n" + "=" * 70)
    print("📊 DATABREW SETUP COMPLETE")
    print("=" * 70)
    print(f"  Dataset:      {DATASET_NAME}")
    print(f"  Profile Job:  {PROFILE_JOB_NAME}")
    print(f"  Recipe:       {RECIPE_NAME} ({11} steps)")
    print(f"  Project:      {PROJECT_NAME}")
    print(f"  IAM Role:     {ROLE_NAME}")
    print(f"")
    print(f"  NEXT STEPS FOR SARAH:")
    print(f"  1. Open AWS Console → Glue DataBrew")
    print(f"  2. Click Projects → {PROJECT_NAME}")
    print(f"  3. Review data in spreadsheet view")
    print(f"  4. Review/modify recipe steps")
    print(f"  5. Publish recipe when satisfied")
    print("=" * 70)


if __name__ == "__main__":
    setup_all_databrew_resources()