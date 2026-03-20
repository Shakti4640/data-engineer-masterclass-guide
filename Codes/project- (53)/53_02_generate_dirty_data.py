# file: 53_02_generate_dirty_data.py
# Purpose: Create realistic dirty customer CSV for DataBrew to clean

import csv
import os
import random
import boto3
from datetime import datetime

BUCKET_NAME = "quickcart-raw-data-prod"
REGION = "us-east-2"
LOCAL_DIR = "/tmp/exports"
TODAY = datetime.now().strftime("%Y%m%d")


def generate_dirty_customers(num_rows=5000):
    """
    Generate customer CSV with INTENTIONAL quality issues
    that Sarah will fix using DataBrew recipes
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)
    filepath = os.path.join(LOCAL_DIR, f"customers_{TODAY}.csv")

    # --- DIRTY DATA TEMPLATES ---
    name_variations = [
        ("John Smith", "john smith", "JOHN SMITH", "John  Smith"),
        ("Jane Doe", "jane doe", "JANE DOE", "Jane  Doe"),
        ("Bob Wilson", "bob wilson", "BOB WILSON", "Bob   Wilson"),
    ]

    phone_formats = [
        "555-{0:04d}",
        "(555) {0:04d}",
        "555{0:04d}",
        "+1-555-{0:04d}",
        "1555{0:04d}",
        "+1 555 {0:04d}",
        "555.{0:04d}",
    ]

    state_variations = {
        "California": ["California", "CA", "ca", "Calif.", "calif", "CALIFORNIA"],
        "New York": ["New York", "NY", "ny", "N.Y.", "new york", "NEW YORK"],
        "Texas": ["Texas", "TX", "tx", "Tex.", "texas", "TEXAS"],
        "Florida": ["Florida", "FL", "fl", "Fla.", "florida", "FLORIDA"],
        "Illinois": ["Illinois", "IL", "il", "Ill.", "illinois", "ILLINOIS"],
    }

    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
              "Philadelphia", "San Antonio", "San Diego", None, None, ""]

    email_templates = [
        "{name}@example.com",          # Valid
        "{name}@gmail.com",            # Valid
        "{name} @example.com",         # Invalid: space
        "{name}@",                     # Invalid: no domain
        "test@test.test",              # Suspicious
        "{name}example.com",           # Invalid: no @
        "{name}@example..com",         # Invalid: double dot
    ]

    tiers = ["bronze", "silver", "gold", "platinum", "Bronze", "SILVER", "Gold"]

    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "customer_id", "name", "email", "phone",
            "city", "state", "tier", "signup_date"
        ])

        for i in range(1, num_rows + 1):
            cust_id = f"CUST-{i:06d}"

            # Name with variations (sometimes duplicate IDs)
            if random.random() < 0.05:  # 5% duplicates
                cust_id = f"CUST-{random.randint(1, i):06d}"
            name_group = random.choice(name_variations)
            name = random.choice(name_group)

            # Email with quality issues
            email_template = random.choice(email_templates)
            clean_name = name.lower().replace(" ", ".")
            email = email_template.format(name=clean_name)

            # Phone in random format
            phone_num = random.randint(1000, 9999)
            phone = random.choice(phone_formats).format(phone_num)

            # City with nulls
            city = random.choice(cities)
            if city == "":
                city = ""  # Empty string (different from NULL)

            # State with variations
            state_name = random.choice(list(state_variations.keys()))
            state = random.choice(state_variations[state_name])

            # Tier with case issues
            tier = random.choice(tiers)

            # Signup date with some nulls
            if random.random() < 0.08:  # 8% missing
                signup_date = ""
            else:
                signup_date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

            writer.writerow([
                cust_id, name, email, phone,
                city, state, tier, signup_date
            ])

    size_kb = round(os.path.getsize(filepath) / 1024, 1)
    print(f"✅ Generated: {filepath} ({num_rows} rows, {size_kb} KB)")
    print(f"   Intentional issues:")
    print(f"   → ~5% duplicate customer_ids")
    print(f"   → ~14% bad emails (missing @, spaces, suspicious)")
    print(f"   → 7 different phone formats")
    print(f"   → 6 variations per state name")
    print(f"   → ~12% missing cities, ~8% missing signup_dates")
    print(f"   → Inconsistent tier casing")

    return filepath


def upload_dirty_data(filepath):
    """Upload dirty CSV to S3 for DataBrew to process"""
    s3_client = boto3.client("s3", region_name=REGION)
    s3_key = f"daily_exports/customers/customers_{TODAY}.csv"

    s3_client.upload_file(
        Filename=filepath,
        Bucket=BUCKET_NAME,
        Key=s3_key,
        ExtraArgs={"ContentType": "text/csv"}
    )
    print(f"✅ Uploaded to s3://{BUCKET_NAME}/{s3_key}")
    return s3_key


if __name__ == "__main__":
    filepath = generate_dirty_customers(5000)
    upload_dirty_data(filepath)