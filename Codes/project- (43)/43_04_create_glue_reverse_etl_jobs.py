# file: 43_04_create_glue_reverse_etl_jobs.py
# Purpose: Upload the Glue script to S3 and create Glue job definitions

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET_NAME = "quickcart-raw-data-prod"
GLUE_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/quickcart-glue-service-role"

MARIADB_JDBC_URL = "jdbc:mysql://10.0.1.50:3306/quickcart"
MARIADB_USER = "quickcart_app"
MARIADB_PASSWORD = "[REDACTED:PASSWORD]"


def upload_glue_script():
    """Upload the reverse ETL Glue script to S3"""
    s3_client = boto3.client("s3", region_name=REGION)

    # Read the script file
    script_path = "43_03_glue_reverse_etl.py"
    with open(script_path, "r") as f:
        script_content = f.read()

    s3_key = "glue-scripts/reverse_etl_to_mariadb.py"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=script_content.encode("utf-8"),
        ContentType="text/x-python"
    )

    print(f"✅ Script uploaded: s3://{BUCKET_NAME}/{s3_key}")
    return f"s3://{BUCKET_NAME}/{s3_key}"


def create_reverse_etl_jobs(script_location):
    """
    Create two Glue jobs — one per reverse ETL target table.
    
    WHY separate jobs (not one job with parameters):
    → Different load strategies (upsert vs full_replace)
    → Different DPU requirements (500K upsert vs 2.5M replace)
    → Independent failure handling (one can fail without blocking other)
    → Step Functions can run them in parallel
    """
    glue_client = boto3.client("glue", region_name=REGION)

    jobs = [
        {
            "name": "quickcart-reverse-etl-customer-metrics",
            "description": "Reverse ETL: Redshift customer_metrics → MariaDB (upsert)",
            "target_table": "customer_metrics",
            "load_strategy": "upsert",
            "workers": 2,
            "timeout": 15
        },
        {
            "name": "quickcart-reverse-etl-customer-recommendations",
            "description": "Reverse ETL: Redshift recommendations → MariaDB (full replace)",
            "target_table": "customer_recommendations",
            "load_strategy": "full_replace",
            "workers": 4,
            "timeout": 30
        }
    ]

    for job_config in jobs:
        try:
            glue_client.create_job(
                Name=job_config["name"],
                Description=job_config["description"],
                Role=GLUE_ROLE_ARN,
                Command={
                    "Name": "glueetl",
                    "ScriptLocation": script_location,
                    "PythonVersion": "3"
                },
                DefaultArguments={
                    "--job-language": "python",
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--TempDir": f"s3://{BUCKET_NAME}/glue-temp/reverse-etl/",
                    "--enable-metrics": "true",
                    "--enable-continuous-cloudwatch-log": "true",

                    # Reverse ETL specific parameters
                    "--target_table": job_config["target_table"],
                    "--load_strategy": job_config["load_strategy"],
                    "--mariadb_url": MARIADB_JDBC_URL,
                    "--mariadb_user": MARIADB_USER,
                    "--mariadb_password": MARIADB_PASSWORD,

                    # MariaDB JDBC driver
                    "--extra-jars": f"s3://{BUCKET_NAME}/glue-libs/mariadb-java-client-3.1.4.jar",

                    # pymysql for upsert pattern
                    "--additional-python-modules": "pymysql"
                },
                MaxRetries=1,
                GlueVersion="4.0",
                NumberOfWorkers=job_config["workers"],
                WorkerType="G.1X",
                Timeout=job_config["timeout"],
                Tags={
                    "Environment": "Production",
                    "Pipeline": "ReverseETL",
                    "Project": "QuickCart-ETL"
                }
            )
            print(f"✅ Glue Job created: {job_config['name']}")
            print(f"   Strategy: {job_config['load_strategy']}")
            print(f"   Workers: {job_config['workers']}")

        except glue_client.exceptions.AlreadyExistsException:
            print(f"ℹ️  Glue Job already exists: {job_config['name']}")


def upload_mariadb_jdbc_driver():
    """
    Upload MariaDB JDBC driver JAR to S3.
    Glue needs this to connect to MariaDB.
    
    Download from: https://mariadb.com/downloads/connectors/
    Or: Maven Central → org.mariadb.jdbc:mariadb-java-client
    """
    s3_client = boto3.client("s3", region_name=REGION)
    jar_key = "glue-libs/mariadb-java-client-3.1.4.jar"

    # Check if already uploaded
    try:
        s3_client.head_object(Bucket=BUCKET_NAME, Key=jar_key)
        print(f"ℹ️  JDBC driver already exists: s3://{BUCKET_NAME}/{jar_key}")
        return
    except Exception:
        pass

    print(f"⚠️  MariaDB JDBC driver not found at: s3://{BUCKET_NAME}/{jar_key}")
    print(f"   Download from: https://mariadb.com/downloads/connectors/")
    print(f"   Upload to: s3://{BUCKET_NAME}/{jar_key}")
    print(f"   Command: aws s3 cp mariadb-java-client-3.1.4.jar s3://{BUCKET_NAME}/{jar_key}")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 SETTING UP REVERSE ETL GLUE JOBS")
    print("=" * 60)

    upload_mariadb_jdbc_driver()
    script_loc = upload_glue_script()
    create_reverse_etl_jobs(script_loc)

    print("\n✅ All reverse ETL Glue jobs ready")