# file: migration/04_gold_builder.py
# Purpose: Build Gold layer aggregations and load into Redshift Serverless
# Runs after Silver layer is populated (triggered by Step Functions — Project 55)

import boto3
import json
import time

REGION = "us-east-2"
ACCOUNT_B = "[REDACTED:BANK_ACCOUNT_NUMBER]"
ACCOUNT_C = "[REDACTED:BANK_ACCOUNT_NUMBER]"
REDSHIFT_WORKGROUP = "quickcart-serving"
REDSHIFT_DATABASE = "quickcart"
GOLD_BUCKET = "quickcart-gold-prod"


def build_gold_finance_summary():
    """
    Create Gold finance_summary using Athena CTAS (Project 36)
    
    Aggregates Silver orders + refunds into daily revenue metrics
    This is the table that feeds the CFO dashboard
    """
    athena = boto3.client("athena", region_name=REGION)

    # Drop existing gold table if exists
    drop_query = "DROP TABLE IF EXISTS gold.finance_summary"
    run_athena_query(athena, drop_query, "gold")

    # Create Gold table via CTAS
    ctas_query = """
    CREATE TABLE gold.finance_summary
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://quickcart-gold-prod/finance_summary/',
        partitioned_by = ARRAY['year', 'month']
    ) AS
    SELECT
        o.order_date,
        COUNT(DISTINCT o.order_id) as total_orders,
        SUM(o.order_total) as gross_revenue,
        COALESCE(SUM(r.refund_amount), 0) as total_refunds,
        SUM(o.order_total) - COALESCE(SUM(r.refund_amount), 0) as net_revenue,
        COUNT(DISTINCT o.customer_id) as unique_customers,
        AVG(o.order_total) as avg_order_value,
        YEAR(o.order_date) as year,
        MONTH(o.order_date) as month
    FROM silver.ecommerce_orders o
    LEFT JOIN silver.payments_refunds r
        ON o.order_id = r.order_id
    WHERE o.order_date IS NOT NULL
    GROUP BY o.order_date, YEAR(o.order_date), MONTH(o.order_date)
    """

    run_athena_query(athena, ctas_query, "gold")
    print("✅ Gold finance_summary created via CTAS")


def build_gold_customer_360():
    """Customer 360 view — aggregates across all customer touchpoints"""
    athena = boto3.client("athena", region_name=REGION)

    drop_query = "DROP TABLE IF EXISTS gold.customer_360"
    run_athena_query(athena, drop_query, "gold")

    ctas_query = """
    CREATE TABLE gold.customer_360
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://quickcart-gold-prod/customer_360/'
    ) AS
    SELECT
        c.customer_id,
        c.city,
        c.tier,
        c.signup_date,
        COUNT(DISTINCT o.order_id) as lifetime_orders,
        COALESCE(SUM(o.order_total), 0) as lifetime_revenue,
        COALESCE(AVG(o.order_total), 0) as avg_order_value,
        MIN(o.order_date) as first_order_date,
        MAX(o.order_date) as last_order_date,
        DATE_DIFF('day', MAX(o.order_date), CURRENT_DATE) as days_since_last_order,
        COALESCE(SUM(r.refund_amount), 0) as lifetime_refunds,
        COUNT(DISTINCT r.refund_id) as refund_count
    FROM silver.ecommerce_customers c
    LEFT JOIN silver.ecommerce_orders o ON c.customer_id = o.customer_id
    LEFT JOIN silver.payments_refunds r ON o.order_id = r.order_id
    GROUP BY c.customer_id, c.city, c.tier, c.signup_date
    """

    run_athena_query(athena, ctas_query, "gold")
    print("✅ Gold customer_360 created via CTAS")


def load_gold_to_redshift():
    """
    Load Gold Parquet from S3 into Redshift Serverless (Account C)
    Uses Redshift Data API (cross-account via IAM role)
    
    STRATEGY:
    → COPY from Gold S3 into Redshift staging tables
    → MERGE into production tables (idempotent — Project 65)
    → ANALYZE for query optimization (Project 39)
    """
    redshift_data = boto3.client("redshift-data", region_name=REGION)

    # Assume role in Account C for Redshift access (Project 56)
    sts = boto3.client("sts", region_name=REGION)
    assumed = sts.assume_role(
        RoleArn=f"arn:aws:iam::{ACCOUNT_C}:role/CrossAccountRedshiftLoader",
        RoleSessionName="migration-gold-loader"
    )

    redshift_data_c = boto3.client(
        "redshift-data",
        region_name=REGION,
        aws_access_key_id=assumed["Credentials"]["AccessKeyId"],
        aws_secret_access_key=assumed["Credentials"]["SecretAccessKey"],
        aws_session_token=assumed["Credentials"]["SessionToken"]
    )

    # Create schema and tables if not exist
    ddl_statements = [
        "CREATE SCHEMA IF NOT EXISTS gold;",

        """
        CREATE TABLE IF NOT EXISTS gold.finance_summary (
            order_date DATE,
            total_orders BIGINT,
            gross_revenue DECIMAL(14,2),
            total_refunds DECIMAL(14,2),
            net_revenue DECIMAL(14,2),
            unique_customers BIGINT,
            avg_order_value DECIMAL(10,2),
            year INT,
            month INT
        )
        DISTSTYLE KEY DISTKEY(order_date)
        SORTKEY(order_date);
        """,

        """
        CREATE TABLE IF NOT EXISTS gold.customer_360 (
            customer_id VARCHAR(20),
            city VARCHAR(100),
            tier VARCHAR(20),
            signup_date DATE,
            lifetime_orders BIGINT,
            lifetime_revenue DECIMAL(14,2),
            avg_order_value DECIMAL(10,2),
            first_order_date DATE,
            last_order_date DATE,
            days_since_last_order INT,
            lifetime_refunds DECIMAL(14,2),
            refund_count BIGINT
        )
        DISTSTYLE KEY DISTKEY(customer_id)
        SORTKEY(customer_id);
        """
    ]

    for ddl in ddl_statements:
        execute_redshift_sql(redshift_data_c, ddl)

    # COPY finance_summary from Gold S3
    copy_finance = f"""
    COPY gold.finance_summary
    FROM 's3://{GOLD_BUCKET}/finance_summary/'
    IAM_ROLE 'arn:aws:iam::{ACCOUNT_C}:role/RedshiftS3ReadRole'
    FORMAT AS PARQUET;
    """
    execute_redshift_sql(redshift_data_c, "TRUNCATE gold.finance_summary;")
    execute_redshift_sql(redshift_data_c, copy_finance)
    print("✅ finance_summary loaded to Redshift")

    # COPY customer_360
    copy_customer = f"""
    COPY gold.customer_360
    FROM 's3://{GOLD_BUCKET}/customer_360/'
    IAM_ROLE 'arn:aws:iam::{ACCOUNT_C}:role/RedshiftS3ReadRole'
    FORMAT AS PARQUET;
    """
    execute_redshift_sql(redshift_data_c, "TRUNCATE gold.customer_360;")
    execute_redshift_sql(redshift_data_c, copy_customer)
    print("✅ customer_360 loaded to Redshift")

    # ANALYZE tables (Project 39)
    execute_redshift_sql(redshift_data_c, "ANALYZE gold.finance_summary;")
    execute_redshift_sql(redshift_data_c, "ANALYZE gold.customer_360;")
    print("✅ ANALYZE complete")


def execute_redshift_sql(client, sql):
    """Execute SQL on Redshift Serverless via Data API"""
    response = client.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP,
        Database=REDSHIFT_DATABASE,
        Sql=sql
    )
    statement_id = response["Id"]

    # Wait for completion
    while True:
        status = client.describe_statement(Id=statement_id)
        state = status["Status"]
        if state == "FINISHED":
            return status
        elif state in ("FAILED", "ABORTED"):
            raise RuntimeError(
                f"Redshift SQL failed: {status.get('Error', 'Unknown')}"
            )
        time.sleep(2)


def run_athena_query(athena_client, query, database):
    """Execute Athena query and wait for completion"""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={
            "OutputLocation": "s3://quickcart-athena-results-prod/gold-builder/"
        }
    )
    exec_id = response["QueryExecutionId"]

    while True:
        status = athena_client.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            return
        elif state in ("FAILED", "CANCELLED"):
            error = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            raise RuntimeError(f"Athena query failed: {error}")
        time.sleep(3)


if __name__ == "__main__":
    print("=" * 70)
    print("🏗️  BUILDING GOLD LAYER + REDSHIFT LOADING")
    print("=" * 70)

    build_gold_finance_summary()
    build_gold_customer_360()
    load_gold_to_redshift()

    print("\n🎯 Gold layer complete — Redshift ready for queries")