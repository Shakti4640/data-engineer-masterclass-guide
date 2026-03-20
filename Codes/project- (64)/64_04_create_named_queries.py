# file: 64_04_create_named_queries.py
# Run from: Account B (222222222222)
# Purpose: Create saved/named queries that Marketing team can run
# These are the "productionized" versions of the test queries

import boto3

REGION = "us-east-2"
WORKGROUP = "federated-queries"

athena = boto3.client("athena", region_name=REGION)


NAMED_QUERIES = [
    {
        "name": "Campaign ROI — Last 30 Days",
        "description": "Cross-account: DynamoDB campaigns (Acct C) + S3 orders (Acct B). Shows ROAS per campaign.",
        "database": "analytics_lake",
        "sql": """
-- Campaign ROI Analysis
-- Sources: Account C (DynamoDB) + Account B (S3 Data Lake)
-- Last updated: auto-refreshed from live sources

SELECT
    camp.campaign_name,
    camp.channel,
    COUNT(DISTINCT camp.user_id) AS converting_users,
    ROUND(SUM(CAST(camp.cost AS DOUBLE)), 2) AS total_spend,
    COUNT(ord.order_id) AS total_orders,
    ROUND(COALESCE(SUM(ord.total_amount), 0), 2) AS total_revenue,
    ROUND(
        COALESCE(SUM(ord.total_amount), 0) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0),
        2
    ) AS roas,
    CASE
        WHEN COALESCE(SUM(ord.total_amount), 0) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0) > 5 THEN 'EXCELLENT'
        WHEN COALESCE(SUM(ord.total_amount), 0) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0) > 2 THEN 'GOOD'
        WHEN COALESCE(SUM(ord.total_amount), 0) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0) > 1 THEN 'BREAK-EVEN'
        ELSE 'UNDERPERFORMING'
    END AS performance_tier
FROM dynamodb_acct_c.default.campaign_events camp
LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
    ON camp.user_id = ord.customer_id
    AND ord.order_date >= DATE_ADD('day', -30, CURRENT_DATE)
WHERE camp.event_type = 'conversion'
GROUP BY camp.campaign_name, camp.channel
HAVING SUM(CAST(camp.cost AS DOUBLE)) > 0
ORDER BY total_revenue DESC
"""
    },
    {
        "name": "Campaign Performance by Customer Tier",
        "description": "Three-account query: DynamoDB (Acct C) + S3 orders (Acct B) + MariaDB customers (Acct A)",
        "database": "analytics_lake",
        "sql": """
-- Campaign Performance segmented by Customer Tier
-- Sources: Account A (MariaDB) + Account B (S3) + Account C (DynamoDB)
-- Shows which campaigns attract which customer tiers

SELECT
    camp.campaign_name,
    camp.channel,
    cust.tier AS customer_tier,
    COUNT(DISTINCT camp.user_id) AS users,
    COUNT(ord.order_id) AS orders,
    ROUND(COALESCE(SUM(ord.total_amount), 0), 2) AS revenue,
    ROUND(COALESCE(AVG(ord.total_amount), 0), 2) AS avg_order_value,
    ROUND(SUM(CAST(camp.cost AS DOUBLE)), 2) AS spend
FROM dynamodb_acct_c.default.campaign_events camp
LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
    ON camp.user_id = ord.customer_id
    AND ord.order_date >= DATE_ADD('day', -90, CURRENT_DATE)
LEFT JOIN mysql_acct_a.quickcart.customers cust
    ON camp.user_id = cust.customer_id
WHERE camp.event_type = 'conversion'
GROUP BY camp.campaign_name, camp.channel, cust.tier
ORDER BY revenue DESC
LIMIT 50
"""
    },
    {
        "name": "Channel Attribution Summary",
        "description": "Which marketing channels drive the most revenue? Cross-account DynamoDB + S3.",
        "database": "analytics_lake",
        "sql": """
-- Channel Attribution Summary
-- Sources: Account C (DynamoDB) + Account B (S3)

SELECT
    camp.channel,
    COUNT(DISTINCT camp.campaign_name) AS campaigns,
    COUNT(DISTINCT camp.user_id) AS unique_users,
    ROUND(SUM(CAST(camp.cost AS DOUBLE)), 2) AS total_spend,
    COUNT(ord.order_id) AS total_orders,
    ROUND(COALESCE(SUM(ord.total_amount), 0), 2) AS total_revenue,
    ROUND(
        COALESCE(SUM(ord.total_amount), 0) / NULLIF(SUM(CAST(camp.cost AS DOUBLE)), 0),
        2
    ) AS roas,
    ROUND(
        COALESCE(SUM(ord.total_amount), 0) / NULLIF(COUNT(DISTINCT camp.user_id), 0),
        2
    ) AS revenue_per_user
FROM dynamodb_acct_c.default.campaign_events camp
LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
    ON camp.user_id = ord.customer_id
WHERE camp.event_type = 'conversion'
GROUP BY camp.channel
ORDER BY total_revenue DESC
"""
    },
    {
        "name": "Materialized Campaign ROI Refresh",
        "description": "CTAS: Materialize cross-account campaign data to S3 Parquet for fast repeated queries",
        "database": "analytics_lake",
        "sql": """
-- Materialize campaign ROI data to S3 Parquet
-- Run daily after ETL to pre-compute cross-account join
-- Subsequent queries on this table: fast native S3 scan

-- DROP existing table first (CTAS doesn't support IF NOT EXISTS)
-- DROP TABLE IF EXISTS analytics_lake.campaign_roi_daily;

CREATE TABLE analytics_lake.campaign_roi_daily
WITH (
    format = 'PARQUET',
    write_compression = 'SNAPPY',
    external_location = 's3://quickcart-analytics-datalake/gold/campaign_roi_daily/',
    partitioned_by = ARRAY['channel']
)
AS
SELECT
    camp.campaign_name,
    camp.user_id,
    CAST(camp.cost AS DOUBLE) AS cost,
    camp.event_type,
    ord.order_id,
    ord.total_amount,
    ord.order_date,
    ord.status AS order_status,
    CURRENT_TIMESTAMP AS materialized_at,
    camp.channel
FROM dynamodb_acct_c.default.campaign_events camp
LEFT JOIN AwsDataCatalog.analytics_lake.silver_orders ord
    ON camp.user_id = ord.customer_id
WHERE camp.event_type IN ('conversion', 'click')
"""
    }
]


def create_named_queries():
    """
    Create saved queries in Athena for the Marketing team
    
    WHY NAMED QUERIES:
    → Marketing team doesn't need to write SQL
    → Pre-validated, pre-tested queries
    → Discoverable in Athena console under "Saved queries"
    → Can be shared via query ID or name
    """
    print("=" * 60)
    print("📝 CREATING NAMED QUERIES FOR MARKETING TEAM")
    print("=" * 60)

    for nq in NAMED_QUERIES:
        try:
            response = athena.create_named_query(
                Name=nq["name"],
                Description=nq["description"],
                Database=nq["database"],
                QueryString=nq["sql"],
                WorkGroup=WORKGROUP
            )
            query_id = response["NamedQueryId"]
            print(f"\n✅ Created: {nq['name']}")
            print(f"   ID: {query_id}")
            print(f"   Workgroup: {WORKGROUP}")
        except Exception as e:
            print(f"\n❌ Failed: {nq['name']}: {str(e)[:100]}")

    print(f"\n{'='*60}")
    print(f"📋 Marketing team can find these in:")
    print(f"   Athena Console → Saved queries → Workgroup: {WORKGROUP}")
    print(f"{'='*60}")


if __name__ == "__main__":
    create_named_queries()