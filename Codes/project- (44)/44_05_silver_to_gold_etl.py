# file: 44_05_silver_to_gold_etl.py
# Purpose: Glue ETL script that creates Gold zone business aggregations from Silver
# This script runs INSIDE AWS Glue

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "datalake_bucket",
    "process_date"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args["datalake_bucket"]
process_date = args["process_date"]
year, month, day = process_date.split("-")

silver_base = f"s3://{bucket}/silver"
gold_base = f"s3://{bucket}/gold"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# READ SILVER TABLES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"📥 Reading Silver tables...")

orders_df = spark.read.parquet(f"{silver_base}/orders/")
customers_df = spark.read.parquet(f"{silver_base}/customers/")
products_df = spark.read.parquet(f"{silver_base}/products/")

print(f"   Orders:    {orders_df.count():,} rows")
print(f"   Customers: {customers_df.count():,} rows")
print(f"   Products:  {products_df.count():,} rows")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GOLD TABLE 1: customer_360
# Complete customer profile with aggregated metrics
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🏆 Building Gold: customer_360")

# Aggregate orders per customer
order_aggs = orders_df \
    .filter(F.col("status") != "cancelled") \
    .groupBy("customer_id") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("lifetime_value"),
        F.avg("total_amount").alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("product_id").alias("unique_products_ordered")
    )

# Compute churn risk
order_aggs = order_aggs.withColumn(
    "days_since_last_order",
    F.datediff(F.current_date(), F.col("last_order_date"))
).withColumn(
    "churn_risk",
    F.when(F.col("days_since_last_order") > 180, 0.90)
     .when(F.col("days_since_last_order") > 90, 0.60)
     .when(F.col("days_since_last_order") > 30, 0.25)
     .otherwise(0.05)
)

# Compute segment
order_aggs = order_aggs.withColumn(
    "segment",
    F.when(F.col("lifetime_value") > 5000, "platinum")
     .when(F.col("lifetime_value") > 1000, "gold")
     .when(F.col("lifetime_value") > 200, "silver")
     .otherwise("bronze")
)

# Join with customer details
customer_360 = customers_df \
    .join(order_aggs, "customer_id", "left") \
    .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0))) \
    .withColumn("lifetime_value", F.coalesce(F.col("lifetime_value"), F.lit(0.0))) \
    .withColumn("avg_order_value", F.coalesce(F.col("avg_order_value"), F.lit(0.0))) \
    .withColumn("churn_risk", F.coalesce(F.col("churn_risk"), F.lit(0.95))) \
    .withColumn("segment", F.coalesce(F.col("segment"), F.lit("bronze"))) \
    .withColumn("computed_at", F.current_timestamp()) \
    .withColumn("year", F.lit(year)) \
    .withColumn("month", F.lit(month)) \
    .withColumn("day", F.lit(day))

# Select final columns
customer_360 = customer_360.select(
    "customer_id", "name", "email", "city", "state", "tier",
    "signup_date", "total_orders", "lifetime_value", "avg_order_value",
    "first_order_date", "last_order_date", "days_since_last_order",
    "unique_products_ordered", "churn_risk", "segment",
    "computed_at", "year", "month", "day"
)

# ━━━ QUALITY GATE: Validate before writing ━━━
c360_count = customer_360.count()
null_pk = customer_360.filter(F.col("customer_id").isNull()).count()
negative_ltv = customer_360.filter(F.col("lifetime_value") < 0).count()

print(f"   📊 Quality check:")
print(f"      Rows: {c360_count:,}")
print(f"      NULL customer_id: {null_pk}")
print(f"      Negative LTV: {negative_ltv}")

if null_pk > 0 or negative_ltv > 0:
    print(f"   ❌ QUALITY GATE FAILED — not writing to Gold")
    raise ValueError(f"customer_360 quality gate failed: null_pk={null_pk}, neg_ltv={negative_ltv}")

# Write
c360_path = f"{gold_base}/customer_360/"
customer_360.coalesce(4).write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .parquet(c360_path)

print(f"   ✅ customer_360: {c360_count:,} rows → {c360_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GOLD TABLE 2: revenue_daily
# Daily revenue aggregation by date + segment
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🏆 Building Gold: revenue_daily")

# Join orders with customer segment
orders_with_segment = orders_df \
    .filter(F.col("status").isin("completed", "shipped")) \
    .join(
        customer_360.select("customer_id", "segment"),
        "customer_id",
        "left"
    )

revenue_daily = orders_with_segment \
    .groupBy("order_date", "segment") \
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("computed_at", F.current_timestamp()) \
    .withColumn("year", F.year("order_date").cast("string")) \
    .withColumn("month", F.format_string("%02d", F.month("order_date"))) \
    .withColumn("day", F.format_string("%02d", F.dayofmonth("order_date")))

rev_count = revenue_daily.count()

rev_path = f"{gold_base}/revenue_daily/"
revenue_daily.coalesce(4).write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .option("compression", "snappy") \
    .parquet(rev_path)

print(f"   ✅ revenue_daily: {rev_count:,} rows → {rev_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GOLD TABLE 3: product_performance
# Product-level metrics for inventory and marketing
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🏆 Building Gold: product_performance")

product_orders = orders_df \
    .filter(F.col("status") != "cancelled") \
    .groupBy("product_id") \
    .agg(
        F.count("order_id").alias("times_ordered"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_buyers"),
        F.avg("total_amount").alias("avg_order_value")
    )

product_performance = products_df \
    .join(product_orders, "product_id", "left") \
    .withColumn("times_ordered", F.coalesce(F.col("times_ordered"), F.lit(0))) \
    .withColumn("total_units_sold", F.coalesce(F.col("total_units_sold"), F.lit(0))) \
    .withColumn("total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0.0))) \
    .withColumn("unique_buyers", F.coalesce(F.col("unique_buyers"), F.lit(0))) \
    .withColumn("computed_at", F.current_timestamp()) \
    .withColumn("year", F.lit(year)) \
    .withColumn("month", F.lit(month)) \
    .withColumn("day", F.lit(day))

prod_count = product_performance.count()

prod_path = f"{gold_base}/product_performance/"
product_performance.coalesce(2).write \
    .mode("overwrite") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .parquet(prod_path)

print(f"   ✅ product_performance: {prod_count:,} rows → {prod_path}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FINAL SUMMARY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n{'='*60}")
print(f"📊 SILVER → GOLD SUMMARY ({process_date})")
print(f"{'='*60}")
print(f"   customer_360:        {c360_count:>10,} rows")
print(f"   revenue_daily:       {rev_count:>10,} rows")
print(f"   product_performance: {prod_count:>10,} rows")
print(f"{'='*60}")

job.commit()