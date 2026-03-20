# file: 34_01_glue_silver_layer_transforms.py
# Purpose: Transform bronze layer → silver layer with full cleaning pipeline
# Execution: Runs INSIDE Glue service
# Upload to: s3://quickcart-datalake-prod/scripts/glue/silver_layer_transforms.py

import sys
import boto3
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, DateType, TimestampType
)

# ═══════════════════════════════════════════════════════════════════
# STEP 1: Initialize
# ═══════════════════════════════════════════════════════════════════
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_s3_path",          # s3://bucket/bronze/mariadb/
    "silver_s3_path",          # s3://bucket/silver/
    "extraction_date",         # YYYY-MM-DD
    "catalog_database"         # silver_quickcart
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

BRONZE_PATH = args["bronze_s3_path"]
SILVER_PATH = args["silver_s3_path"]
EXTRACTION_DATE = args["extraction_date"]
CATALOG_DB = args["catalog_database"]

ext_date = datetime.strptime(EXTRACTION_DATE, "%Y-%m-%d")
YEAR = ext_date.strftime("%Y")
MONTH = ext_date.strftime("%m")
DAY = ext_date.strftime("%d")

# ── SPARK CONFIGURATION FOR JOINS ──
# Increase broadcast threshold from 10MB to 200MB
# This ensures products table (< 200MB) uses broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 200 * 1024 * 1024)

logger.info("=" * 70)
logger.info("🚀 SILVER LAYER TRANSFORMATION")
logger.info(f"   Bronze: {BRONZE_PATH}")
logger.info(f"   Silver: {SILVER_PATH}")
logger.info(f"   Date: {EXTRACTION_DATE}")
logger.info("=" * 70)


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Read Bronze Tables
# ═══════════════════════════════════════════════════════════════════
def read_bronze_table(table_name):
    """
    Read today's partition from bronze layer
    Uses date-partition filtering (Project 33 — Approach 3)
    """
    path = (
        f"{BRONZE_PATH}{table_name}/"
        f"year={YEAR}/month={MONTH}/day={DAY}/"
    )
    logger.info(f"\n📖 Reading: {path}")

    try:
        df = spark.read.parquet(path)
        count = df.count()
        logger.info(f"   Rows: {count:,}")
        logger.info(f"   Columns: {df.columns}")
        return df, count
    except Exception as e:
        logger.error(f"   ❌ Failed to read {table_name}: {e}")
        return None, 0


# Read all three source tables
orders_raw, orders_count = read_bronze_table("orders")
customers_raw, customers_count = read_bronze_table("customers")
products_raw, products_count = read_bronze_table("products")

# Verify all tables loaded
if orders_raw is None or customers_raw is None or products_raw is None:
    raise Exception("Failed to read one or more bronze tables")

logger.info(f"\n📊 Bronze totals: orders={orders_count:,}, "
            f"customers={customers_count:,}, products={products_count:,}")


# ═══════════════════════════════════════════════════════════════════
# STEP 3: CLEAN — Orders Table
# ═══════════════════════════════════════════════════════════════════
def clean_orders(df):
    """
    Cleaning pipeline for orders table:
    1. Remove rows with NULL customer_id
    2. Remove rows with negative or zero total_amount
    3. Normalize status values (lowercase, trim)
    4. Cast types explicitly
    5. Add derived columns
    """
    logger.info(f"\n🧹 CLEANING: orders")
    initial_count = df.count()

    # ── FILTER: Remove NULL customer_id ──
    # Business rule: every order must have a customer
    null_customer_count = df.filter(F.col("customer_id").isNull()).count()
    df = df.filter(F.col("customer_id").isNotNull())
    logger.info(f"   Removed {null_customer_count:,} rows with NULL customer_id")

    # ── FILTER: Remove invalid amounts ──
    # Business rule: total_amount must be positive
    # Negative amounts are returns — handled separately
    invalid_amount_count = df.filter(
        (F.col("total_amount").isNull()) |
        (F.col("total_amount") <= 0)
    ).count()
    df = df.filter(
        (F.col("total_amount").isNotNull()) &
        (F.col("total_amount") > 0)
    )
    logger.info(f"   Removed {invalid_amount_count:,} rows with invalid amount")

    # ── NORMALIZE: Status values ──
    # Raw data has: "Completed", "COMPLETED", "completed ", " Completed"
    # Normalize to: "completed"
    df = df.withColumn(
        "status",
        F.lower(F.trim(F.col("status")))
    )

    # ── NORMALIZE: Map status variants to standard values ──
    status_mapping = {
        "complete": "completed",
        "done": "completed",
        "shipped": "shipped",
        "ship": "shipped",
        "pending": "pending",
        "cancel": "cancelled",
        "canceled": "cancelled",
        "cancelled": "cancelled",
        "refund": "refunded",
        "refunded": "refunded"
    }

    # Build CASE WHEN expression from mapping
    status_expr = F.col("status")
    for raw_val, clean_val in status_mapping.items():
        status_expr = F.when(
            F.col("status") == raw_val, F.lit(clean_val)
        ).otherwise(status_expr)

    df = df.withColumn("status", status_expr)

    # ── CAST: Explicit type enforcement ──
    df = df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
    df = df.withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    df = df.withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    df = df.withColumn("order_date", F.to_date(F.col("order_date")))

    # ── DERIVE: Calculated columns ──
    # Recalculate total to verify data integrity
    df = df.withColumn(
        "calculated_total",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    # Flag discrepancies
    df = df.withColumn(
        "amount_discrepancy",
        F.abs(F.col("total_amount") - F.col("calculated_total")) > 0.01
    )

    # Extract date parts for analysis
    df = df.withColumn("order_year", F.year(F.col("order_date")))
    df = df.withColumn("order_month", F.month(F.col("order_date")))
    df = df.withColumn("order_day_of_week", F.dayofweek(F.col("order_date")))

    final_count = df.count()
    removed = initial_count - final_count
    pct_removed = round(removed / max(initial_count, 1) * 100, 1)

    logger.info(f"   ✅ Clean complete: {initial_count:,} → {final_count:,} "
                f"({pct_removed}% removed)")

    return df


# ═══════════════════════════════════════════════════════════════════
# STEP 4: CLEAN — Customers Table
# ═══════════════════════════════════════════════════════════════════
def clean_customers(df):
    """
    Cleaning pipeline for customers table:
    1. Normalize email (lowercase)
    2. Normalize tier values
    3. Remove invalid records
    4. Standardize city names
    """
    logger.info(f"\n🧹 CLEANING: customers")
    initial_count = df.count()

    # ── FILTER: Remove NULL customer_id ──
    df = df.filter(F.col("customer_id").isNotNull())

    # ── NORMALIZE: Email to lowercase ──
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # ── NORMALIZE: Name — proper case ──
    df = df.withColumn("name", F.initcap(F.trim(F.col("name"))))

    # ── NORMALIZE: City — proper case ──
    df = df.withColumn("city", F.initcap(F.trim(F.col("city"))))

    # ── NORMALIZE: Tier — lowercase ──
    df = df.withColumn("tier", F.lower(F.trim(F.col("tier"))))

    # ── VALIDATE: Email format (basic check) ──
    df = df.withColumn(
        "email_valid",
        F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    )

    # ── CAST: signup_date ──
    df = df.withColumn("signup_date", F.to_date(F.col("signup_date")))

    # ── DERIVE: Customer tenure ──
    df = df.withColumn(
        "tenure_days",
        F.datediff(F.current_date(), F.col("signup_date"))
    )

    final_count = df.count()
    logger.info(f"   ✅ Clean complete: {initial_count:,} → {final_count:,}")

    return df


# ═══════════════════════════════════════════════════════════════════
# STEP 5: CLEAN — Products Table
# ═══════════════════════════════════════════════════════════════════
def clean_products(df):
    """
    Cleaning pipeline for products table:
    1. Normalize category names
    2. Cast types
    3. Derive price tiers
    """
    logger.info(f"\n🧹 CLEANING: products")
    initial_count = df.count()

    # ── FILTER: Active products with valid ID ──
    df = df.filter(F.col("product_id").isNotNull())

    # ── NORMALIZE: Category ──
    df = df.withColumn("category", F.initcap(F.trim(F.col("category"))))

    # ── NORMALIZE: Product name ──
    df = df.withColumn("name", F.trim(F.col("name")))

    # ── CAST ──
    df = df.withColumn("price", F.col("price").cast(DoubleType()))
    df = df.withColumn("stock_quantity", F.col("stock_quantity").cast(IntegerType()))

    # ── DERIVE: Price tier ──
    df = df.withColumn(
        "price_tier",
        F.when(F.col("price") < 25, "budget")
         .when(F.col("price") < 100, "mid_range")
         .when(F.col("price") < 500, "premium")
         .otherwise("luxury")
    )

    # ── DERIVE: Stock status ──
    df = df.withColumn(
        "stock_status",
        F.when(F.col("stock_quantity") == 0, "out_of_stock")
         .when(F.col("stock_quantity") < 10, "low_stock")
         .when(F.col("stock_quantity") < 100, "normal")
         .otherwise("well_stocked")
    )

    final_count = df.count()
    logger.info(f"   ✅ Clean complete: {initial_count:,} → {final_count:,}")

    return df


# ═══════════════════════════════════════════════════════════════════
# STEP 6: DEDUPLICATE
# ═══════════════════════════════════════════════════════════════════
def deduplicate(df, key_column, order_column=None, ascending=False):
    """
    Remove duplicate rows — keep the LATEST version
    
    Uses Window function + row_number:
    → Partition by key_column (group duplicates together)
    → Order by order_column (sort within group)
    → Keep row_number = 1 (first row = latest)
    
    WHY NOT dropDuplicates():
    → dropDuplicates keeps arbitrary row (non-deterministic)
    → Window + row_number keeps the LATEST (deterministic)
    → Essential for CDC data where same ID appears multiple times
    """
    logger.info(f"\n🔄 DEDUPLICATING on: {key_column}")
    initial_count = df.count()

    if order_column and order_column in df.columns:
        # Deterministic dedup: keep latest by order_column
        order_expr = F.col(order_column).asc() if ascending else F.col(order_column).desc()
        
        window = Window.partitionBy(key_column).orderBy(order_expr)
        
        df = df.withColumn("_row_number", F.row_number().over(window))
        df = df.filter(F.col("_row_number") == 1)
        df = df.drop("_row_number")
        
        logger.info(f"   Strategy: Window(partitionBy={key_column}, "
                     f"orderBy={order_column} desc) → keep row 1")
    else:
        # Simple dedup: keep any one row per key
        df = df.dropDuplicates([key_column])
        logger.info(f"   Strategy: dropDuplicates([{key_column}])")

    final_count = df.count()
    dupes_removed = initial_count - final_count
    pct_dupes = round(dupes_removed / max(initial_count, 1) * 100, 2)

    logger.info(f"   ✅ Dedup complete: {initial_count:,} → {final_count:,} "
                f"({dupes_removed:,} duplicates = {pct_dupes}%)")

    return df


# ═══════════════════════════════════════════════════════════════════
# STEP 7: Execute Clean + Dedup Pipeline
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("PHASE 1: CLEAN & DEDUPLICATE")
logger.info("=" * 70)

# Clean
orders_clean = clean_orders(orders_raw)
customers_clean = clean_customers(customers_raw)
products_clean = clean_products(products_raw)

# Deduplicate
orders_deduped = deduplicate(
    orders_clean, 
    key_column="order_id",
    order_column="_extraction_timestamp"   # Keep latest extraction
)

customers_deduped = deduplicate(
    customers_clean,
    key_column="customer_id",
    order_column="_extraction_timestamp"
)

products_deduped = deduplicate(
    products_clean,
    key_column="product_id",
    order_column="_extraction_timestamp"
)

# Cache for reuse in joins (each used in multiple operations)
orders_deduped.cache()
customers_deduped.cache()
products_deduped.cache()

logger.info(f"\n📊 After clean + dedup:")
logger.info(f"   Orders:    {orders_deduped.count():,}")
logger.info(f"   Customers: {customers_deduped.count():,}")
logger.info(f"   Products:  {products_deduped.count():,}")


# ═══════════════════════════════════════════════════════════════════
# STEP 8: JOIN — Create Enriched Order Details
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("PHASE 2: JOIN")
logger.info("=" * 70)

def create_order_details(orders_df, customers_df, products_df):
    """
    Join orders with products and customers to create enriched order details
    
    JOIN STRATEGY:
    → orders LEFT JOIN products ON product_id
      (LEFT: keep orders even if product not found)
    → result LEFT JOIN customers ON customer_id
      (LEFT: keep orders even if customer details missing)
    
    WHY LEFT JOIN (not INNER):
    → INNER join would drop orders with unknown product_id
    → Business: we want ALL revenue counted, even for unknown products
    → NULL product_name in result = "investigate data quality"
    
    BROADCAST OPTIMIZATION:
    → products: 50K rows → broadcast (sent to all executors)
    → customers: 2M rows → may broadcast if < 200MB threshold
    → orders: 43M rows → NEVER broadcast (too large)
    """
    logger.info(f"\n🔗 Joining: orders × products × customers")

    pre_count = orders_df.count()

    # ── RENAME COLUMNS TO AVOID AMBIGUITY ──
    # Both orders and products have "name" column
    # Rename before join to prevent confusion
    products_renamed = products_df.select(
        F.col("product_id"),
        F.col("name").alias("product_name"),
        F.col("category").alias("product_category"),
        F.col("price").alias("product_list_price"),
        F.col("price_tier"),
        F.col("stock_status")
    )

    customers_renamed = customers_df.select(
        F.col("customer_id"),
        F.col("name").alias("customer_name"),
        F.col("email").alias("customer_email"),
        F.col("city").alias("customer_city"),
        F.col("tier").alias("customer_tier"),
        F.col("tenure_days").alias("customer_tenure_days")
    )

    # ── JOIN 1: orders × products (broadcast products — small table) ──
    from pyspark.sql.functions import broadcast

    order_products = orders_df.join(
        broadcast(products_renamed),
        on="product_id",
        how="left"
    )

    logger.info(f"   orders × products: {order_products.count():,} rows")

    # ── JOIN 2: result × customers ──
    order_details = order_products.join(
        customers_renamed,
        on="customer_id",
        how="left"
    )

    post_count = order_details.count()
    logger.info(f"   result × customers: {post_count:,} rows")

    # ── VERIFY NO FAN-OUT ──
    if post_count > pre_count * 1.01:  # Allow 1% tolerance
        logger.warn(f"   ⚠️  FAN-OUT DETECTED! {pre_count:,} → {post_count:,}")
        logger.warn(f"   → Check for duplicate keys in dimension tables")
    else:
        logger.info(f"   ✅ No fan-out: {pre_count:,} → {post_count:,}")

    # ── DERIVE: Revenue metrics ──
    order_details = order_details.withColumn(
        "revenue",
        F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    # ── DERIVE: Discount indicator ──
    order_details = order_details.withColumn(
        "discount_pct",
        F.when(
            F.col("product_list_price").isNotNull() & (F.col("product_list_price") > 0),
            F.round(
                (1 - F.col("unit_price") / F.col("product_list_price")) * 100, 1
            )
        ).otherwise(F.lit(0.0))
    )

    logger.info(f"   ✅ Order details created: {post_count:,} rows, "
                f"{len(order_details.columns)} columns")

    return order_details


order_details = create_order_details(
    orders_deduped, customers_deduped, products_deduped
)


# ═══════════════════════════════════════════════════════════════════
# STEP 9: AGGREGATE — Pre-Compute Summary Tables
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("PHASE 3: AGGREGATE")
logger.info("=" * 70)

def create_daily_revenue_summary(order_details_df):
    """
    Daily revenue summary by product category and customer tier
    
    OUTPUT: ~200 rows/day (8 categories × 4 tiers × ~6 statuses)
    vs INPUT: ~200K rows/day
    → Dashboard reads 200 rows instead of 200K → 1000x faster
    """
    logger.info(f"\n📊 Creating daily revenue summary...")

    summary = order_details_df.groupBy(
        "order_date",
        "product_category",
        "customer_tier",
        "status"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_order_value"),
        F.min("revenue").alias("min_order_value"),
        F.max("revenue").alias("max_order_value"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products"),
        F.avg("discount_pct").alias("avg_discount_pct")
    )

    # Round monetary values
    summary = summary.withColumn("total_revenue", F.round("total_revenue", 2))
    summary = summary.withColumn("avg_order_value", F.round("avg_order_value", 2))
    summary = summary.withColumn("avg_discount_pct", F.round("avg_discount_pct", 1))

    # Add metadata
    summary = summary.withColumn("_aggregation_date", F.lit(EXTRACTION_DATE))
    summary = summary.withColumn("_aggregation_level", F.lit("daily_category_tier"))

    row_count = summary.count()
    logger.info(f"   ✅ Daily summary: {row_count:,} rows")

    return summary


def create_monthly_category_summary(order_details_df):
    """
    Monthly revenue summary by category
    Used for: trend charts, month-over-month comparison
    """
    logger.info(f"\n📊 Creating monthly category summary...")

    summary = order_details_df.groupBy(
        "order_year",
        "order_month",
        "product_category"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_order_value"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("customer_id").alias("unique_customers")
    )

    summary = summary.withColumn("total_revenue", F.round("total_revenue", 2))
    summary = summary.withColumn("avg_order_value", F.round("avg_order_value", 2))

    # Add metadata
    summary = summary.withColumn("_aggregation_date", F.lit(EXTRACTION_DATE))
    summary = summary.withColumn("_aggregation_level", F.lit("monthly_category"))

    row_count = summary.count()
    logger.info(f"   ✅ Monthly summary: {row_count:,} rows")

    return summary


def create_customer_rfm(order_details_df):
    """
    Customer RFM (Recency, Frequency, Monetary) analysis
    
    Used for: customer segmentation, marketing targeting
    → Recency: days since last order
    → Frequency: total number of orders
    → Monetary: total revenue from this customer
    """
    logger.info(f"\n📊 Creating customer RFM analysis...")

    rfm = order_details_df.filter(
        F.col("status") == "completed"
    ).groupBy(
        "customer_id",
        "customer_name",
        "customer_email",
        "customer_city",
        "customer_tier"
    ).agg(
        F.datediff(
            F.current_date(), F.max("order_date")
        ).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("revenue"), 2).alias("monetary"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.avg("revenue").alias("avg_order_value")
    )

    rfm = rfm.withColumn("avg_order_value", F.round("avg_order_value", 2))

    # RFM scoring (simple quartile-based)
    rfm = rfm.withColumn(
        "rfm_segment",
        F.when(
            (F.col("recency_days") < 30) &
            (F.col("frequency") >= 5) &
            (F.col("monetary") >= 1000),
            "champion"
        ).when(
            (F.col("recency_days") < 60) &
            (F.col("frequency") >= 3),
            "loyal"
        ).when(
            (F.col("recency_days") < 90),
            "recent"
        ).when(
            (F.col("frequency") >= 3),
            "regular"
        ).when(
            (F.col("recency_days") > 180),
            "at_risk"
        ).otherwise("occasional")
    )

    rfm = rfm.withColumn("_aggregation_date", F.lit(EXTRACTION_DATE))

    row_count = rfm.count()
    logger.info(f"   ✅ Customer RFM: {row_count:,} rows")

    return rfm


# Execute aggregations
daily_summary = create_daily_revenue_summary(order_details)
monthly_summary = create_monthly_category_summary(order_details)
customer_rfm = create_customer_rfm(order_details)


# ═══════════════════════════════════════════════════════════════════
# STEP 10: WRITE — Silver Layer Output
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("PHASE 4: WRITE SILVER LAYER")
logger.info("=" * 70)

def write_silver_table(df, table_name, partition_cols, coalesce_num=None):
    """
    Write DataFrame to silver layer as partitioned Parquet
    
    WRITE MODE: overwrite
    → With partitionBy: overwrites ONLY the specified partitions
    → Previous dates' data is PRESERVED
    → Today's partition is replaced (idempotent for re-runs)
    """
    output_path = f"{SILVER_PATH}{table_name}/"

    logger.info(f"\n💾 Writing: {table_name}")
    logger.info(f"   Path: {output_path}")
    logger.info(f"   Partitions: {partition_cols}")

    # Add date partition columns if not present
    if "year" not in df.columns:
        df = df.withColumn("year", F.lit(YEAR))
    if "month" not in df.columns:
        df = df.withColumn("month", F.lit(MONTH))
    if "day" not in df.columns:
        df = df.withColumn("day", F.lit(DAY))

    # Coalesce to control file count
    if coalesce_num:
        df = df.coalesce(coalesce_num)

    # Write
    df.write \
        .mode("overwrite") \
        .partitionBy(partition_cols) \
        .option("compression", "snappy") \
        .parquet(output_path)

    logger.info(f"   ✅ Written: {table_name}")

    return output_path


# ── Write all silver tables ──
write_silver_table(
    orders_deduped, "clean_orders",
    ["year", "month", "day"], coalesce_num=10
)

write_silver_table(
    customers_deduped, "clean_customers",
    ["year", "month", "day"], coalesce_num=2
)

write_silver_table(
    products_deduped, "clean_products",
    ["year", "month", "day"], coalesce_num=1
)

write_silver_table(
    order_details, "order_details",
    ["year", "month", "day"], coalesce_num=10
)

write_silver_table(
    daily_summary, "daily_revenue_summary",
    ["year", "month", "day"], coalesce_num=1
)

write_silver_table(
    monthly_summary, "monthly_category_summary",
    ["year", "month"], coalesce_num=1
)

write_silver_table(
    customer_rfm, "customer_rfm",
    ["year", "month", "day"], coalesce_num=2
)


# ═══════════════════════════════════════════════════════════════════
# STEP 11: Release Cached DataFrames
# ═══════════════════════════════════════════════════════════════════
orders_deduped.unpersist()
customers_deduped.unpersist()
products_deduped.unpersist()
logger.info("\n♻️  Cached DataFrames released")


# ═══════════════════════════════════════════════════════════════════
# STEP 12: Register in Glue Catalog
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("PHASE 5: CATALOG REGISTRATION")
logger.info("=" * 70)

glue_client = boto3.client("glue", region_name="us-east-2")

# Create catalog database
try:
    glue_client.create_database(
        DatabaseInput={
            "Name": CATALOG_DB,
            "Description": "Silver layer — cleaned, joined, aggregated data"
        }
    )
    logger.info(f"   Created catalog database: {CATALOG_DB}")
except Exception:
    logger.info(f"   Catalog database exists: {CATALOG_DB}")

# Register tables using Spark saveAsTable
silver_tables = [
    "clean_orders", "clean_customers", "clean_products",
    "order_details", "daily_revenue_summary",
    "monthly_category_summary", "customer_rfm"
]

for table_name in silver_tables:
    table_path = f"{SILVER_PATH}{table_name}/"
    try:
        # Read back to get schema, then register
        df_check = spark.read.parquet(table_path)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG_DB}")
        df_check.write \
            .mode("overwrite") \
            .option("path", table_path) \
            .saveAsTable(f"{CATALOG_DB}.{table_name}")
        logger.info(f"   ✅ Registered: {CATALOG_DB}.{table_name}")
    except Exception as e:
        logger.warn(f"   ⚠️  Catalog registration failed for {table_name}: {e}")
        logger.info(f"   → Run Glue Crawler as fallback (Project 23)")


# ═══════════════════════════════════════════════════════════════════
# STEP 13: Final Summary
# ═══════════════════════════════════════════════════════════════════
logger.info("\n" + "=" * 70)
logger.info("📊 SILVER LAYER TRANSFORMATION COMPLETE")
logger.info("=" * 70)
logger.info(f"""
   INPUTS (Bronze):
   → orders:    {orders_count:,} raw rows
   → customers: {customers_count:,} raw rows
   → products:  {products_count:,} raw rows

   OUTPUTS (Silver):
   → clean_orders:              {orders_deduped.count():,} rows (cleaned + deduped)
   → clean_customers:           {customers_deduped.count():,} rows
   → clean_products:            {products_deduped.count():,} rows
   → order_details:             {order_details.count():,} rows (enriched with joins)
   → daily_revenue_summary:     {daily_summary.count():,} rows (pre-aggregated)
   → monthly_category_summary:  {monthly_summary.count():,} rows
   → customer_rfm:              {customer_rfm.count():,} rows

   CATALOG: {CATALOG_DB}.*
   DATE: {EXTRACTION_DATE}
""")

logger.info("=" * 70)

# ═══════════════════════════════════════════════════════════════════
# STEP 14: Commit Job
# ═══════════════════════════════════════════════════════════════════
job.commit()
logger.info("📌 Job committed successfully")