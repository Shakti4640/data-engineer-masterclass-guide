# file: 43_03_glue_reverse_etl.py
# Purpose: Glue ETL script that reads Parquet from S3 and writes to MariaDB
# This script runs INSIDE Glue — uploaded to S3 as the job script

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# GLUE JOB SCRIPT: reverse_etl_to_mariadb.py
# Runs inside AWS Glue managed Spark environment
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_s3_path",
    "target_table",
    "load_strategy",     # "upsert" or "full_replace"
    "mariadb_url",       # jdbc:mysql://host:3306/quickcart
    "mariadb_user",
    "mariadb_password"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# READ: Parquet from S3 (UNLOAD output)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
source_path = args["source_s3_path"]
print(f"📥 Reading from: {source_path}")

source_df = spark.read.parquet(source_path)
record_count = source_df.count()
print(f"📊 Records read: {record_count}")

if record_count == 0:
    print("⚠️  No records to load — exiting")
    job.commit()
    sys.exit(0)

# Show sample
source_df.show(5, truncate=False)
source_df.printSchema()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# JDBC connection properties
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
jdbc_url = args["mariadb_url"]
jdbc_properties = {
    "user": args["mariadb_user"],
    "password": args["mariadb_password"],
    "driver": "org.mariadb.jdbc.Driver",
    "batchsize": "1000",
    "rewriteBatchedStatements": "true"
}

target_table = args["target_table"]
load_strategy = args["load_strategy"]

print(f"🎯 Target: {target_table} (strategy: {load_strategy})")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STRATEGY 1: FULL REPLACE (for customer_recommendations)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if load_strategy == "full_replace":
    """
    PATTERN: Write to _new table → Atomic RENAME swap
    
    1. TRUNCATE the _new table (clean slate)
    2. Write all data to _new table via JDBC
    3. Execute RENAME TABLE swap (atomic)
    4. DROP _old table
    
    App always sees complete, consistent data.
    """
    new_table = f"{target_table}_new"
    old_table = f"{target_table}_old"

    print(f"   Strategy: FULL REPLACE via table swap")
    print(f"   Writing to: {new_table}")

    # Repartition to control parallelism (don't overwhelm MariaDB)
    # 4 partitions = 4 concurrent JDBC connections
    write_df = source_df.repartition(4)

    # Truncate _new table first (via JDBC pre-action)
    # Spark JDBC "truncate" mode handles this
    write_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", new_table) \
        .option("user", args["mariadb_user"]) \
        .option("password", args["mariadb_password"]) \
        .option("driver", "org.mariadb.jdbc.Driver") \
        .option("batchsize", "5000") \
        .option("rewriteBatchedStatements", "true") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print(f"   ✅ {record_count} rows written to {new_table}")

    # Atomic table swap via direct JDBC call
    import java.sql as jdbc_module  # noqa: available in Glue Spark env

    # Use Spark's internal JDBC connection for DDL
    from py4j.java_gateway import java_import
    java_import(spark._jvm, "java.sql.DriverManager")

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        jdbc_url, args["mariadb_user"], args["mariadb_password"]
    )
    stmt = conn.createStatement()

    try:
        # Drop old table if exists from previous run
        stmt.execute(f"DROP TABLE IF EXISTS {old_table}")

        # Atomic rename: both renames in single statement
        rename_sql = (
            f"RENAME TABLE "
            f"{target_table} TO {old_table}, "
            f"{new_table} TO {target_table}"
        )
        print(f"   🔀 Executing: {rename_sql}")
        stmt.execute(rename_sql)
        print(f"   ✅ Table swap complete (atomic)")

        # Recreate _new for next run
        stmt.execute(f"CREATE TABLE {new_table} LIKE {target_table}")

        # Drop old table
        stmt.execute(f"DROP TABLE IF EXISTS {old_table}")
        print(f"   ✅ Old table dropped, new template created")

    except Exception as swap_err:
        print(f"   ❌ Table swap failed: {swap_err}")
        # Attempt recovery: if _new exists and target doesn't
        try:
            stmt.execute(f"RENAME TABLE {new_table} TO {target_table}")
            print(f"   ⚠️  Recovery: renamed {new_table} → {target_table}")
        except Exception:
            pass
        raise swap_err
    finally:
        stmt.close()
        conn.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# STRATEGY 2: UPSERT (for customer_metrics)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
elif load_strategy == "upsert":
    """
    PATTERN: INSERT ... ON DUPLICATE KEY UPDATE
    
    Spark JDBC doesn't natively support UPSERT.
    Solution: Use foreachPartition with pymysql for custom SQL.
    
    Each Spark partition:
    1. Opens pymysql connection to MariaDB
    2. Batches rows into INSERT ... ON DUPLICATE KEY UPDATE
    3. Commits every 5000 rows
    4. Closes connection
    """
    print(f"   Strategy: UPSERT via ON DUPLICATE KEY UPDATE")

    # Get column names from DataFrame schema
    columns = source_df.columns

    # Broadcast column info to executors
    columns_broadcast = sc.broadcast(columns)
    jdbc_url_broadcast = sc.broadcast(jdbc_url)
    user_broadcast = sc.broadcast(args["mariadb_user"])
    pass_broadcast = sc.broadcast(args["mariadb_password"])
    table_broadcast = sc.broadcast(target_table)

    # Repartition for controlled parallelism
    upsert_df = source_df.repartition(4)

    def upsert_partition(partition_rows):
        """
        Process one Spark partition: batch upsert to MariaDB.
        
        Runs on Spark EXECUTOR (not driver).
        Must import pymysql here (executor-side).
        """
        import pymysql
        from decimal import Decimal
        from datetime import datetime as dt

        rows = list(partition_rows)
        if not rows:
            return

        cols = columns_broadcast.value
        table = table_broadcast.value

        # Build upsert SQL template
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join(cols)
        update_clause = ", ".join(
            [f"{c} = VALUES({c})" for c in cols if c != cols[0]]
        )

        upsert_sql = (
            f"INSERT INTO {table} ({col_names}) "
            f"VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_clause}"
        )

        # Parse JDBC URL to pymysql format
        url = jdbc_url_broadcast.value
        url_parts = url.replace("jdbc:mysql://", "").split("/")
        host_port = url_parts[0].split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 3306
        database = url_parts[1].split("?")[0]

        conn = pymysql.connect(
            host=host,
            port=port,
            user=user_broadcast.value,
            password=pass_broadcast.value,
            database=database,
            autocommit=False
        )
        cursor = conn.cursor()

        batch_size = 5000
        batch = []
        total_upserted = 0

        for row in rows:
            # Convert Row to tuple, handling types
            values = []
            for val in row:
                if isinstance(val, Decimal):
                    values.append(float(val))
                elif isinstance(val, dt):
                    values.append(val.strftime("%Y-%m-%d %H:%M:%S"))
                elif val is None:
                    values.append(None)
                else:
                    values.append(val)
            batch.append(tuple(values))

            # Execute batch when full
            if len(batch) >= batch_size:
                cursor.executemany(upsert_sql, batch)
                conn.commit()
                total_upserted += len(batch)
                batch = []

        # Final batch (remaining rows)
        if batch:
            cursor.executemany(upsert_sql, batch)
            conn.commit()
            total_upserted += len(batch)

        cursor.close()
        conn.close()

        # Return count for logging (via accumulator if needed)
        print(f"   Partition upserted: {total_upserted} rows")

    # Execute across all partitions
    print(f"   🔀 Upserting {record_count} rows across 4 partitions...")
    upsert_df.rdd.foreachPartition(upsert_partition)
    print(f"   ✅ Upsert complete: {record_count} rows")

else:
    raise ValueError(f"Unknown load_strategy: {load_strategy}. Use 'upsert' or 'full_replace'")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# POST-LOAD VALIDATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
print(f"\n🔍 Post-load validation...")

# Read back from MariaDB to verify
verify_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"(SELECT COUNT(*) AS cnt FROM {target_table}) AS t") \
    .option("user", args["mariadb_user"]) \
    .option("password", args["mariadb_password"]) \
    .option("driver", "org.mariadb.jdbc.Driver") \
    .load()

mariadb_count = verify_df.collect()[0]["cnt"]
print(f"   Source (S3):   {record_count} rows")
print(f"   Target (MariaDB): {mariadb_count} rows")

if load_strategy == "full_replace":
    if mariadb_count == record_count:
        print(f"   ✅ MATCH — full replace verified")
    else:
        print(f"   ⚠️  MISMATCH — investigate! Expected {record_count}, got {mariadb_count}")
elif load_strategy == "upsert":
    if mariadb_count >= record_count:
        print(f"   ✅ OK — MariaDB has {mariadb_count} rows (>= source {record_count})")
        print(f"      (Historical rows + new upserts)")
    else:
        print(f"   ⚠️  MariaDB has FEWER rows than source — investigate!")

print(f"\n🎉 Reverse ETL complete for {target_table}")
job.commit()