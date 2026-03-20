# file: 62_01_smart_write.py
# Purpose: Add to ANY Glue ETL job — prevents small files at source
# This replaces the naive write in all previous projects
# Integration: add these functions to Project 61's Glue ETL script

import math

# --- TARGET FILE SIZE ---
TARGET_FILE_SIZE_MB = 128  # Optimal for Athena + Spectrum


def estimate_dataframe_size_mb(df):
    """
    Estimate DataFrame size in MB
    
    WHY ESTIMATION (not exact):
    → Spark doesn't track exact byte size of DataFrames in memory
    → df.storageLevel doesn't give serialized size
    → We use: row count × avg row size (sampled)
    → Or: Spark's execution plan statistics if available
    
    METHOD: Sample 1% of rows, measure, extrapolate
    → Fast even for millions of rows
    → Accurate within ~20% (good enough for file sizing)
    """
    total_rows = df.count()

    if total_rows == 0:
        return 0.0

    # Sample up to 10,000 rows for size estimation
    sample_fraction = min(10000 / total_rows, 1.0)
    sample_df = df.sample(fraction=sample_fraction, seed=42)

    # Convert sample to Pandas for byte measurement
    # (only the sample — not the full DF)
    try:
        pandas_sample = sample_df.toPandas()
        sample_bytes = pandas_sample.memory_usage(deep=True).sum()
        sample_rows = len(pandas_sample)

        if sample_rows == 0:
            return 0.0

        avg_row_bytes = sample_bytes / sample_rows

        # Parquet compression ratio: typically 3x-10x
        # Conservative estimate: 5x compression
        COMPRESSION_RATIO = 5.0

        estimated_raw_bytes = total_rows * avg_row_bytes
        estimated_parquet_bytes = estimated_raw_bytes / COMPRESSION_RATIO
        estimated_mb = estimated_parquet_bytes / (1024 * 1024)

        return estimated_mb

    except Exception:
        # Fallback: rough estimate based on row count
        # Assume 500 bytes per row after Parquet compression
        return (total_rows * 500) / (1024 * 1024)


def calculate_optimal_file_count(df, target_mb=TARGET_FILE_SIZE_MB):
    """
    Calculate how many output files to create
    
    LOGIC:
    → estimated_size / target_file_size = number of files
    → Minimum: 1 file (never 0)
    → Maximum: current partition count (don't INCREASE partitions)
    → Round up to ensure files don't exceed target
    """
    estimated_mb = estimate_dataframe_size_mb(df)
    current_partitions = df.rdd.getNumPartitions()

    if estimated_mb <= 0:
        return 1

    optimal_count = max(1, math.ceil(estimated_mb / target_mb))

    # Don't increase beyond current partitions (coalesce can't increase)
    optimal_count = min(optimal_count, current_partitions)

    print(f"📐 File sizing calculation:")
    print(f"   Estimated data size: {estimated_mb:.1f} MB")
    print(f"   Target file size: {target_mb} MB")
    print(f"   Current partitions: {current_partitions}")
    print(f"   Optimal file count: {optimal_count}")

    return optimal_count


def smart_write_parquet(df, glueContext, output_path, partition_cols=None):
    """
    Write DataFrame to S3 Parquet with OPTIMAL file count
    
    REPLACES: 
    → dynamic_frame.write(...) — which uses raw partition count
    → df.write.parquet(...) — which uses raw partition count
    
    THIS VERSION:
    → Calculates optimal file count dynamically
    → coalesces before write
    → Logs file sizing decision
    → Works for any data volume
    """
    optimal_count = calculate_optimal_file_count(df)

    # --- COALESCE ---
    df_coalesced = df.coalesce(optimal_count)

    print(f"📝 Writing {optimal_count} Parquet file(s) to: {output_path}")

    # --- WRITE ---
    writer = df_coalesced.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)

    print(f"✅ Write complete: {optimal_count} file(s)")

    return optimal_count


def smart_write_dynamic_frame(dynamic_frame, glueContext, output_path,
                               format_type="parquet", partition_keys=None):
    """
    DynamicFrame-native version of smart write
    
    For Glue jobs that stay in DynamicFrame API
    → Convert to DF for coalesce → convert back
    → Or use Glue's native repartition
    """
    # Convert to DataFrame for coalesce control
    df = dynamic_frame.toDF()

    optimal_count = calculate_optimal_file_count(df)
    df_coalesced = df.coalesce(optimal_count)

    # Convert back to DynamicFrame
    from awsglue.dynamicframe import DynamicFrame
    coalesced_dyf = DynamicFrame.fromDF(
        df_coalesced, glueContext, "coalesced_output"
    )

    # Write using Glue API
    additional_options = {}
    if partition_keys:
        additional_options["partitionKeys"] = partition_keys

    glueContext.write_dynamic_frame.from_options(
        frame=coalesced_dyf,
        connection_type="s3",
        connection_options={
            "path": output_path
        },
        format=format_type,
        format_options={"compression": "snappy"},
        transformation_ctx="smart_write"
    )

    print(f"✅ Smart write complete: {optimal_count} file(s) to {output_path}")
    return optimal_count