# file: 05_verify_partitions.py
# Verify S3 folder structure and partition contents

import boto3

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"
PREFIX = "silver_partitioned/orders/"


def verify_partition_structure():
    """List all partition folders and their contents"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 60)
    print("📂 VERIFYING PARTITIONED S3 STRUCTURE")
    print(f"   Prefix: s3://{BUCKET_NAME}/{PREFIX}")
    print("=" * 60)

    # Use delimiter to get "folder" level listing
    paginator = s3_client.get_paginator("list_objects_v2")

    # Collect all objects
    partitions = {}
    total_size = 0
    total_files = 0

    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            key = obj["Key"]
            size = obj["Size"]

            # Extract partition value from path
            # key: silver_partitioned/orders/order_date=2025-01-15/part-00000.parquet
            parts = key.replace(PREFIX, "").split("/")
            if len(parts) >= 2 and parts[0].startswith("order_date="):
                partition_value = parts[0]  # order_date=2025-01-15
                if partition_value not in partitions:
                    partitions[partition_value] = {"files": 0, "size": 0}
                partitions[partition_value]["files"] += 1
                partitions[partition_value]["size"] += size
                total_size += size
                total_files += 1

    if not partitions:
        print("\n   ❌ No partition folders found!")
        print(f"   Check: did the Glue job write to {PREFIX}?")
        return

    # Sort by partition value
    sorted_partitions = sorted(partitions.items())

    print(f"\n   Found {len(sorted_partitions)} partitions, {total_files} files total\n")
    print(f"   {'Partition':<30} {'Files':>6} {'Size (MB)':>10}")
    print(f"   {'─'*30} {'─'*6} {'─'*10}")

    for partition, info in sorted_partitions:
        size_mb = round(info["size"] / (1024 * 1024), 2)
        print(f"   {partition:<30} {info['files']:>6} {size_mb:>10}")

    total_mb = round(total_size / (1024 * 1024), 2)
    print(f"   {'─'*30} {'─'*6} {'─'*10}")
    print(f"   {'TOTAL':<30} {total_files:>6} {total_mb:>10}")

    # Validate partition consistency
    print(f"\n📊 PARTITION HEALTH CHECK:")

    sizes = [info["size"] for info in partitions.values()]
    avg_size = sum(sizes) / len(sizes)
    min_size = min(sizes)
    max_size = max(sizes)
    
    min_mb = round(min_size / (1024 * 1024), 2)
    max_mb = round(max_size / (1024 * 1024), 2)
    avg_mb = round(avg_size / (1024 * 1024), 2)

    print(f"   Partition count:    {len(sorted_partitions)}")
    print(f"   Avg partition size: {avg_mb} MB")
    print(f"   Min partition size: {min_mb} MB")
    print(f"   Max partition size: {max_mb} MB")

    # Check for anomalies
    if max_size > avg_size * 3:
        print(f"   ⚠️  WARNING: largest partition is 3x+ bigger than average")
        print(f"      → Possible data skew on certain dates")
    else:
        print(f"   ✅ Partition sizes are balanced")

    # Check for empty partitions
    empty = [p for p, info in partitions.items() if info["size"] == 0]
    if empty:
        print(f"   ⚠️  WARNING: {len(empty)} empty partition(s) found")
        for e in empty:
            print(f"      → {e}")
    else:
        print(f"   ✅ No empty partitions")

    # Files per partition
    files_per_partition = [info["files"] for info in partitions.values()]
    max_files = max(files_per_partition)
    if max_files > 10:
        print(f"   ⚠️  WARNING: some partitions have {max_files} files")
        print(f"      → Consider coalescing (Project 62)")
    else:
        print(f"   ✅ File count per partition is reasonable (max: {max_files})")

    return sorted_partitions


def compare_unpartitioned_vs_partitioned():
    """Compare storage between unpartitioned and partitioned layouts"""
    s3_client = boto3.client("s3", region_name=REGION)

    def get_total_size(prefix):
        total = 0
        files = 0
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
            if "Contents" not in page:
                continue
            for obj in page["Contents"]:
                total += obj["Size"]
                files += 1
        return total, files

    unpart_size, unpart_files = get_total_size("silver/orders/")
    part_size, part_files = get_total_size("silver_partitioned/orders/")

    unpart_mb = round(unpart_size / (1024 * 1024), 2)
    part_mb = round(part_size / (1024 * 1024), 2)

    print(f"\n{'=' * 60}")
    print(f"💾 STORAGE COMPARISON: Unpartitioned vs Partitioned")
    print(f"{'=' * 60}")
    print(f"   {'Layout':<25} {'Files':>6} {'Size (MB)':>10}")
    print(f"   {'─'*25} {'─'*6} {'─'*10}")
    print(f"   {'Unpartitioned (silver/)':<25} {unpart_files:>6} {unpart_mb:>10}")
    print(f"   {'Partitioned (silver_p/)':<25} {part_files:>6} {part_mb:>10}")

    if unpart_size > 0:
        diff_pct = round((part_size / unpart_size - 1) * 100, 1)
        print(f"\n   Storage overhead from partitioning: {diff_pct:+.1f}%")
        print(f"   (Small overhead expected: more files = more Parquet footers)")
    
    print(f"\n   💡 Partitioning adds ~5-15% storage but saves 90%+ on queries")


if __name__ == "__main__":
    verify_partition_structure()
    compare_unpartitioned_vs_partitioned()