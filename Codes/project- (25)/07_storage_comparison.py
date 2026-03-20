# file: 07_storage_comparison.py
# Compare S3 storage usage between CSV (bronze) and Parquet (silver)
# Proves the storage savings from format conversion

import boto3

REGION = "us-east-2"
BUCKET_NAME = "quickcart-raw-data-prod"


def get_prefix_size(s3_client, bucket, prefix):
    """Calculate total size and file count for an S3 prefix"""
    paginator = s3_client.get_paginator("list_objects_v2")
    total_size = 0
    file_count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if "Contents" not in page:
            continue
        for obj in page["Contents"]:
            total_size += obj["Size"]
            file_count += 1

    return total_size, file_count


def compare_storage():
    """Compare CSV vs Parquet storage for all tables"""
    s3_client = boto3.client("s3", region_name=REGION)

    print("=" * 70)
    print("💾 STORAGE COMPARISON: CSV (Bronze) vs PARQUET (Silver)")
    print("=" * 70)

    comparisons = [
        {"name": "Orders",    "csv": "daily_exports/orders/",    "parquet": "silver/orders/"},
        {"name": "Customers", "csv": "daily_exports/customers/", "parquet": "silver/customers/"},
        {"name": "Products",  "csv": "daily_exports/products/",  "parquet": "silver/products/"}
    ]

    total_csv = 0
    total_parquet = 0

    print(f"\n   {'Table':<15} {'CSV Size':>12} {'CSV Files':>10} {'Parquet':>12} {'PQ Files':>10} {'Savings':>10}")
    print(f"   {'─'*15} {'─'*12} {'─'*10} {'─'*12} {'─'*10} {'─'*10}")

    for comp in comparisons:
        csv_size, csv_files = get_prefix_size(s3_client, BUCKET_NAME, comp["csv"])
        pq_size, pq_files = get_prefix_size(s3_client, BUCKET_NAME, comp["parquet"])

        csv_mb = round(csv_size / (1024 * 1024), 1)
        pq_mb = round(pq_size / (1024 * 1024), 1)
        savings = round((1 - pq_size / max(csv_size, 1)) * 100, 1)

        total_csv += csv_size
        total_parquet += pq_size

        print(f"   {comp['name']:<15} {csv_mb:>10.1f} MB {csv_files:>9} {pq_mb:>10.1f} MB {pq_files:>9} {savings:>9.1f}%")

    total_csv_mb = round(total_csv / (1024 * 1024), 1)
    total_pq_mb = round(total_parquet / (1024 * 1024), 1)
    total_savings = round((1 - total_parquet / max(total_csv, 1)) * 100, 1)

    print(f"   {'─'*15} {'─'*12} {'─'*10} {'─'*12} {'─'*10} {'─'*10}")
    print(f"   {'TOTAL':<15} {total_csv_mb:>10.1f} MB {'':>10} {total_pq_mb:>10.1f} MB {'':>10} {total_savings:>9.1f}%")

    # Cost projection
    csv_storage_cost = total_csv * 0.023 / (1024 ** 3)
    pq_storage_cost = total_parquet * 0.023 / (1024 ** 3)

    print(f"\n   💰 Monthly Storage Cost (S3 Standard):")
    print(f"      CSV:     ${csv_storage_cost:.4f}/month")
    print(f"      Parquet: ${pq_storage_cost:.4f}/month")
    print(f"      Savings: ${csv_storage_cost - pq_storage_cost:.4f}/month")

    # Query cost projection
    queries_per_month = 9000  # 10 analysts × 30 queries/day × 30 days
    csv_query_cost = queries_per_month * (total_csv / (1024 ** 4)) * 5.0
    pq_query_cost = queries_per_month * (total_parquet / (1024 ** 4)) * 5.0 * 0.33  # ~33% column read

    print(f"\n   💰 Monthly Athena Query Cost ({queries_per_month:,} queries):")
    print(f"      CSV tables:     ${csv_query_cost:.2f}/month")
    print(f"      Parquet tables: ${pq_query_cost:.2f}/month")
    print(f"      Savings:        ${csv_query_cost - pq_query_cost:.2f}/month")

    print("=" * 70)


if __name__ == "__main__":
    compare_storage()