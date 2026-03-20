# file: 06_validate_conversion.py
# Comprehensive validation that Parquet output matches CSV source
# Run after Glue jobs complete to ensure data integrity

import boto3
import time

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


class ConversionValidator:
    """Validates CSV → Parquet conversion by comparing both layers"""

    def __init__(self, workgroup="data_engineering"):
        self.athena = boto3.client("athena", region_name=REGION)
        self.s3 = boto3.client("s3", region_name=REGION)
        self.workgroup = workgroup
        self.database = DATABASE_NAME

    def run_query(self, sql, description=""):
        """Execute Athena query and return results"""
        if description:
            print(f"   ⏳ {description}...")

        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            WorkGroup=self.workgroup
        )
        query_id = response["QueryExecutionId"]

        # Poll for completion
        while True:
            status = self.athena.get_query_execution(QueryExecutionId=query_id)
            state = status["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                bytes_scanned = status["QueryExecution"]["Statistics"].get(
                    "DataScannedInBytes", 0
                )
                mb_scanned = round(bytes_scanned / (1024 * 1024), 3)

                results = self.athena.get_query_results(QueryExecutionId=query_id)
                rows = results["ResultSet"]["Rows"]
                
                # Parse header and data
                if len(rows) > 1:
                    headers = [col.get("VarCharValue", "") for col in rows[0]["Data"]]
                    data = []
                    for row in rows[1:]:
                        values = [col.get("VarCharValue", None) for col in row["Data"]]
                        data.append(dict(zip(headers, values)))
                    return data, mb_scanned
                return [], mb_scanned

            elif state in ["FAILED", "CANCELLED"]:
                error = status["QueryExecution"]["Status"].get(
                    "StateChangeReason", "Unknown"
                )
                print(f"   ❌ Query failed: {error}")
                return None, 0

            time.sleep(2)

    def validate_table_pair(self, csv_table, parquet_table):
        """Compare CSV table with its Parquet counterpart"""
        print(f"\n{'─' * 60}")
        print(f"🔍 VALIDATING: {csv_table} → {parquet_table}")
        print(f"{'─' * 60}")

        # --- CHECK 1: Row counts ---
        csv_data, csv_mb = self.run_query(
            f"SELECT COUNT(*) as cnt FROM {csv_table}",
            "Counting CSV rows"
        )
        parquet_data, parquet_mb = self.run_query(
            f"SELECT COUNT(*) as cnt FROM {parquet_table}",
            "Counting Parquet rows"
        )

        if csv_data is None or parquet_data is None:
            print(f"   ❌ Cannot compare — query failed")
            return False

        csv_count = int(csv_data[0]["cnt"])
        parquet_count = int(parquet_data[0]["cnt"])

        match = csv_count == parquet_count
        status = "✅ MATCH" if match else "❌ MISMATCH"
        print(f"\n   📊 Row Count: {status}")
        print(f"      CSV:     {csv_count:,}")
        print(f"      Parquet: {parquet_count:,}")
        if not match:
            diff = csv_count - parquet_count
            print(f"      Missing: {diff:,} rows ({round(100*diff/max(csv_count,1), 2)}%)")

        # --- CHECK 2: Data scan comparison ---
        print(f"\n   💰 Data Scan Comparison:")
        print(f"      CSV scan:     {csv_mb} MB")
        print(f"      Parquet scan: {parquet_mb} MB")
        if csv_mb > 0:
            savings = round((1 - parquet_mb / csv_mb) * 100, 1)
            print(f"      Savings:      {savings}% less data scanned")

        # --- CHECK 3: Aggregate comparison ---
        # Only for orders table (has numeric columns)
        if "orders" in csv_table:
            csv_agg, _ = self.run_query(
                f"""SELECT 
                    ROUND(SUM(CAST(total_amount AS DOUBLE)), 2) as total_rev,
                    ROUND(AVG(CAST(total_amount AS DOUBLE)), 2) as avg_rev
                FROM {csv_table}""",
                "CSV aggregate check"
            )
            parquet_agg, _ = self.run_query(
                f"""SELECT 
                    ROUND(SUM(total_amount), 2) as total_rev,
                    ROUND(AVG(total_amount), 2) as avg_rev
                FROM {parquet_table}""",
                "Parquet aggregate check"
            )

            if csv_agg and parquet_agg:
                csv_rev = csv_agg[0]["total_rev"]
                pq_rev = parquet_agg[0]["total_rev"]
                rev_match = csv_rev == pq_rev

                status = "✅ MATCH" if rev_match else "⚠️  CLOSE"
                print(f"\n   📊 Revenue Check: {status}")
                print(f"      CSV total:     ${csv_rev}")
                print(f"      Parquet total: ${pq_rev}")

        # --- CHECK 4: Schema comparison ---
        csv_schema, _ = self.run_query(
            f"DESCRIBE {csv_table}",
            "CSV schema check"
        )
        parquet_schema, _ = self.run_query(
            f"DESCRIBE {parquet_table}",
            "Parquet schema check"
        )

        if csv_schema and parquet_schema:
            print(f"\n   📋 Schema Comparison:")
            print(f"      {'Column':<20} {'CSV Type':<15} {'Parquet Type':<15} {'Status'}")
            print(f"      {'─'*20} {'─'*15} {'─'*15} {'─'*10}")

            csv_cols = {r.get("col_name", ""): r.get("data_type", "") for r in csv_schema}
            pq_cols = {r.get("col_name", ""): r.get("data_type", "") for r in parquet_schema}

            for col_name in csv_cols:
                csv_type = csv_cols.get(col_name, "N/A")
                pq_type = pq_cols.get(col_name, "N/A")

                # Type improvement check
                if csv_type == "string" and pq_type in ("int", "double", "date", "bigint"):
                    improvement = "⬆️ IMPROVED"
                elif csv_type == pq_type:
                    improvement = "✅ Same"
                else:
                    improvement = "🔄 Changed"

                print(f"      {col_name:<20} {csv_type:<15} {pq_type:<15} {improvement}")

        # --- CHECK 5: Sample data spot check ---
        print(f"\n   📋 Sample data (first 3 rows from Parquet):")
        sample, _ = self.run_query(
            f"SELECT * FROM {parquet_table} LIMIT 3",
            "Sampling Parquet"
        )
        if sample:
            for i, row in enumerate(sample):
                print(f"      Row {i+1}: {row}")

        return match

    def validate_all(self):
        """Validate all table pairs"""
        print("=" * 60)
        print("🔍 CONVERSION VALIDATION REPORT")
        print("=" * 60)

        pairs = [
            ("raw_orders", "silver_orders"),
            ("raw_customers", "silver_customers"),
            ("raw_products", "silver_products")
        ]

        results = {}
        for csv_table, parquet_table in pairs:
            success = self.validate_table_pair(csv_table, parquet_table)
            results[f"{csv_table} → {parquet_table}"] = success

        # Summary
        print(f"\n{'=' * 60}")
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)

        all_passed = True
        for pair, passed in results.items():
            status = "✅ PASSED" if passed else "❌ FAILED"
            print(f"   {status} — {pair}")
            if not passed:
                all_passed = False

        if all_passed:
            print(f"\n   🎉 All conversions validated successfully!")
            print(f"   Analysts can now query silver_* tables for optimal cost")
        else:
            print(f"\n   ⚠️  Some validations failed — investigate before switching analysts")

        return all_passed


if __name__ == "__main__":
    validator = ConversionValidator()
    validator.validate_all()