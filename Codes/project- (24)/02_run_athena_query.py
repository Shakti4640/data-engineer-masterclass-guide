# file: 02_run_athena_query.py
# Programmatic Athena query execution with cost tracking
# Used by: scripts, Lambda functions, automated reports

import boto3
import time
import csv
import io

REGION = "us-east-2"
DATABASE_NAME = "quickcart_datalake"
BUCKET_NAME = "quickcart-raw-data-prod"


class AthenaQueryRunner:
    """
    Reusable Athena query executor with:
    - Workgroup-based execution
    - Automatic polling for completion
    - Result parsing
    - Cost tracking
    """

    def __init__(self, workgroup="data_engineering"):
        self.athena_client = boto3.client("athena", region_name=REGION)
        self.s3_client = boto3.client("s3", region_name=REGION)
        self.workgroup = workgroup
        self.database = DATABASE_NAME

    def execute_query(self, sql, timeout_seconds=300):
        """
        Submit query, wait for completion, return results
        
        ATHENA QUERY LIFECYCLE:
        1. QUEUED → query submitted, waiting for resources
        2. RUNNING → actively scanning S3 and processing
        3. SUCCEEDED → results available in S3
        4. FAILED → error occurred
        5. CANCELLED → user or scan limit cancelled the query
        """
        print(f"\n📝 Query: {sql[:100]}{'...' if len(sql) > 100 else ''}")
        print(f"   Workgroup: {self.workgroup}")

        # --- SUBMIT QUERY ---
        response = self.athena_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                "Database": self.database
            },
            WorkGroup=self.workgroup
        )

        query_id = response["QueryExecutionId"]
        print(f"   Query ID: {query_id}")

        # --- POLL FOR COMPLETION ---
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                print(f"   ❌ Timeout after {timeout_seconds}s")
                self.athena_client.stop_query_execution(QueryExecutionId=query_id)
                return None

            status_response = self.athena_client.get_query_execution(
                QueryExecutionId=query_id
            )
            status = status_response["QueryExecution"]["Status"]
            state = status["State"]

            if state == "SUCCEEDED":
                # --- GET EXECUTION STATS ---
                stats = status_response["QueryExecution"]["Statistics"]
                bytes_scanned = stats.get("DataScannedInBytes", 0)
                exec_time_ms = stats.get("EngineExecutionTimeInMillis", 0)
                total_time_ms = stats.get("TotalExecutionTimeInMillis", 0)

                mb_scanned = round(bytes_scanned / (1024 * 1024), 3)
                cost = round((bytes_scanned / (1024 ** 4)) * 5.0, 6)

                print(f"   ✅ SUCCEEDED in {total_time_ms}ms")
                print(f"   📊 Data scanned: {mb_scanned} MB")
                print(f"   💰 Estimated cost: ${cost:.6f}")

                # --- FETCH RESULTS ---
                results = self._fetch_results(query_id)
                return {
                    "query_id": query_id,
                    "state": state,
                    "rows": results,
                    "bytes_scanned": bytes_scanned,
                    "mb_scanned": mb_scanned,
                    "cost_usd": cost,
                    "execution_time_ms": exec_time_ms,
                    "total_time_ms": total_time_ms
                }

            elif state == "FAILED":
                reason = status.get("StateChangeReason", "Unknown")
                print(f"   ❌ FAILED: {reason}")
                return {"query_id": query_id, "state": state, "error": reason}

            elif state == "CANCELLED":
                reason = status.get("StateChangeReason", "Unknown")
                print(f"   ⚠️  CANCELLED: {reason}")
                return {"query_id": query_id, "state": state, "error": reason}

            else:
                # Still running
                time.sleep(1)

    def _fetch_results(self, query_id):
        """
        Fetch query results via paginated API
        
        NOTE: Athena results are also saved as CSV in S3
        → For large results: read from S3 directly (more efficient)
        → For small results: API pagination works fine
        """
        rows = []
        headers = None
        next_token = None

        while True:
            kwargs = {"QueryExecutionId": query_id, "MaxResults": 1000}
            if next_token:
                kwargs["NextToken"] = next_token

            response = self.athena_client.get_query_results(**kwargs)

            # First row of first page is the header
            result_rows = response["ResultSet"]["Rows"]

            for i, row in enumerate(result_rows):
                values = [col.get("VarCharValue", None) for col in row["Data"]]

                if headers is None and i == 0:
                    headers = values
                    continue

                rows.append(dict(zip(headers, values)))

            next_token = response.get("NextToken")
            if not next_token:
                break

        return rows

    def explain_query(self, sql):
        """Run EXPLAIN to preview query plan without cost"""
        return self.execute_query(f"EXPLAIN {sql}")

    def get_query_cost_history(self, days=7):
        """Get recent query history with costs for this workgroup"""
        response = self.athena_client.list_query_executions(
            WorkGroup=self.workgroup,
            MaxResults=50
        )

        query_ids = response.get("QueryExecutionIds", [])
        if not query_ids:
            print("No recent queries found")
            return []

        # Batch get execution details
        batch_response = self.athena_client.batch_get_query_execution(
            QueryExecutionIds=query_ids[:50]
        )

        history = []
        total_bytes = 0
        total_cost = 0

        for execution in batch_response["QueryExecutions"]:
            state = execution["Status"]["State"]
            stats = execution.get("Statistics", {})
            bytes_scanned = stats.get("DataScannedInBytes", 0)
            cost = (bytes_scanned / (1024 ** 4)) * 5.0

            sql = execution["Query"][:60]
            submit_time = execution["Status"].get("SubmissionDateTime", "")

            history.append({
                "query_id": execution["QueryExecutionId"],
                "sql_preview": sql,
                "state": state,
                "bytes_scanned": bytes_scanned,
                "cost_usd": cost,
                "time": submit_time
            })

            total_bytes += bytes_scanned
            total_cost += cost

        return history, total_bytes, total_cost


if __name__ == "__main__":
    runner = AthenaQueryRunner(workgroup="data_engineering")

    # --- QUERY 1: Simple count ---
    result = runner.execute_query(
        "SELECT COUNT(*) as total_orders FROM raw_orders"
    )
    if result and result["state"] == "SUCCEEDED":
        print(f"   Result: {result['rows'][0]}")

    # --- QUERY 2: Aggregation ---
    result = runner.execute_query("""
        SELECT 
            status,
            COUNT(*) as order_count,
            ROUND(SUM(total_amount), 2) as revenue
        FROM raw_orders
        GROUP BY status
        ORDER BY revenue DESC
    """)
    if result and result["state"] == "SUCCEEDED":
        print(f"\n   📊 Revenue by Status:")
        for row in result["rows"]:
            print(f"      {row['status']:<15} {row['order_count']:>10} orders  ${row['revenue']:>15}")

    # --- QUERY 3: Compare CSV vs Parquet cost ---
    print(f"\n{'═' * 60}")
    print("💰 CSV vs PARQUET COST COMPARISON")
    print("═" * 60)

    csv_result = runner.execute_query("""
        SELECT status, COUNT(*) as cnt, ROUND(SUM(total_amount), 2) as revenue
        FROM raw_orders
        GROUP BY status
    """)

    parquet_result = runner.execute_query("""
        SELECT status, COUNT(*) as cnt, ROUND(SUM(total_amount), 2) as revenue
        FROM curated_orders_snapshot
        GROUP BY status
    """)

    if csv_result and parquet_result:
        csv_mb = csv_result["mb_scanned"]
        pq_mb = parquet_result["mb_scanned"]
        savings = round((1 - pq_mb / max(csv_mb, 0.001)) * 100, 1)

        print(f"\n   CSV table:     {csv_mb} MB scanned → ${csv_result['cost_usd']:.6f}")
        print(f"   Parquet table:  {pq_mb} MB scanned → ${parquet_result['cost_usd']:.6f}")
        print(f"   Savings:        {savings}% less data scanned with Parquet")