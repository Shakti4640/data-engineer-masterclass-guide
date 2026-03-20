# file: lineage/08_seed_full_lineage.py
# Purpose: Populate DynamoDB with the COMPLETE 5-hop lineage chain
# from the CFO's problem so we can test the traversal engine

from lineage_emitter import LineageEmitter, LineageNode

ACCOUNT_A = "[REDACTED:BANK_ACCOUNT_NUMBER]"   # Ingestion
ACCOUNT_B = "[REDACTED:BANK_ACCOUNT_NUMBER]"   # Processing
ACCOUNT_C = "[REDACTED:BANK_ACCOUNT_NUMBER]"   # Serving


def seed_full_lineage():
    """
    Seed the complete 5-hop chain:
    
    HOP 0 (Source):
      MariaDB ecommerce.orders.quantity → bronze.raw_orders.qty
      MariaDB payments.refunds.refund_amount → bronze.raw_refunds.refund_amount
    
    HOP 1 (Bronze → Silver Clean):
      bronze.raw_orders.qty → silver.orders_cleaned.quantity (RENAME+CAST)
      bronze.raw_orders.unit_price → silver.orders_cleaned.unit_price (CAST)
      silver.orders_cleaned.quantity × unit_price → silver.orders_cleaned.order_total (DERIVE)
    
    HOP 2 (Bronze → Silver Refunds):
      bronze.raw_refunds.refund_amount → silver.refunds_processed.refund_amount (CAST+FILTER)
    
    HOP 3 (Silver → Silver Aggregation via Athena View):
      silver.orders_cleaned.order_total → silver.order_metrics.sum_order_total (SUM)
      silver.refunds_processed.refund_amount → silver.order_metrics.sum_refund_amount (SUM)
    
    HOP 4 (Silver → Gold):
      silver.order_metrics.sum_order_total - sum_refund_amount
        → gold.finance_summary.total_revenue_adjusted (EXPRESSION)
    """

    # ═══════════════════════════════════════════════════
    # HOP 0: MariaDB → Bronze (JDBC Extract — Project 31)
    # ═══════════════════════════════════════════════════

    emitter_extract = LineageEmitter(
        job_name="job_extract_orders",
        job_run_id="jr_extract_20250115_001",
        account_id=ACCOUNT_A
    )

    # MariaDB source columns
    maria_qty = LineageNode(
        ACCOUNT_A, "mariadb", "ecommerce", "orders", "quantity",
        data_type="varchar(10)", tags=["source-of-truth", "financial"]
    )
    maria_price = LineageNode(
        ACCOUNT_A, "mariadb", "ecommerce", "orders", "unit_price",
        data_type="varchar(10)", tags=["source-of-truth", "financial"]
    )
    maria_status = LineageNode(
        ACCOUNT_A, "mariadb", "ecommerce", "orders", "status",
        data_type="varchar(20)", tags=["source-of-truth"]
    )
    maria_date = LineageNode(
        ACCOUNT_A, "mariadb", "ecommerce", "orders", "order_date",
        data_type="varchar(10)", tags=["source-of-truth", "temporal"]
    )
    maria_refund = LineageNode(
        ACCOUNT_A, "mariadb", "payments", "refunds", "refund_amount",
        data_type="decimal(10,2)", tags=["source-of-truth", "financial"]
    )

    # Bronze landing columns
    bronze_qty = LineageNode(
        ACCOUNT_A, "glue", "bronze", "raw_orders", "qty",
        data_type="string", tags=["raw"]
    )
    bronze_price = LineageNode(
        ACCOUNT_A, "glue", "bronze", "raw_orders", "unit_price",
        data_type="string", tags=["raw"]
    )
    bronze_status = LineageNode(
        ACCOUNT_A, "glue", "bronze", "raw_orders", "status",
        data_type="string", tags=["raw"]
    )
    bronze_date = LineageNode(
        ACCOUNT_A, "glue", "bronze", "raw_orders", "order_date",
        data_type="string", tags=["raw"]
    )
    bronze_refund = LineageNode(
        ACCOUNT_A, "glue", "bronze", "raw_refunds", "refund_amount",
        data_type="string", tags=["raw"]
    )

    # Emit HOP 0 edges
    emitter_extract.emit_column_mapping(
        maria_qty, bronze_qty,
        {"type": "JDBC_EXTRACT", "note": "All types land as STRING in Parquet"}
    )
    emitter_extract.emit_column_mapping(
        maria_price, bronze_price,
        {"type": "JDBC_EXTRACT", "note": "All types land as STRING"}
    )
    emitter_extract.emit_column_mapping(
        maria_status, bronze_status,
        {"type": "JDBC_EXTRACT", "note": "Includes cancelled orders"}
    )
    emitter_extract.emit_column_mapping(
        maria_date, bronze_date,
        {"type": "JDBC_EXTRACT", "note": "Date as string yyyy-MM-dd"}
    )
    emitter_extract.emit_column_mapping(
        maria_refund, bronze_refund,
        {"type": "JDBC_EXTRACT", "note": "From payments DB, not ecommerce DB"}
    )
    emitter_extract.finalize()
    print("✅ HOP 0 seeded: MariaDB → Bronze")

    # ═══════════════════════════════════════════════════
    # HOP 1: Bronze → Silver (Clean Orders — Project 34)
    # ═══════════════════════════════════════════════════

    emitter_clean = LineageEmitter(
        job_name="job_clean_orders",
        job_run_id="jr_clean_20250115_001",
        account_id=ACCOUNT_B
    )

    silver_quantity = LineageNode(
        ACCOUNT_B, "glue", "silver", "orders_cleaned", "quantity",
        data_type="int", tags=["cleaned", "financial"]
    )
    silver_price = LineageNode(
        ACCOUNT_B, "glue", "silver", "orders_cleaned", "unit_price",
        data_type="decimal(10,2)", tags=["cleaned", "financial"]
    )
    silver_total = LineageNode(
        ACCOUNT_B, "glue", "silver", "orders_cleaned", "order_total",
        data_type="decimal(10,2)", tags=["cleaned", "financial", "derived"]
    )
    silver_date = LineageNode(
        ACCOUNT_B, "glue", "silver", "orders_cleaned", "order_date",
        data_type="date", tags=["cleaned", "temporal"]
    )

    emitter_clean.emit_column_mapping(
        bronze_qty, silver_quantity,
        {
            "type": "RENAME_AND_CAST",
            "rename": {"from": "qty", "to": "quantity"},
            "cast": {"from_type": "string", "to_type": "int"},
            "filter": "quantity > 0 (rows with qty <= 0 DROPPED)"
        }
    )
    emitter_clean.emit_column_mapping(
        bronze_price, silver_price,
        {
            "type": "CAST",
            "cast": {"from_type": "string", "to_type": "decimal(10,2)"},
            "filter": "unit_price > 0 (rows with price <= 0 DROPPED)"
        }
    )
    # THE KEY EDGE: status filter that drops cancelled orders
    # THIS IS WHERE THE $1.6M DISCREPANCY LIVES
    emitter_clean.emit_column_mapping(
        bronze_status, silver_quantity,
        {
            "type": "FILTER",
            "filter": "status != 'cancelled' (ALL CANCELLED ORDERS DROPPED)",
            "note": "THIS FILTER REMOVES ROWS — affects downstream totals",
            "severity": "HIGH"
        }
    )
    emitter_clean.emit_column_mapping(
        bronze_qty, silver_total,
        {
            "type": "EXPRESSION",
            "expression": "CAST(qty AS INT) * CAST(unit_price AS DECIMAL)",
            "role": "multiplicand"
        }
    )
    emitter_clean.emit_column_mapping(
        bronze_price, silver_total,
        {
            "type": "EXPRESSION",
            "expression": "CAST(qty AS INT) * CAST(unit_price AS DECIMAL)",
            "role": "multiplier"
        }
    )
    emitter_clean.emit_column_mapping(
        bronze_date, silver_date,
        {
            "type": "CAST",
            "cast": {"from_type": "string", "to_type": "date"}
        }
    )
    emitter_clean.finalize()
    print("✅ HOP 1 seeded: Bronze → Silver (orders_cleaned)")

    # ═══════════════════════════════════════════════════
    # HOP 2: Bronze → Silver (Refunds)
    # ═══════════════════════════════════════════════════

    emitter_refunds = LineageEmitter(
        job_name="job_process_refunds",
        job_run_id="jr_refunds_20250115_001",
        account_id=ACCOUNT_B
    )

    silver_refund_amt = LineageNode(
        ACCOUNT_B, "glue", "silver", "refunds_processed", "refund_amount",
        data_type="decimal(10,2)", tags=["cleaned", "financial"]
    )

    emitter_refunds.emit_column_mapping(
        bronze_refund, silver_refund_amt,
        {
            "type": "CAST_AND_FILTER",
            "cast": {"from_type": "string", "to_type": "decimal(10,2)"},
            "filter": "refund_amount > 0 AND status = 'approved'"
        }
    )
    emitter_refunds.finalize()
    print("✅ HOP 2 seeded: Bronze → Silver (refunds_processed)")

    # ═══════════════════════════════════════════════════
    # HOP 3: Silver → Silver Aggregation (Athena View — Project 37)
    # ═══════════════════════════════════════════════════

    emitter_agg = LineageEmitter(
        job_name="athena_view_order_metrics",
        job_run_id="view_definition_v3",
        account_id=ACCOUNT_B
    )

    metrics_sum_total = LineageNode(
        ACCOUNT_B, "athena", "silver", "order_metrics", "sum_order_total",
        data_type="decimal(12,2)", tags=["aggregated", "financial"]
    )
    metrics_sum_refund = LineageNode(
        ACCOUNT_B, "athena", "silver", "order_metrics", "sum_refund_amount",
        data_type="decimal(12,2)", tags=["aggregated", "financial"]
    )

    emitter_agg.emit_column_mapping(
        silver_total, metrics_sum_total,
        {
            "type": "AGGREGATION",
            "expression": "SUM(order_total) GROUP BY order_date",
            "note": "Athena view — SQL-based transformation"
        }
    )
    emitter_agg.emit_column_mapping(
        silver_refund_amt, metrics_sum_refund,
        {
            "type": "AGGREGATION",
            "expression": "SUM(refund_amount) GROUP BY order_date",
            "note": "LEFT JOIN — unmatched orders get NULL refund (COALESCE 0)"
        }
    )
    emitter_agg.finalize()
    print("✅ HOP 3 seeded: Silver → Silver (order_metrics view)")

    # ═══════════════════════════════════════════════════
    # HOP 4: Silver → Gold (Final Derived Metric)
    # ═══════════════════════════════════════════════════

    emitter_gold = LineageEmitter(
        job_name="job_build_finance_summary",
        job_run_id="jr_gold_20250115_001",
        account_id=ACCOUNT_C
    )

    gold_revenue = LineageNode(
        ACCOUNT_C, "redshift", "gold", "finance_summary",
        "total_revenue_adjusted",
        data_type="decimal(12,2)",
        tags=["gold-tier", "financial", "derived", "cfo-dashboard"]
    )

    emitter_gold.emit_column_mapping(
        metrics_sum_total, gold_revenue,
        {
            "type": "EXPRESSION",
            "expression": "sum_order_total - COALESCE(sum_refund_amount, 0)",
            "role": "minuend",
            "note": "Revenue = orders - refunds"
        }
    )
    emitter_gold.emit_column_mapping(
        metrics_sum_refund, gold_revenue,
        {
            "type": "EXPRESSION",
            "expression": "sum_order_total - COALESCE(sum_refund_amount, 0)",
            "role": "subtrahend",
            "note": "Revenue = orders - refunds"
        }
    )
    emitter_gold.finalize()
    print("✅ HOP 4 seeded: Silver → Gold (finance_summary)")

    print("\n" + "=" * 70)
    print("🎯 FULL 5-HOP LINEAGE CHAIN SEEDED SUCCESSFULLY")
    print("=" * 70)
    print("""
    mariadb.ecommerce.orders.quantity
      → [JDBC_EXTRACT] → bronze.raw_orders.qty
      → [RENAME+CAST+FILTER(cancelled)] → silver.orders_cleaned.quantity
      → [MULTIPLY by unit_price] → silver.orders_cleaned.order_total
      → [SUM GROUP BY date] → silver.order_metrics.sum_order_total
      → [SUBTRACT refunds] → gold.finance_summary.total_revenue_adjusted
    
    Run investigation:
    python lineage_cli.py investigate \\
        --node "333:redshift:gold.finance_summary.total_revenue_adjusted"
    """)


if __name__ == "__main__":
    seed_full_lineage()