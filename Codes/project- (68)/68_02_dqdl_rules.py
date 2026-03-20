# file: 68_02_dqdl_rules.py
# Purpose: Define DQDL rulesets for each source
# These are used inside Glue ETL jobs via EvaluateDataQuality transform

# ============================================================
# DQDL RULESETS — Declarative quality rules per source
# ============================================================

DQDL_RULES = {
    "partner_feed": {
        "description": "Quality rules for partner feed CSV",
        "severity": "HARD_FAIL",
        "ruleset": """
            Rules = [
                # VOLUME: row count within expected range
                RowCount between 2000 and 15000,
                
                # UNIQUENESS: order_id must be unique (catches the 10x duplicate incident)
                IsUnique "order_id",
                
                # COMPLETENESS: critical fields must not be null
                IsComplete "order_id",
                IsComplete "customer_id",
                Completeness "total_amount" >= 0.99,
                Completeness "order_date" >= 0.99,
                
                # VALIDITY: values in expected ranges
                ColumnValues "total_amount" > 0,
                ColumnValues "total_amount" < 50000,
                
                # FRESHNESS: order dates should be recent
                DataFreshness "order_date" <= 7 days
            ]
        """
    },

    "payment_data": {
        "description": "Quality rules for payment data JSON",
        "severity": "HARD_FAIL",
        "ruleset": """
            Rules = [
                # VOLUME: at least some rows
                RowCount > 0,
                
                # COMPLETENESS
                IsComplete "transaction_id",
                IsComplete "amount",
                IsComplete "payment_date",
                
                # UNIQUENESS
                IsUnique "transaction_id",
                
                # VALIDITY: no negative payments
                ColumnValues "amount" > 0,
                
                # VALIDITY: no future dates
                ColumnValues "payment_date" <= now()
            ]
        """
    },

    "cdc_mariadb_orders": {
        "description": "Quality rules for CDC delta from MariaDB orders",
        "severity": "HARD_FAIL",
        "ruleset": """
            Rules = [
                # COMPLETENESS: primary key never null
                IsComplete "order_id",
                
                # VALIDITY: amounts make sense
                ColumnValues "total_amount" >= 0,
                ColumnValues "quantity" > 0,
                
                # VALIDITY: status is known value
                ColumnValues "status" in ["completed", "pending", "shipped", "cancelled", "refunded"],
                
                # COMPLETENESS: audit columns present
                IsComplete "updated_at",
                IsComplete "created_at"
            ]
        """
    },

    "silver_layer_generic": {
        "description": "Generic quality rules for any silver layer table",
        "severity": "SOFT_WARN",
        "ruleset": """
            Rules = [
                # ETL metadata present
                IsComplete "etl_loaded_at",
                IsComplete "etl_source",
                
                # Freshness: loaded recently
                DataFreshness "etl_loaded_at" <= 24 hours
            ]
        """
    },

    "gold_daily_revenue": {
        "description": "Quality rules for gold daily revenue aggregation",
        "severity": "HARD_FAIL",
        "ruleset": """
            Rules = [
                # VOLUME: should have rows for each day
                RowCount between 1 and 31,
                
                # COMPLETENESS
                IsComplete "order_date",
                IsComplete "total_revenue",
                
                # VALIDITY: revenue should be positive
                ColumnValues "total_revenue" > 0,
                
                # VALIDITY: order count should be positive
                ColumnValues "order_count" > 0,
                
                # STATISTICAL: avg order value should be reasonable
                Mean "avg_order_value" between 10.0 and 1000.0
            ]
        """
    }
}