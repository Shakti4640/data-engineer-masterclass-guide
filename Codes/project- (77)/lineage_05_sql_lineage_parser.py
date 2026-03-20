# file: lineage/05_sql_lineage_parser.py
# Purpose: Extract column-level lineage from Athena/Redshift SQL view definitions
# Handles the views that Glue jobs don't touch — pure SQL transformations

import re
import boto3
import json
from datetime import datetime, timezone

REGION = "us-east-2"


class SQLLineageParser:
    """
    Parses SQL view definitions to extract column-level lineage
    
    WHY THIS IS NEEDED:
    → Athena views (Project 37) create transformations via SQL
    → Redshift views/materialized views (Project 63) do the same
    → These are NOT Glue jobs — no active lineage emission
    → Must PARSE the SQL to infer lineage
    
    LIMITATIONS:
    → Cannot handle dynamic SQL or complex CTEs perfectly
    → Regex-based parsing covers 80% of cases
    → For remaining 20%: use sqllineage or sqlglot library
    → Production systems use sqlglot (AST-based SQL parser)
    """

    def __init__(self, account_id):
        self.account_id = account_id
        self.athena = boto3.client("athena", region_name=REGION)

    def get_athena_view_ddl(self, database, view_name):
        """
        Retrieve CREATE VIEW statement from Athena
        Uses SHOW CREATE VIEW query
        """
        query = f"SHOW CREATE VIEW {database}.{view_name}"
        
        # Start query execution
        response = self.athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={
                "OutputLocation": "s3://quickcart-athena-results-prod/lineage/"
            }
        )
        execution_id = response["QueryExecutionId"]

        # Wait for completion (simplified — production uses waiter)
        import time
        for _ in range(30):
            status = self.athena.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = status["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)

        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena query failed: {state}")

        # Get results
        results = self.athena.get_query_results(
            QueryExecutionId=execution_id
        )

        # Extract DDL from results
        ddl_lines = []
        for row in results["ResultSet"]["Rows"]:
            for datum in row["Data"]:
                if "VarCharValue" in datum:
                    ddl_lines.append(datum["VarCharValue"])

        return "\n".join(ddl_lines)

    def parse_select_columns(self, sql):
        """
        Parse SELECT clause to extract column mappings
        
        Handles:
        → Direct columns: o.order_date → maps to source table.column
        → Aliases: SUM(o.order_total) as sum_order_total
        → Expressions: a.qty * a.price as total
        → Aggregations: COUNT(*), AVG(x), etc.
        
        Returns list of:
        {
            "output_column": "sum_order_total",
            "expression": "SUM(o.order_total)",
            "source_columns": [{"alias": "o", "column": "order_total"}],
            "transform_type": "AGGREGATION"
        }
        """
        # Remove CREATE VIEW ... AS prefix
        select_match = re.search(
            r'SELECT\s+(.*?)\s+FROM',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if not select_match:
            return []

        select_clause = select_match.group(1)

        # Split by comma (respecting parentheses)
        columns = self._split_select_columns(select_clause)

        mappings = []
        for col_expr in columns:
            col_expr = col_expr.strip()
            if not col_expr:
                continue

            mapping = self._parse_single_column(col_expr)
            if mapping:
                mappings.append(mapping)

        return mappings

    def _split_select_columns(self, select_clause):
        """Split SELECT columns by comma, respecting parentheses depth"""
        columns = []
        current = ""
        depth = 0

        for char in select_clause:
            if char == "(":
                depth += 1
                current += char
            elif char == ")":
                depth -= 1
                current += char
            elif char == "," and depth == 0:
                columns.append(current.strip())
                current = ""
            else:
                current += char

        if current.strip():
            columns.append(current.strip())

        return columns

    def _parse_single_column(self, expr):
        """
        Parse a single SELECT column expression
        
        Examples:
        → "o.order_date"
        → "SUM(o.order_total) as sum_order_total"
        → "o.qty * o.unit_price as order_total"
        → "COALESCE(r.refund_amount, 0) as refund_amount"
        """
        # Check for alias: ... AS alias_name
        alias_match = re.match(
            r'(.+?)\s+(?:AS\s+)?(\w+)\s*$',
            expr,
            re.IGNORECASE
        )

        if alias_match:
            expression = alias_match.group(1).strip()
            output_name = alias_match.group(2).strip()
        else:
            expression = expr.strip()
            # No alias — output name is the column name itself
            col_match = re.match(r'(?:\w+\.)?(\w+)', expression)
            output_name = col_match.group(1) if col_match else expression

        # Extract all table.column references from expression
        source_refs = re.findall(r'(\w+)\.(\w+)', expression)
        source_columns = [
            {"alias": ref[0], "column": ref[1]}
            for ref in source_refs
        ]

        # Determine transform type
        transform_type = "PASSTHROUGH"
        agg_functions = ["SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV"]
        for func in agg_functions:
            if func in expression.upper():
                transform_type = "AGGREGATION"
                break

        if "*" in expression and not any(
            f in expression.upper() for f in agg_functions
        ):
            transform_type = "EXPRESSION"
        elif len(source_columns) > 1:
            transform_type = "EXPRESSION"
        elif "COALESCE" in expression.upper():
            transform_type = "COALESCE"
        elif "CASE" in expression.upper():
            transform_type = "CONDITIONAL"
        elif "CAST" in expression.upper():
            transform_type = "CAST"

        return {
            "output_column": output_name,
            "expression": expression,
            "source_columns": source_columns,
            "transform_type": transform_type
        }

    def parse_from_clause(self, sql):
        """
        Parse FROM and JOIN clauses to build alias → table mapping
        
        Example:
        FROM silver.orders_cleaned o
        LEFT JOIN silver.refunds_processed r ON o.order_id = r.order_id
        
        Returns: {"o": "silver.orders_cleaned", "r": "silver.refunds_processed"}
        """
        alias_map = {}

        # Match FROM table alias
        from_match = re.search(
            r'FROM\s+(\w+\.\w+)\s+(\w+)',
            sql,
            re.IGNORECASE
        )
        if from_match:
            alias_map[from_match.group(2)] = from_match.group(1)

        # Match JOIN table alias
        join_matches = re.finditer(
            r'JOIN\s+(\w+\.\w+)\s+(\w+)',
            sql,
            re.IGNORECASE
        )
        for match in join_matches:
            alias_map[match.group(2)] = match.group(1)

        return alias_map

    def extract_lineage_from_view(self, database, view_name, sql=None):
        """
        Full lineage extraction from a SQL view
        
        Combines:
        → Column parsing (what outputs come from what inputs)
        → FROM parsing (resolve aliases to real table names)
        → Builds LineageNode + LineageEdge objects
        
        Returns list of edges ready for LineageEmitter
        """
        if sql is None:
            sql = self.get_athena_view_ddl(database, view_name)

        # Parse alias map
        alias_map = self.parse_from_clause(sql)

        # Parse column mappings
        column_mappings = self.parse_select_columns(sql)

        edges = []
        for mapping in column_mappings:
            output_col = mapping["output_column"]

            for src in mapping["source_columns"]:
                alias = src["alias"]
                src_column = src["column"]

                # Resolve alias to real table
                real_table = alias_map.get(alias, f"unknown_{alias}")

                # Split database.table
                parts = real_table.split(".")
                src_db = parts[0] if len(parts) > 1 else database
                src_tbl = parts[1] if len(parts) > 1 else parts[0]

                edge = {
                    "source": {
                        "account_id": self.account_id,
                        "service": "athena",
                        "database": src_db,
                        "table": src_tbl,
                        "column": src_column
                    },
                    "target": {
                        "account_id": self.account_id,
                        "service": "athena",
                        "database": database,
                        "table": view_name,
                        "column": output_col
                    },
                    "transform": {
                        "type": mapping["transform_type"],
                        "expression": mapping["expression"]
                    }
                }
                edges.append(edge)

        return edges


def parse_and_emit_view_lineage(database, view_name, sql, emitter):
    """
    Convenience function: parse SQL view and emit lineage via emitter
    
    Bridges passive SQL parsing with active lineage emission
    """
    from lineage_emitter import LineageNode

    parser = SQLLineageParser(account_id=emitter.account_id)
    edges = parser.extract_lineage_from_view(database, view_name, sql)

    for edge_data in edges:
        src = edge_data["source"]
        tgt = edge_data["target"]
        transform = edge_data["transform"]

        source_node = LineageNode(
            account_id=src["account_id"],
            service=src["service"],
            database=src["database"],
            table=src["table"],
            column=src["column"]
        )

        target_node = LineageNode(
            account_id=tgt["account_id"],
            service=tgt["service"],
            database=tgt["database"],
            table=tgt["table"],
            column=tgt["column"]
        )

        emitter.emit_column_mapping(
            source_node, target_node, transform
        )

    return len(edges)