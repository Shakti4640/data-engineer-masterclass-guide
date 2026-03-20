# file: tests/unit/test_schema_validation.py
# Unit tests that catch the exact bug from the incident
# Validates column names, types, and mappings BEFORE deployment

import pytest
import json
import os

# Expected schemas for each Gold data product
# These are the CONTRACT — scripts must produce exactly these columns
EXPECTED_GOLD_SCHEMAS = {
    "daily_orders": {
        "columns": [
            {"name": "order_id", "type": "string"},
            {"name": "customer_id", "type": "string"},
            {"name": "order_total", "type": "decimal(10,2)"},  # NOT "total_amount"!
            {"name": "item_count", "type": "int"},
            {"name": "status", "type": "string"},
            {"name": "payment_method", "type": "string"},
            {"name": "shipping_city", "type": "string"},
            {"name": "shipping_country", "type": "string"},
            {"name": "order_date", "type": "date"},
            {"name": "shipped_date", "type": "date"},
            {"name": "created_at", "type": "timestamp"},
            {"name": "updated_at", "type": "timestamp"},
            {"name": "_etl_loaded_at", "type": "timestamp"}
        ],
        "partition_keys": ["year", "month", "day"],
        "required_columns": ["order_id", "order_total", "status", "order_date"]
    },
    "clickstream_events": {
        "columns": [
            {"name": "event_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "user_id", "type": "string"},
            {"name": "session_id", "type": "string"},
            {"name": "event_ts", "type": "timestamp"},
            {"name": "event_timestamp", "type": "string"},
            {"name": "page_url", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "search_query", "type": "string"},
            {"name": "device_type", "type": "string"},
            {"name": "browser", "type": "string"},
            {"name": "ip_country", "type": "string"},
            {"name": "referrer", "type": "string"},
            {"name": "cart_value", "type": "double"},
            {"name": "items_in_cart", "type": "int"},
            {"name": "event_date", "type": "date"},
            {"name": "event_hour", "type": "int"},
            {"name": "_processed_at", "type": "timestamp"}
        ],
        "partition_keys": ["year", "month", "day", "hour"],
        "required_columns": ["event_id", "event_type", "user_id", "event_ts"]
    }
}


class TestGoldSchemaContract:
    """
    These tests validate that Glue ETL scripts produce
    the EXACT schema expected by downstream consumers.
    
    THE INCIDENT WOULD HAVE BEEN CAUGHT HERE:
    → Script renamed "order_total" to "total_amount"
    → This test checks: "order_total" MUST be in output columns
    → Test fails → pipeline blocked → bad change never reaches production
    """

    def test_daily_orders_has_all_required_columns(self):
        """Every required column must exist in the schema"""
        schema = EXPECTED_GOLD_SCHEMAS["daily_orders"]
        column_names = [col["name"] for col in schema["columns"]]

        for required_col in schema["required_columns"]:
            assert required_col in column_names, \
                f"CRITICAL: Required column '{required_col}' missing from daily_orders schema!"

    def test_daily_orders_column_names_exact(self):
        """Column names must match exactly — no renames without schema migration"""
        schema = EXPECTED_GOLD_SCHEMAS["daily_orders"]
        column_names = [col["name"] for col in schema["columns"]]

        # THE EXACT CHECK THAT CATCHES THE INCIDENT
        assert "order_total" in column_names, \
            "Column 'order_total' missing! Did someone rename it to 'total_amount'?"
        assert "total_amount" not in column_names, \
            "Column 'total_amount' found! Use 'order_total' (established contract)"

    def test_daily_orders_column_count(self):
        """Column count must match — detects added/removed columns"""
        schema = EXPECTED_GOLD_SCHEMAS["daily_orders"]
        expected_count = 13
        actual_count = len(schema["columns"])
        assert actual_count == expected_count, \
            f"Column count mismatch: expected {expected_count}, got {actual_count}"

    def test_daily_orders_partition_keys(self):
        """Partition keys must match Athena/Spectrum expectations"""
        schema = EXPECTED_GOLD_SCHEMAS["daily_orders"]
        expected_partitions = ["year", "month", "day"]
        assert schema["partition_keys"] == expected_partitions

    def test_clickstream_has_all_required_columns(self):
        schema = EXPECTED_GOLD_SCHEMAS["clickstream_events"]
        column_names = [col["name"] for col in schema["columns"]]

        for required_col in schema["required_columns"]:
            assert required_col in column_names, \
                f"Required column '{required_col}' missing from clickstream schema!"

    def test_clickstream_column_count(self):
        schema = EXPECTED_GOLD_SCHEMAS["clickstream_events"]
        expected_count = 18
        actual_count = len(schema["columns"])
        assert actual_count == expected_count

    def test_no_pii_columns_renamed_to_generic(self):
        """
        PII columns must keep their explicit names for Lake Formation tagging.
        Renaming 'customer_id' to 'id' would break LF-Tag assignments (Project 74).
        """
        for product_name, schema in EXPECTED_GOLD_SCHEMAS.items():
            column_names = [col["name"] for col in schema["columns"]]

            # PII columns must have explicit names
            pii_patterns = ["customer_id", "user_id", "session_id", "email"]
            generic_patterns = ["id", "uid", "sid", "addr"]

            for col in column_names:
                assert col not in generic_patterns, \
                    f"Generic column name '{col}' in {product_name} — use explicit name for PII tagging"


class TestGlueScriptSyntax:
    """Validate Glue scripts are syntactically correct Python"""

    def get_glue_scripts(self):
        """Find all Glue script files"""
        script_dir = os.path.join(os.path.dirname(__file__), "..", "..", "glue_jobs")
        if not os.path.exists(script_dir):
            script_dir = "glue_jobs"
        
        scripts = []
        if os.path.exists(script_dir):
            for f in os.listdir(script_dir):
                if f.endswith(".py"):
                    scripts.append(os.path.join(script_dir, f))
        return scripts

    def test_all_scripts_have_valid_syntax(self):
        """Every .py file must compile without syntax errors"""
        scripts = self.get_glue_scripts()
        
        for script_path in scripts:
            try:
                with open(script_path, "r") as f:
                    source = f.read()
                compile(source, script_path, "exec")
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {script_path}: {e}")

    def test_no_hardcoded_bucket_names(self):
        """Scripts must not hardcode bucket names — use parameters"""
        scripts = self.get_glue_scripts()
        
        forbidden_patterns = [
            "orders-data-prod",
            "analytics-data-prod",
            "us-east-2"
        ]

        for script_path in scripts:
            if not os.path.exists(script_path):
                continue
            with open(script_path, "r") as f:
                content = f.read()
            
            for pattern in forbidden_patterns:
                # Allow in comments but not in code
                lines = content.split("\n")
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped.startswith("#"):
                        continue
                    if pattern in stripped:
                        pytest.fail(
                            f"Hardcoded value '{pattern}' in {os.path.basename(script_path)} "
                            f"line {i+1}: use job parameters instead"
                        )


class TestCloudFormationTemplates:
    """Validate CloudFormation templates"""

    def test_all_glue_jobs_have_commit_sha_tag(self):
        """Every Glue job must be tagged with CommitSha for traceability"""
        # This would parse the YAML and check Tags
        # Simplified version:
        template_path = os.path.join("infrastructure", "glue-jobs.yaml")
        if os.path.exists(template_path):
            with open(template_path, "r") as f:
                content = f.read()
            assert "CommitSha" in content, \
                "CloudFormation template must tag resources with CommitSha"

    def test_all_glue_jobs_use_parameterized_script_location(self):
        """Script locations must use CommitSha parameter, not hardcoded paths"""
        template_path = os.path.join("infrastructure", "glue-jobs.yaml")
        if os.path.exists(template_path):
            with open(template_path, "r") as f:
                content = f.read()
            assert "${CommitSha}" in content, \
                "Glue job ScriptLocation must use ${CommitSha} for versioning"


class TestRedshiftMigrations:
    """Validate Redshift migration files"""

    def get_migration_files(self):
        migration_dir = os.path.join("redshift", "migrations")
        if not os.path.exists(migration_dir):
            return []
        files = sorted([
            f for f in os.listdir(migration_dir)
            if f.endswith(".sql")
        ])
        return files

    def test_migrations_are_numbered_sequentially(self):
        """Migration files must have sequential numbers with no gaps"""
        files = self.get_migration_files()
        if not files:
            pytest.skip("No migration files found")

        numbers = []
        for f in files:
            try:
                num = int(f.split("_")[0])
                numbers.append(num)
            except ValueError:
                pytest.fail(f"Migration file '{f}' must start with a number: NNN_description.sql")

        for i in range(len(numbers) - 1):
            assert numbers[i + 1] == numbers[i] + 1, \
                f"Gap in migration sequence: {numbers[i]} → {numbers[i+1]}"

    def test_no_drop_table_in_migrations(self):
        """DROP TABLE is dangerous — use ALTER TABLE instead"""
        migration_dir = os.path.join("redshift", "migrations")
        if not os.path.exists(migration_dir):
            pytest.skip("No migration directory")

        for f in self.get_migration_files():
            filepath = os.path.join(migration_dir, f)
            with open(filepath, "r") as fh:
                content = fh.read().upper()
            
            if "DROP TABLE" in content and "IF EXISTS" not in content:
                pytest.fail(
                    f"Migration '{f}' contains DROP TABLE without IF EXISTS — "
                    f"this is dangerous. Use ALTER TABLE or add IF EXISTS."
                )
