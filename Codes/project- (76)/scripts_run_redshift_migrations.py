# file: scripts/run_redshift_migrations.py
# Executes numbered SQL migration files against Redshift
# Tracks applied migrations in _migrations table
# Idempotent: only runs new migrations

import boto3
import os
import sys
import time
import argparse

REGION = "us-east-2"


def get_config(environment):
    """Get environment-specific configuration"""
    configs = {
        "dev": {
            "workgroup": "quickcart-dev",
            "database": "quickcart_dev"
        },
        "staging": {
            "workgroup": "quickcart-staging",
            "database": "quickcart_staging"
        },
        "prod": {
            "workgroup": "quickcart-analytics",
            "database": "quickcart"
        }
    }
    return configs.get(environment, configs["dev"])


def execute_sql(rs_data, workgroup, database, sql, wait=True):
    """Execute SQL via Redshift Data API and wait for result"""
    response = rs_data.execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sql=sql
    )
    stmt_id = response["Id"]

    if not wait:
        return stmt_id, None

    # Poll for completion
    for _ in range(120):  # 2 minute timeout
        status = rs_data.describe_statement(Id=stmt_id)
        state = status["Status"]

        if state == "FINISHED":
            if status.get("HasResultSet"):
                results = rs_data.get_statement_result(Id=stmt_id)
                return stmt_id, results
            return stmt_id, None
        elif state in ("FAILED", "ABORTED"):
            error = status.get("Error", "Unknown error")
            raise Exception(f"SQL failed: {error}\nSQL: {sql[:200]}")

        time.sleep(1)

    raise Exception(f"SQL timed out after 120 seconds")


def ensure_migrations_table(rs_data, workgroup, database):
    """Create migrations tracking table if it doesn't exist"""
    sql = """
    CREATE TABLE IF NOT EXISTS public._migrations (
        version         INTEGER PRIMARY KEY,
        filename        VARCHAR(255) NOT NULL,
        applied_at      TIMESTAMP DEFAULT GETDATE(),
        applied_by      VARCHAR(100) DEFAULT CURRENT_USER,
        execution_time_ms INTEGER
    );
    """
    execute_sql(rs_data, workgroup, database, sql)
    print("   ✅ Migrations table ready: public._migrations")


def get_applied_versions(rs_data, workgroup, database):
    """Get list of already-applied migration versions"""
    sql = "SELECT version FROM public._migrations ORDER BY version;"

    try:
        _, results = execute_sql(rs_data, workgroup, database, sql)
        if results and results.get("Records"):
            return [int(row[0]["longValue"]) for row in results["Records"]]
    except Exception:
        pass

    return []


def get_pending_migrations(migration_dir, applied_versions):
    """Find migration files that haven't been applied yet"""
    if not os.path.exists(migration_dir):
        print(f"   ⚠️  Migration directory not found: {migration_dir}")
        return []

    all_files = sorted([
        f for f in os.listdir(migration_dir)
        if f.endswith(".sql")
    ])

    pending = []
    for filename in all_files:
        try:
            version = int(filename.split("_")[0])
            if version not in applied_versions:
                pending.append((version, filename))
        except ValueError:
            print(f"   ⚠️  Skipping non-numbered file: {filename}")

    return pending


def apply_migration(rs_data, workgroup, database, migration_dir, version, filename):
    """Apply a single migration file"""
    filepath = os.path.join(migration_dir, filename)

    with open(filepath, "r") as f:
        sql = f.read()

    print(f"\n   📋 Applying migration {version}: {filename}")
    print(f"      SQL preview: {sql[:100].strip()}...")

    start_time = time.time()

    try:
        # Execute the migration SQL
        # Split by semicolons for multi-statement files
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        for i, stmt in enumerate(statements):
            if not stmt or stmt.startswith("--"):
                continue
            execute_sql(rs_data, workgroup, database, stmt + ";")

        elapsed_ms = int((time.time() - start_time) * 1000)

        # Record successful migration
        record_sql = f"""
        INSERT INTO public._migrations (version, filename, execution_time_ms)
        VALUES ({version}, '{filename}', {elapsed_ms});
        """
        execute_sql(rs_data, workgroup, database, record_sql)

        print(f"      ✅ Applied successfully ({elapsed_ms}ms)")
        return True

    except Exception as e:
        print(f"      ❌ FAILED: {e}")
        print(f"      ⚠️  Migration {version} NOT applied — pipeline should fail here")
        return False


def run_migrations(environment):
    """Main migration runner"""
    config = get_config(environment)
    workgroup = config["workgroup"]
    database = config["database"]
    migration_dir = os.path.join("redshift", "migrations")

    rs_data = boto3.client("redshift-data", region_name=REGION)

    print("=" * 60)
    print(f"🔄 REDSHIFT MIGRATIONS — {environment.upper()}")
    print(f"   Workgroup: {workgroup}")
    print(f"   Database: {database}")
    print("=" * 60)

    # Step 1: Ensure migrations table exists
    print("\n--- Checking migrations table ---")
    ensure_migrations_table(rs_data, workgroup, database)

    # Step 2: Get applied versions
    applied = get_applied_versions(rs_data, workgroup, database)
    print(f"   Applied migrations: {applied or '(none)'}")

    # Step 3: Find pending migrations
    pending = get_pending_migrations(migration_dir, applied)
    if not pending:
        print("\n   ✅ No pending migrations — database is up to date")
        return True

    print(f"\n   Pending migrations: {len(pending)}")
    for version, filename in pending:
        print(f"      → {version}: {filename}")

    # Step 4: Apply each pending migration IN ORDER
    all_success = True
    for version, filename in pending:
        success = apply_migration(
            rs_data, workgroup, database,
            migration_dir, version, filename
        )
        if not success:
            all_success = False
            print(f"\n   🛑 STOPPING: Migration {version} failed")
            print(f"   Remaining migrations will NOT be applied")
            break

    # Summary
    print(f"\n{'=' * 60}")
    if all_success:
        print(f"✅ ALL MIGRATIONS APPLIED SUCCESSFULLY")
    else:
        print(f"❌ MIGRATION FAILED — manual intervention required")
        sys.exit(1)
    print(f"{'=' * 60}")

    return all_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Redshift migrations")
    parser.add_argument("--environment", required=True, choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    run_migrations(args.environment)