# file: 45_02_schema_drift_detector.py
# Purpose: Detect and classify schema changes between incoming data and baseline
# Used by Bronze→Silver ETL as first step before transformation

import json
from datetime import datetime
from enum import Enum


class ChangeSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ChangeType(Enum):
    COLUMN_ADDED = "COLUMN_ADDED"
    COLUMN_REMOVED = "COLUMN_REMOVED"
    COLUMN_RENAMED = "COLUMN_RENAMED"
    TYPE_WIDENED = "TYPE_WIDENED"
    TYPE_NARROWED = "TYPE_NARROWED"
    TYPE_CHANGED = "TYPE_CHANGED"
    NULLABLE_CHANGED = "NULLABLE_CHANGED"


class SchemaChange:
    """Represents a single detected schema change"""
    def __init__(self, change_type, column_name, severity, handling,
                 details=None, old_value=None, new_value=None):
        self.change_type = change_type
        self.column_name = column_name
        self.severity = severity
        self.handling = handling
        self.details = details or ""
        self.old_value = old_value
        self.new_value = new_value

    def to_dict(self):
        return {
            "change_type": self.change_type.value,
            "column": self.column_name,
            "severity": self.severity.value,
            "handling": self.handling,
            "details": self.details,
            "old_value": str(self.old_value),
            "new_value": str(self.new_value)
        }


# Type compatibility rules
SAFE_TYPE_WIDENINGS = {
    ("integer", "bigint"),
    ("integer", "decimal(10,2)"),
    ("integer", "double"),
    ("float", "double"),
    ("date", "timestamp"),
    ("boolean", "string"),
}

# Dynamically check decimal widening
def is_decimal_widening(old_type, new_type):
    """Check if decimal precision/scale was widened (safe)"""
    if not (old_type.startswith("decimal") and new_type.startswith("decimal")):
        return False
    try:
        old_parts = old_type.replace("decimal(", "").replace(")", "").split(",")
        new_parts = new_type.replace("decimal(", "").replace(")", "").split(",")
        old_precision, old_scale = int(old_parts[0]), int(old_parts[1])
        new_precision, new_scale = int(new_parts[0]), int(new_parts[1])
        return new_precision >= old_precision and new_scale >= old_scale
    except (ValueError, IndexError):
        return False


def is_varchar_widening(old_type, new_type):
    """Check if varchar length was widened (safe)"""
    if not (old_type.startswith("varchar") and new_type.startswith("varchar")):
        return False
    try:
        old_len = int(old_type.replace("varchar(", "").replace(")", ""))
        new_len = int(new_type.replace("varchar(", "").replace(")", ""))
        return new_len >= old_len
    except (ValueError, IndexError):
        return False


def detect_schema_drift(baseline_schema, incoming_columns, incoming_types=None):
    """
    Compare incoming data schema against baseline and detect all changes.
    
    Parameters:
    → baseline_schema: dict from schema registry (get_active_schema output)
    → incoming_columns: list of column names from incoming DataFrame
    → incoming_types: dict of column_name → type_string (optional)
    
    Returns: SchemaChangeReport
    """
    alias_map = baseline_schema.get("alias_map", {})
    baseline_columns = {col["name"]: col for col in baseline_schema["columns"]}

    # ━━━ STEP 1: Apply alias mapping to incoming columns ━━━
    # Reverse alias map: new_name → old_name
    reverse_alias = {v: k for k, v in alias_map.items()}

    canonical_incoming = {}
    applied_aliases = {}

    for col_name in incoming_columns:
        if col_name in reverse_alias:
            canonical_name = reverse_alias[col_name]
            canonical_incoming[canonical_name] = col_name
            applied_aliases[canonical_name] = col_name
        else:
            canonical_incoming[col_name] = col_name

    # ━━━ STEP 2: Detect column-level changes ━━━
    changes = []
    baseline_names = set(baseline_columns.keys())
    incoming_names = set(canonical_incoming.keys())

    # Columns ADDED (in incoming but not in baseline)
    for col_name in (incoming_names - baseline_names):
        original_name = canonical_incoming[col_name]
        changes.append(SchemaChange(
            change_type=ChangeType.COLUMN_ADDED,
            column_name=original_name,
            severity=ChangeSeverity.LOW,
            handling="AUTO_INCLUDE_NULLABLE",
            details=f"New column '{original_name}' found in source — will include as nullable",
            new_value=original_name
        ))

    # Columns REMOVED (in baseline but not in incoming)
    for col_name in (baseline_names - incoming_names):
        baseline_col = baseline_columns[col_name]
        is_required = baseline_col.get("required_for_silver", False)

        if is_required:
            changes.append(SchemaChange(
                change_type=ChangeType.COLUMN_REMOVED,
                column_name=col_name,
                severity=ChangeSeverity.HIGH,
                handling="FAIL_PIPELINE",
                details=f"Required column '{col_name}' missing — pipeline will FAIL",
                old_value=col_name
            ))
        else:
            changes.append(SchemaChange(
                change_type=ChangeType.COLUMN_REMOVED,
                column_name=col_name,
                severity=ChangeSeverity.MEDIUM,
                handling="FILL_NULL",
                details=f"Optional column '{col_name}' missing — will fill with NULL",
                old_value=col_name
            ))

    # Columns RENAMED (detected via alias map)
    for canonical_name, original_name in applied_aliases.items():
        changes.append(SchemaChange(
            change_type=ChangeType.COLUMN_RENAMED,
            column_name=canonical_name,
            severity=ChangeSeverity.MEDIUM,
            handling="ALIAS_APPLIED",
            details=f"Column renamed: '{original_name}' → '{canonical_name}' (alias applied)",
            old_value=canonical_name,
            new_value=original_name
        ))

    # ━━━ STEP 3: Detect type changes (for common columns) ━━━
    if incoming_types:
        for col_name in (baseline_names & incoming_names):
            baseline_type = baseline_columns[col_name]["type"].lower().strip()
            original_incoming_name = canonical_incoming[col_name]
            incoming_type = incoming_types.get(original_incoming_name, "").lower().strip()

            if not incoming_type or incoming_type == baseline_type:
                continue

            # Check if it's a safe widening
            type_pair = (baseline_type, incoming_type)

            if type_pair in SAFE_TYPE_WIDENINGS:
                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_WIDENED,
                    column_name=col_name,
                    severity=ChangeSeverity.LOW,
                    handling="SAFE_CAST",
                    details=f"Type widened: {baseline_type} → {incoming_type}",
                    old_value=baseline_type,
                    new_value=incoming_type
                ))
            elif is_decimal_widening(baseline_type, incoming_type):
                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_WIDENED,
                    column_name=col_name,
                    severity=ChangeSeverity.LOW,
                    handling="SAFE_CAST",
                    details=f"Decimal precision widened: {baseline_type} → {incoming_type}",
                    old_value=baseline_type,
                    new_value=incoming_type
                ))
            elif is_varchar_widening(baseline_type, incoming_type):
                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_WIDENED,
                    column_name=col_name,
                    severity=ChangeSeverity.LOW,
                    handling="SAFE_CAST",
                    details=f"VARCHAR widened: {baseline_type} → {incoming_type}",
                    old_value=baseline_type,
                    new_value=incoming_type
                ))
            else:
                # Potentially unsafe type change
                changes.append(SchemaChange(
                    change_type=ChangeType.TYPE_CHANGED,
                    column_name=col_name,
                    severity=ChangeSeverity.HIGH,
                    handling="ATTEMPT_CAST_WITH_QUARANTINE",
                    details=f"Type changed: {baseline_type} → {incoming_type} — may cause data loss",
                    old_value=baseline_type,
                    new_value=incoming_type
                ))

    # ━━━ STEP 4: Build report ━━━
    report = build_change_report(baseline_schema["entity_name"], changes)
    return report


def build_change_report(entity_name, changes):
    """Build a structured report from detected changes"""
    auto_handled = [c for c in changes if "FAIL" not in c.handling]
    manual_required = [c for c in changes if "FAIL" in c.handling]

    # Determine pipeline action
    if manual_required:
        pipeline_action = "FAIL"
    elif len(changes) > 5:
        pipeline_action = "CONTINUE_WITH_WARNING"
    elif changes:
        pipeline_action = "CONTINUE"
    else:
        pipeline_action = "NO_CHANGES"

    # Determine overall severity
    if any(c.severity == ChangeSeverity.CRITICAL for c in changes):
        overall_severity = "CRITICAL"
    elif any(c.severity == ChangeSeverity.HIGH for c in changes):
        overall_severity = "HIGH"
    elif any(c.severity == ChangeSeverity.MEDIUM for c in changes):
        overall_severity = "MEDIUM"
    elif changes:
        overall_severity = "LOW"
    else:
        overall_severity = "NONE"

    report = {
        "entity": entity_name,
        "detected_at": datetime.utcnow().isoformat(),
        "total_changes": len(changes),
        "auto_handled": len(auto_handled),
        "manual_required": len(manual_required),
        "overall_severity": overall_severity,
        "pipeline_action": pipeline_action,
        "changes": [c.to_dict() for c in changes]
    }

    return report


def print_change_report(report):
    """Pretty-print a schema change report"""
    print(f"\n{'━'*70}")
    print(f"📋 SCHEMA DRIFT REPORT: {report['entity']}")
    print(f"{'━'*70}")
    print(f"   Detected at: {report['detected_at']}")
    print(f"   Total changes: {report['total_changes']}")
    print(f"   Auto-handled: {report['auto_handled']}")
    print(f"   Manual required: {report['manual_required']}")
    print(f"   Overall severity: {report['overall_severity']}")
    print(f"   Pipeline action: {report['pipeline_action']}")

    if report["changes"]:
        print(f"\n   {'Type':<20} {'Column':<20} {'Severity':<10} {'Handling'}")
        print(f"   {'─'*65}")
        for change in report["changes"]:
            print(f"   {change['change_type']:<20} {change['column']:<20} "
                  f"{change['severity']:<10} {change['handling']}")
            if change.get("details"):
                print(f"   {'':>20} ↳ {change['details']}")
    else:
        print(f"\n   ✅ No schema changes detected")

    print(f"{'━'*70}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DEMONSTRATION: Simulate the QuickCart schema change scenario
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
if __name__ == "__main__":
    from schema_registry import get_active_schema

    print("=" * 70)
    print("🧪 SCHEMA DRIFT DETECTION — SIMULATION")
    print("=" * 70)

    # Get baseline schema
    baseline = get_active_schema("orders")
    print(f"\n📋 Baseline schema: orders v{baseline['version']}")
    print(f"   Columns: {[c['name'] for c in baseline['columns']]}")

    # Simulate incoming data AFTER backend Sprint 47 changes
    incoming_cols = [
        "order_id",           # unchanged
        "customer_id",        # unchanged
        "product_id",         # unchanged (but sometimes NULL now)
        "quantity",           # unchanged
        "unit_price",         # type changed: decimal(10,2) → decimal(10,4)
        "total_amount",       # unchanged
        "order_status",       # RENAMED from "status"
        "shipping_address",   # NEW column
        "order_date",         # unchanged
        "created_at",         # unchanged
        "updated_at"          # unchanged
    ]

    incoming_types = {
        "order_id": "string",
        "customer_id": "string",
        "product_id": "string",
        "quantity": "integer",
        "unit_price": "decimal(10,4)",
        "total_amount": "decimal(12,2)",
        "order_status": "string",
        "shipping_address": "string",
        "order_date": "date",
        "created_at": "timestamp",
        "updated_at": "timestamp"
    }

    # First test: WITHOUT alias map (status looks removed, order_status looks added)
    print(f"\n🔍 Test 1: Detection WITHOUT alias mapping")
    baseline_no_alias = {**baseline, "alias_map": {}}
    report1 = detect_schema_drift(baseline_no_alias, incoming_cols, incoming_types)
    print_change_report(report1)

    # Second test: WITH alias map (status → order_status recognized as rename)
    print(f"\n🔍 Test 2: Detection WITH alias mapping")
    baseline_with_alias = {**baseline, "alias_map": {"status": "order_status"}}
    report2 = detect_schema_drift(baseline_with_alias, incoming_cols, incoming_types)
    print_change_report(report2)