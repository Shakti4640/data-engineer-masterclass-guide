# file: 51_04_advanced_filter_patterns.py
# Purpose: Demonstrate all SNS filter policy operators

import boto3
import json

REGION = "us-east-2"


def demonstrate_filter_patterns():
    """
    Show all available filter policy patterns with examples
    
    Each pattern is a REAL use case — not just syntax demo
    """

    print("=" * 70)
    print("📚 SNS FILTER POLICY PATTERNS — COMPLETE REFERENCE")
    print("=" * 70)

    patterns = {
        # ═══════════════════════════════════════════════
        # PATTERN 1: Exact String Match (most common)
        # ═══════════════════════════════════════════════
        "1_exact_match": {
            "use_case": "ETL Queue receives ONLY etl_event messages",
            "filter_policy": {
                "event_type": ["etl_event"]
            },
            "matches": [
                {"event_type": "etl_event"},       # ✅ exact match
            ],
            "does_not_match": [
                {"event_type": "order_event"},     # ❌ different value
                {"event_type": "ETL_EVENT"},        # ❌ case sensitive!
                {},                                 # ❌ attribute missing
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 2: Multiple Values (OR within attribute)
        # ═══════════════════════════════════════════════
        "2_or_match": {
            "use_case": "Alert Queue receives alert OR data_quality events",
            "filter_policy": {
                "event_type": ["alert_event", "data_quality_event"]
            },
            "matches": [
                {"event_type": "alert_event"},          # ✅
                {"event_type": "data_quality_event"},    # ✅
            ],
            "does_not_match": [
                {"event_type": "etl_event"},             # ❌
                {"event_type": "order_event"},            # ❌
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 3: Multiple Attributes (AND between attributes)
        # ═══════════════════════════════════════════════
        "3_and_match": {
            "use_case": "Critical ETL failures only (type AND severity)",
            "filter_policy": {
                "event_type": ["etl_event"],
                "severity": ["critical"]
            },
            "matches": [
                {"event_type": "etl_event", "severity": "critical"},  # ✅ both match
            ],
            "does_not_match": [
                {"event_type": "etl_event", "severity": "info"},       # ❌ severity wrong
                {"event_type": "order_event", "severity": "critical"}, # ❌ type wrong
                {"event_type": "etl_event"},                            # ❌ severity missing
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 4: Prefix Match
        # ═══════════════════════════════════════════════
        "4_prefix_match": {
            "use_case": "Route all events from any 'etl_' source",
            "filter_policy": {
                "event_name": [{"prefix": "glue_"}]
            },
            "matches": [
                {"event_name": "glue_job_started"},    # ✅
                {"event_name": "glue_job_completed"},   # ✅
                {"event_name": "glue_crawler_done"},     # ✅
            ],
            "does_not_match": [
                {"event_name": "order_placed"},          # ❌
                {"event_name": "GLUE_job_started"},      # ❌ case sensitive
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 5: Anything-But (exclusion)
        # ═══════════════════════════════════════════════
        "5_anything_but": {
            "use_case": "Receive everything EXCEPT test/dev events",
            "filter_policy": {
                "environment": [{"anything-but": ["dev", "test"]}]
            },
            "matches": [
                {"environment": "prod"},       # ✅
                {"environment": "staging"},     # ✅
            ],
            "does_not_match": [
                {"environment": "dev"},         # ❌ excluded
                {"environment": "test"},        # ❌ excluded
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 6: Numeric Exact Match
        # ═══════════════════════════════════════════════
        "6_numeric_exact": {
            "use_case": "Route orders with exactly $0 total (free orders)",
            "filter_policy": {
                "order_total": [{"numeric": ["=", 0]}]
            },
            "note": "Attribute must be DataType=Number"
        },

        # ═══════════════════════════════════════════════
        # PATTERN 7: Numeric Range
        # ═══════════════════════════════════════════════
        "7_numeric_range": {
            "use_case": "High-value orders above $1000 for VIP processing",
            "filter_policy": {
                "order_total": [{"numeric": [">=", 1000]}]
            },
            "note": "Supports: =, >, >=, <, <= and combinations"
        },

        # ═══════════════════════════════════════════════
        # PATTERN 8: Numeric Between (range with bounds)
        # ═══════════════════════════════════════════════
        "8_numeric_between": {
            "use_case": "Mid-tier orders between $100 and $999",
            "filter_policy": {
                "order_total": [{"numeric": [">=", 100, "<", 1000]}]
            },
            "note": "Both bounds in single array"
        },

        # ═══════════════════════════════════════════════
        # PATTERN 9: Exists (attribute presence check)
        # ═══════════════════════════════════════════════
        "9_exists_true": {
            "use_case": "Route only messages that HAVE an error_code attribute",
            "filter_policy": {
                "error_code": [{"exists": True}]
            },
            "matches": [
                {"error_code": "ERR_001"},     # ✅ attribute present
                {"error_code": ""},            # ✅ present even if empty
            ],
            "does_not_match": [
                {},                             # ❌ attribute missing
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 10: Not Exists (attribute absence check)
        # ═══════════════════════════════════════════════
        "10_exists_false": {
            "use_case": "Route messages that DON'T have error_code (successes)",
            "filter_policy": {
                "error_code": [{"exists": False}]
            },
            "matches": [
                {},                             # ✅ no error_code
            ],
            "does_not_match": [
                {"error_code": "ERR_001"},     # ❌ attribute present
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 11: Anything-But with Prefix
        # ═══════════════════════════════════════════════
        "11_anything_but_prefix": {
            "use_case": "Exclude all internal test events starting with 'test_'",
            "filter_policy": {
                "event_name": [{"anything-but": {"prefix": "test_"}}]
            },
            "matches": [
                {"event_name": "order_placed"},       # ✅
                {"event_name": "glue_job_failed"},    # ✅
            ],
            "does_not_match": [
                {"event_name": "test_order"},          # ❌
                {"event_name": "test_etl_run"},        # ❌
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 12: Suffix Match (via $suffix — added 2023)
        # ═══════════════════════════════════════════════
        "12_suffix_match": {
            "use_case": "Route only events from .csv file arrivals",
            "filter_policy": {
                "file_extension": [{"suffix": ".csv"}]
            },
            "matches": [
                {"file_extension": "orders_20250115.csv"},    # ✅
            ],
            "does_not_match": [
                {"file_extension": "orders_20250115.parquet"}, # ❌
            ]
        },

        # ═══════════════════════════════════════════════
        # PATTERN 13: Complex Combo (real production policy)
        # ═══════════════════════════════════════════════
        "13_production_combo": {
            "use_case": "Critical ETL or DQ failures in production only",
            "filter_policy": {
                # AND between these two attributes
                "event_type": ["etl_event", "data_quality_event"],  # OR within
                "severity": ["critical"],                            # AND with above
                "environment": [{"anything-but": ["dev", "test"]}]  # AND exclude dev/test
            },
            "matches": [
                {
                    "event_type": "etl_event",
                    "severity": "critical",
                    "environment": "prod"
                },  # ✅ all three match
                {
                    "event_type": "data_quality_event",
                    "severity": "critical",
                    "environment": "staging"
                },  # ✅ all three match
            ],
            "does_not_match": [
                {
                    "event_type": "etl_event",
                    "severity": "info",
                    "environment": "prod"
                },  # ❌ severity wrong
                {
                    "event_type": "etl_event",
                    "severity": "critical",
                    "environment": "dev"
                },  # ❌ environment excluded
                {
                    "event_type": "order_event",
                    "severity": "critical",
                    "environment": "prod"
                },  # ❌ event_type wrong
            ]
        }
    }

    # --- PRINT ALL PATTERNS ---
    for pattern_key, pattern_info in patterns.items():
        print(f"\n{'─' * 60}")
        print(f"  📌 {pattern_key.upper()}")
        print(f"  Use Case: {pattern_info['use_case']}")
        print(f"  Filter Policy:")
        print(f"    {json.dumps(pattern_info['filter_policy'], indent=6)}")

        if "matches" in pattern_info:
            print(f"  ✅ Matches:")
            for m in pattern_info["matches"]:
                print(f"      {m}")

        if "does_not_match" in pattern_info:
            print(f"  ❌ Does NOT match:")
            for m in pattern_info["does_not_match"]:
                print(f"      {m}")

        if "note" in pattern_info:
            print(f"  📝 Note: {pattern_info['note']}")

    # --- SUMMARY TABLE ---
    print("\n" + "=" * 70)
    print("📊 FILTER OPERATOR CHEAT SHEET")
    print("=" * 70)
    print(f"{'Operator':<25} {'Syntax':<35} {'Logic'}")
    print("-" * 70)
    operators = [
        ("Exact match",        '["value"]',                    "value == X"),
        ("OR match",           '["val1", "val2"]',             "value IN [list]"),
        ("AND (multi-attr)",   '{"a":["x"], "b":["y"]}',      "a==x AND b==y"),
        ("Prefix",             '[{"prefix": "str"}]',          "value.startsWith()"),
        ("Suffix",             '[{"suffix": "str"}]',          "value.endsWith()"),
        ("Anything-but",       '[{"anything-but": ["x"]}]',    "value NOT IN [list]"),
        ("Anything-but prefix",'[{"anything-but":{"prefix":"x"}}]', "NOT startsWith()"),
        ("Numeric =",          '[{"numeric": ["=", N]}]',      "value == N"),
        ("Numeric range",      '[{"numeric": [">=",A,"<",B]}]',"A <= value < B"),
        ("Exists",             '[{"exists": true}]',           "attribute present"),
        ("Not exists",         '[{"exists": false}]',          "attribute absent"),
    ]
    for op, syntax, logic in operators:
        print(f"  {op:<25} {syntax:<35} {logic}")

    print("=" * 70)


if __name__ == "__main__":
    demonstrate_filter_patterns()