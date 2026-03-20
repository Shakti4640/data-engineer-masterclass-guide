# file: lineage/07_lineage_cli.py
# Purpose: Command-line tool to query lineage — used by data engineers for debugging
# This is the tool that solves the CFO's $1.6M discrepancy question

import sys
import json
import argparse
from datetime import datetime

# Reuse modules from this project
from lineage_traversal import LineageTraverser, format_lineage_chain
from lineage_emitter import LineageEmitter, LineageNode
from sql_lineage_parser import SQLLineageParser
from cloudtrail_lineage_collector import CloudTrailLineageCollector

ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"


def cmd_trace_upstream(args):
    """
    Trace column backward to its root sources
    
    Usage:
    python lineage_cli.py upstream \
        --node "222:glue:gold.finance_summary.total_revenue_adjusted" \
        --depth 10
    """
    traverser = LineageTraverser()
    chain = traverser.trace_upstream(args.node, max_depth=args.depth)

    print(format_lineage_chain(chain, direction="upstream"))

    # Find and highlight root sources
    roots = traverser.find_root_sources(args.node, max_depth=args.depth)
    if roots:
        print("\n" + "=" * 70)
        print("  🌱 ROOT SOURCES (original data origins):")
        print("=" * 70)
        for root in roots:
            print(f"  → {root['node_id']}")
            print(f"    Service: {root['service']}")
            print(f"    Type: {root.get('data_type', 'N/A')}")
            print(f"    Tags: {root.get('tags', [])}")

    return chain


def cmd_trace_downstream(args):
    """
    Impact analysis: what breaks if this column changes?
    
    Usage:
    python lineage_cli.py downstream \
        --node "111:mariadb:ecommerce.orders.quantity" \
        --depth 10
    """
    traverser = LineageTraverser()
    chain = traverser.trace_downstream(args.node, max_depth=args.depth)

    print(format_lineage_chain(chain, direction="downstream"))

    # Count total impacted assets
    total_impacted = len(chain) - 1  # Exclude starting node
    print(f"\n⚠️  IMPACT: {total_impacted} downstream assets affected")

    return chain


def cmd_full_lineage(args):
    """
    Bidirectional lineage: both upstream and downstream
    
    Usage:
    python lineage_cli.py full \
        --node "222:glue:silver.orders_cleaned.order_total"
    """
    traverser = LineageTraverser()
    result = traverser.get_full_lineage_tree(args.node, max_depth=args.depth)

    print("\n📊 FULL LINEAGE REPORT")
    print(f"   Center Node: {result['center_node']}")
    print(f"   Queried At: {result['queried_at']}")
    print(f"   Upstream Hops: {result['total_upstream_hops']}")
    print(f"   Downstream Hops: {result['total_downstream_hops']}")

    print("\n" + format_lineage_chain(result["upstream"], "upstream"))
    print("\n" + format_lineage_chain(result["downstream"], "downstream"))

    return result


def cmd_job_lineage(args):
    """
    Show all lineage edges from a specific job run
    
    Usage:
    python lineage_cli.py job-run --run-id "jr_2025011500001"
    """
    traverser = LineageTraverser()
    edges = traverser.get_job_lineage(args.run_id)

    print(f"\n📋 LINEAGE FOR JOB RUN: {args.run_id}")
    print("=" * 70)

    for edge in edges:
        print(f"  {edge.get('source_id', '?')}")
        print(f"    → {edge.get('target_id', '?')}")
        print(f"    Transform: {edge.get('transform', 'N/A')}")
        print()

    print(f"Total edges: {len(edges)}")
    return edges


def cmd_parse_view(args):
    """
    Extract lineage from an Athena/Redshift SQL view definition
    
    Usage:
    python lineage_cli.py parse-view \
        --sql "SELECT SUM(o.order_total) as sum_total FROM silver.orders_cleaned o"
    """
    parser = SQLLineageParser(account_id=ACCOUNT_ID)

    # If SQL provided directly
    if args.sql:
        sql = args.sql
    elif args.sql_file:
        with open(args.sql_file, "r") as f:
            sql = f.read()
    else:
        print("❌ Must provide --sql or --sql-file")
        return

    edges = parser.extract_lineage_from_view(
        database=args.database or "default",
        view_name=args.view_name or "unknown_view",
        sql=sql
    )

    print(f"\n📋 PARSED LINEAGE FROM SQL VIEW")
    print("=" * 70)

    for edge in edges:
        src = edge["source"]
        tgt = edge["target"]
        transform = edge["transform"]

        print(
            f"  {src['database']}.{src['table']}.{src['column']}"
        )
        print(
            f"    → [{transform['type']}] → "
            f"{tgt['database']}.{tgt['table']}.{tgt['column']}"
        )
        print(f"    Expression: {transform.get('expression', 'N/A')}")
        print()

    print(f"Total column mappings found: {len(edges)}")
    return edges


def cmd_investigate_discrepancy(args):
    """
    THE KILLER FEATURE: Investigate the CFO's $1.6M discrepancy
    
    Traces the suspicious column upstream AND checks for filters at each hop
    
    Usage:
    python lineage_cli.py investigate \
        --node "222:redshift:gold.finance_summary.total_revenue_adjusted"
    """
    print("\n🔍 DISCREPANCY INVESTIGATION")
    print("=" * 70)
    print(f"   Investigating: {args.node}")
    print("=" * 70)

    traverser = LineageTraverser()
    chain = traverser.trace_upstream(args.node, max_depth=10)

    suspects = []

    for hop in chain:
        for edge in hop.get("upstream_edges", []):
            transform_str = edge.get("transform", "{}")
            try:
                transform = json.loads(transform_str)
            except (json.JSONDecodeError, TypeError):
                transform = {"raw": transform_str}

            # CHECK 1: Does this hop have a FILTER?
            if "filter" in transform:
                suspects.append({
                    "hop_depth": hop["depth"],
                    "node": hop["node_id"],
                    "issue": "FILTER DETECTED",
                    "detail": transform["filter"],
                    "job": edge.get("job_name", "unknown"),
                    "severity": "HIGH"
                })

            # CHECK 2: Is this an aggregation that might lose rows?
            if transform.get("type") == "AGGREGATION":
                suspects.append({
                    "hop_depth": hop["depth"],
                    "node": hop["node_id"],
                    "issue": "AGGREGATION (potential row loss via GROUP BY)",
                    "detail": transform.get("expression", "N/A"),
                    "job": edge.get("job_name", "unknown"),
                    "severity": "MEDIUM"
                })

            # CHECK 3: Is there a COALESCE that replaces NULLs?
            if transform.get("type") == "COALESCE":
                suspects.append({
                    "hop_depth": hop["depth"],
                    "node": hop["node_id"],
                    "issue": "COALESCE (NULL values replaced — may hide missing data)",
                    "detail": transform.get("expression", "N/A"),
                    "job": edge.get("job_name", "unknown"),
                    "severity": "MEDIUM"
                })

            # CHECK 4: Is there a LEFT JOIN (may exclude unmatched rows)?
            if "LEFT JOIN" in str(transform).upper():
                suspects.append({
                    "hop_depth": hop["depth"],
                    "node": hop["node_id"],
                    "issue": "LEFT JOIN (unmatched rows get NULLs)",
                    "detail": str(transform),
                    "job": edge.get("job_name", "unknown"),
                    "severity": "LOW"
                })

    # REPORT SUSPECTS
    print(f"\n🚨 FOUND {len(suspects)} SUSPECT TRANSFORMATION(S):")
    print("-" * 70)

    for i, suspect in enumerate(suspects, 1):
        severity_icon = {
            "HIGH": "🔴",
            "MEDIUM": "🟡",
            "LOW": "🟢"
        }.get(suspect["severity"], "⚪")

        print(f"\n  {severity_icon} Suspect #{i} [{suspect['severity']}]")
        print(f"     At: {suspect['node']} (depth {suspect['hop_depth']})")
        print(f"     Issue: {suspect['issue']}")
        print(f"     Detail: {suspect['detail']}")
        print(f"     Job: {suspect['job']}")

    if suspects:
        high_suspects = [s for s in suspects if s["severity"] == "HIGH"]
        if high_suspects:
            print(f"\n⚡ RECOMMENDATION: Start investigation at:")
            for s in high_suspects:
                print(f"   → {s['node']} — {s['issue']}")
                print(f"     Check job '{s['job']}' filter logic")
    else:
        print("\n✅ No obvious filter/aggregation suspects found")
        print("   → Check for data quality issues at source (Project 68)")
        print("   → Check for CDC watermark issues (Project 65)")

    return suspects


def main():
    parser = argparse.ArgumentParser(
        description="Data Lineage CLI — Trace any column to its source"
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- upstream ---
    p_up = subparsers.add_parser(
        "upstream", help="Trace column backward to sources"
    )
    p_up.add_argument("--node", required=True, help="Node ID to trace")
    p_up.add_argument(
        "--depth", type=int, default=10, help="Max traversal depth"
    )

    # --- downstream ---
    p_down = subparsers.add_parser(
        "downstream", help="Impact analysis — what depends on this column"
    )
    p_down.add_argument("--node", required=True)
    p_down.add_argument("--depth", type=int, default=10)

    # --- full ---
    p_full = subparsers.add_parser(
        "full", help="Bidirectional lineage"
    )
    p_full.add_argument("--node", required=True)
    p_full.add_argument("--depth", type=int, default=10)

    # --- job-run ---
    p_job = subparsers.add_parser(
        "job-run", help="Lineage from a specific job run"
    )
    p_job.add_argument("--run-id", required=True)

    # --- parse-view ---
    p_view = subparsers.add_parser(
        "parse-view", help="Extract lineage from SQL view"
    )
    p_view.add_argument("--sql", help="SQL string")
    p_view.add_argument("--sql-file", help="Path to SQL file")
    p_view.add_argument("--database", default="default")
    p_view.add_argument("--view-name", default="unknown_view")

    # --- investigate ---
    p_inv = subparsers.add_parser(
        "investigate", help="Investigate data discrepancy"
    )
    p_inv.add_argument("--node", required=True)

    args = parser.parse_args()

    commands = {
        "upstream": cmd_trace_upstream,
        "downstream": cmd_trace_downstream,
        "full": cmd_full_lineage,
        "job-run": cmd_job_lineage,
        "parse-view": cmd_parse_view,
        "investigate": cmd_investigate_discrepancy
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()