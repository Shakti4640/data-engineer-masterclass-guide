# file: lineage/04_lineage_traversal.py
# Purpose: Query the lineage graph — trace any column to its root source
# This is the engine that answers: "Where did this column come from 5 hops ago?"

import boto3
import json
from collections import deque, OrderedDict
from datetime import datetime, timezone

REGION = "us-east-2"
LINEAGE_TABLE = "DataLineage"


class LineageTraverser:
    """
    Traverses the lineage graph stored in DynamoDB
    
    Supports two directions:
    → UPSTREAM (backward): "Where did this column come from?"
    → DOWNSTREAM (forward): "What does this column feed into?"
    
    Uses BFS (Breadth-First Search) for level-by-level traversal
    → Level 0: the starting node
    → Level 1: direct parents/children
    → Level 2: grandparents/grandchildren
    → ...up to max_depth
    
    WHY BFS over DFS:
    → BFS gives results by distance (closest ancestors first)
    → More intuitive for lineage: "show me immediate sources, then their sources"
    → DFS would dive deep into one branch before showing siblings
    """

    def __init__(self):
        self.dynamodb = boto3.resource("dynamodb", region_name=REGION)
        self.table = self.dynamodb.Table(LINEAGE_TABLE)

    def _get_node_metadata(self, node_id):
        """Fetch META record for a node"""
        response = self.table.get_item(
            Key={
                "PK": f"NODE#{node_id}",
                "SK": "META"
            }
        )
        return response.get("Item", {})

    def _get_edges(self, node_id, direction="UPSTREAM"):
        """
        Get all edges in specified direction for a node
        
        direction="UPSTREAM"   → who feeds INTO this node
        direction="DOWNSTREAM" → what this node feeds INTO
        
        Uses DynamoDB Query with begins_with on Sort Key
        → PK = NODE#{node_id}
        → SK begins_with "UPSTREAM#" or "DOWNSTREAM#"
        """
        response = self.table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(f"NODE#{node_id}") &
                boto3.dynamodb.conditions.Key("SK").begins_with(f"{direction}#")
            )
        )
        return response.get("Items", [])

    def trace_upstream(self, node_id, max_depth=10):
        """
        Trace full upstream lineage (backward) using BFS
        
        Returns ordered list of hops from target back to source(s)
        
        Example output for gold.finance_summary.total_revenue_adjusted:
        [
          {depth: 0, node: "gold.fin.total_rev_adj", sources: [...]},
          {depth: 1, node: "silver.om.sum_order_total", sources: [...]},
          {depth: 2, node: "silver.oc.order_total", sources: [...]},
          {depth: 3, node: "bronze.ro.qty", sources: [...]},
          {depth: 4, node: "mariadb.eco.orders.quantity", sources: []}  ← ROOT
        ]
        """
        visited = set()
        queue = deque()
        lineage_chain = []

        # Start node
        queue.append((node_id, 0))
        visited.add(node_id)

        while queue:
            current_id, depth = queue.popleft()

            if depth > max_depth:
                break

            # Get metadata for current node
            metadata = self._get_node_metadata(current_id)

            # Get upstream edges
            upstream_edges = self._get_edges(current_id, "UPSTREAM")

            hop = {
                "depth": depth,
                "node_id": current_id,
                "node_type": metadata.get("node_type", "UNKNOWN"),
                "service": metadata.get("service", "unknown"),
                "database": metadata.get("database", "unknown"),
                "table": metadata.get("table_name", "unknown"),
                "column": metadata.get("column_name", "N/A"),
                "data_type": metadata.get("data_type", "unknown"),
                "tags": metadata.get("tags", []),
                "upstream_count": len(upstream_edges),
                "upstream_edges": []
            }

            for edge in upstream_edges:
                source_id = edge.get("source_id", "")
                transform = edge.get("transform", "N/A")
                job_name = edge.get("job_name", "unknown")
                job_run_id = edge.get("job_run_id", "unknown")

                hop["upstream_edges"].append({
                    "source_id": source_id,
                    "transform": transform,
                    "job_name": job_name,
                    "job_run_id": job_run_id,
                    "edge_id": edge.get("edge_id", "")
                })

                # Enqueue unvisited upstream nodes
                if source_id not in visited:
                    visited.add(source_id)
                    queue.append((source_id, depth + 1))

            lineage_chain.append(hop)

        return lineage_chain

    def trace_downstream(self, node_id, max_depth=10):
        """
        Trace full downstream lineage (forward) using BFS
        
        Answers: "If I change/drop this column, what breaks?"
        
        Same algorithm as trace_upstream but follows DOWNSTREAM edges
        """
        visited = set()
        queue = deque()
        impact_chain = []

        queue.append((node_id, 0))
        visited.add(node_id)

        while queue:
            current_id, depth = queue.popleft()

            if depth > max_depth:
                break

            metadata = self._get_node_metadata(current_id)
            downstream_edges = self._get_edges(current_id, "DOWNSTREAM")

            hop = {
                "depth": depth,
                "node_id": current_id,
                "node_type": metadata.get("node_type", "UNKNOWN"),
                "service": metadata.get("service", "unknown"),
                "database": metadata.get("database", "unknown"),
                "table": metadata.get("table_name", "unknown"),
                "column": metadata.get("column_name", "N/A"),
                "downstream_count": len(downstream_edges),
                "downstream_edges": []
            }

            for edge in downstream_edges:
                target_id = edge.get("target_id", "")
                transform = edge.get("transform", "N/A")

                hop["downstream_edges"].append({
                    "target_id": target_id,
                    "transform": transform,
                    "job_name": edge.get("job_name", "unknown")
                })

                if target_id not in visited:
                    visited.add(target_id)
                    queue.append((target_id, depth + 1))

            impact_chain.append(hop)

        return impact_chain

    def get_full_lineage_tree(self, node_id, max_depth=10):
        """
        Get BOTH upstream and downstream lineage
        Returns complete bidirectional graph centered on the given node
        """
        upstream = self.trace_upstream(node_id, max_depth)
        downstream = self.trace_downstream(node_id, max_depth)

        return {
            "center_node": node_id,
            "queried_at": datetime.now(timezone.utc).isoformat(),
            "upstream": upstream,
            "downstream": downstream,
            "total_upstream_hops": max(
                (h["depth"] for h in upstream), default=0
            ),
            "total_downstream_hops": max(
                (h["depth"] for h in downstream), default=0
            )
        }

    def find_root_sources(self, node_id, max_depth=10):
        """
        Find all ROOT sources (nodes with no upstream)
        These are the original data origins: MariaDB tables, API endpoints, CSV files
        """
        lineage = self.trace_upstream(node_id, max_depth)
        roots = [
            hop for hop in lineage
            if hop["upstream_count"] == 0 and hop["depth"] > 0
        ]
        return roots

    def get_job_lineage(self, job_run_id):
        """
        Get all lineage edges produced by a specific job run
        Uses PK=JOB_RUN#{run_id} prefix
        """
        response = self.table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key("PK").eq(
                    f"JOB_RUN#{job_run_id}"
                ) &
                boto3.dynamodb.conditions.Key("SK").begins_with("EDGE#")
            )
        )
        return response.get("Items", [])


def format_lineage_chain(chain, direction="upstream"):
    """
    Pretty-print a lineage chain for human consumption
    
    Output format:
    ═══════════════════════════════════════════════════
    UPSTREAM LINEAGE: gold.finance_summary.total_revenue_adjusted
    ═══════════════════════════════════════════════════
    
    [Depth 0] gold.finance_summary.total_revenue_adjusted
      Type: COLUMN | Service: redshift | DataType: decimal(12,2)
      Tags: [financial, derived, gold-tier]
      ← Fed by 2 upstream source(s):
        ├── silver.order_metrics.sum_order_total
        │   Transform: {"type":"EXPRESSION","expr":"SUM(order_total)"}
        │   Job: job_aggregate_orders (run: jr_20250115_003)
        └── silver.order_metrics.sum_refund_amount
            Transform: {"type":"EXPRESSION","expr":"SUM(refund_amount)"}
            Job: job_aggregate_orders (run: jr_20250115_003)
    
    [Depth 1] silver.order_metrics.sum_order_total
      ...
    """
    arrow = "←" if direction == "upstream" else "→"
    edge_key = "upstream_edges" if direction == "upstream" else "downstream_edges"
    id_key = "source_id" if direction == "upstream" else "target_id"

    lines = []
    lines.append("=" * 70)
    if chain:
        lines.append(
            f"  {direction.upper()} LINEAGE: {chain[0]['node_id']}"
        )
    lines.append("=" * 70)

    for hop in chain:
        indent = "  " * hop["depth"]
        lines.append("")
        lines.append(
            f"{indent}[Depth {hop['depth']}] {hop['node_id']}"
        )
        lines.append(
            f"{indent}  Type: {hop['node_type']} | "
            f"Service: {hop['service']} | "
            f"DataType: {hop.get('data_type', 'N/A')}"
        )
        if hop.get("tags"):
            lines.append(f"{indent}  Tags: {hop['tags']}")

        edges = hop.get(edge_key, [])
        count_key = (
            "upstream_count" if direction == "upstream"
            else "downstream_count"
        )
        count = hop.get(count_key, 0)

        if count == 0:
            lines.append(f"{indent}  🌱 ROOT SOURCE (no further {direction})")
        else:
            lines.append(
                f"{indent}  {arrow} Fed by {count} {direction} source(s):"
            )
            for i, edge in enumerate(edges):
                connector = "└──" if i == len(edges) - 1 else "├──"
                lines.append(
                    f"{indent}    {connector} {edge[id_key]}"
                )

                # Parse transform for display
                try:
                    transform_obj = json.loads(edge.get("transform", "{}"))
                    transform_type = transform_obj.get("type", "UNKNOWN")
                    lines.append(
                        f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                        f"Transform: {transform_type}"
                    )
                    if "expression" in transform_obj:
                        lines.append(
                            f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                            f"Expression: {transform_obj['expression']}"
                        )
                    if "rename" in transform_obj:
                        r = transform_obj["rename"]
                        lines.append(
                            f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                            f"Rename: {r['from']} → {r['to']}"
                        )
                    if "filter" in transform_obj:
                        lines.append(
                            f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                            f"⚠️  Filter: {transform_obj['filter']}"
                        )
                except (json.JSONDecodeError, TypeError):
                    lines.append(
                        f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                        f"Transform: {edge.get('transform', 'N/A')}"
                    )

                lines.append(
                    f"{indent}    {'   ' if i == len(edges)-1 else '│  '} "
                    f"Job: {edge.get('job_name', 'N/A')}"
                )

    return "\n".join(lines)