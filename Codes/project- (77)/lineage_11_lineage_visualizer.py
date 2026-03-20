# file: lineage/11_lineage_visualizer.py
# Purpose: Render lineage graph as ASCII art + exportable JSON for UI tools

import json
from lineage_traversal import LineageTraverser


class LineageVisualizer:
    """
    Renders lineage as:
    → ASCII DAG (for terminal / interview whiteboard)
    → JSON graph (for D3.js / Graphviz / web UI)
    → DOT format (for Graphviz rendering)
    """

    def __init__(self):
        self.traverser = LineageTraverser()

    def render_ascii_dag(self, node_id, direction="upstream", max_depth=10):
        """
        Render lineage as ASCII directed acyclic graph
        
        Example output:
        
        🎯 gold.finance_summary.total_revenue_adjusted
        ├── 📊 silver.order_metrics.sum_order_total [SUM]
        │   └── 🔧 silver.orders_cleaned.order_total [qty*price]
        │       ├── 📦 bronze.raw_orders.qty [RENAME+CAST]
        │       │   └── 💾 mariadb.ecommerce.orders.quantity [ROOT]
        │       └── 📦 bronze.raw_orders.unit_price [CAST]
        │           └── 💾 mariadb.ecommerce.orders.unit_price [ROOT]
        └── 📊 silver.order_metrics.sum_refund_amount [SUM]
            └── 🔧 silver.refunds_processed.refund_amount [CAST]
                └── 📦 bronze.raw_refunds.refund_amount [JDBC]
                    └── 💾 mariadb.payments.refunds.refund_amount [ROOT]
        """
        if direction == "upstream":
            chain = self.traverser.trace_upstream(node_id, max_depth)
        else:
            chain = self.traverser.trace_downstream(node_id, max_depth)

        # Build adjacency map for tree rendering
        children_map = {}  # node_id → [child_node_ids]
        transform_map = {}  # (parent, child) → transform_type
        edge_key = "upstream_edges" if direction == "upstream" else "downstream_edges"
        id_key = "source_id" if direction == "upstream" else "target_id"

        for hop in chain:
            nid = hop["node_id"]
            children = []
            for edge in hop.get(edge_key, []):
                child_id = edge[id_key]
                children.append(child_id)
                try:
                    t = json.loads(edge.get("transform", "{}"))
                    transform_map[(nid, child_id)] = t.get("type", "?")
                except (json.JSONDecodeError, TypeError):
                    transform_map[(nid, child_id)] = "?"
            children_map[nid] = children

        # Service → icon mapping
        icons = {
            "mariadb": "💾",
            "glue": "🔧",
            "athena": "📊",
            "redshift": "🎯",
            "s3": "📦"
        }

        # Metadata lookup
        meta_map = {}
        for hop in chain:
            meta_map[hop["node_id"]] = hop

        lines = []

        def render_node(nid, prefix="", is_last=True):
            meta = meta_map.get(nid, {})
            service = meta.get("service", "unknown")
            icon = icons.get(service, "⚪")

            # Short display name (remove account prefix)
            short_name = nid.split(":", 2)[-1] if ":" in nid else nid

            connector = "└── " if is_last else "├── "
            lines.append(f"{prefix}{connector}{icon} {short_name}")

            children = children_map.get(nid, [])
            child_prefix = prefix + ("    " if is_last else "│   ")

            for i, child_id in enumerate(children):
                is_child_last = (i == len(children) - 1)
                t_type = transform_map.get((nid, child_id), "?")

                # Add transform annotation
                t_connector = "└── " if is_child_last else "├── "
                child_meta = meta_map.get(child_id, {})
                child_service = child_meta.get("service", "unknown")
                child_icon = icons.get(child_service, "⚪")
                child_short = child_id.split(":", 2)[-1] if ":" in child_id else child_id

                is_root = len(children_map.get(child_id, [])) == 0
                root_tag = " [ROOT]" if is_root else ""

                lines.append(
                    f"{child_prefix}{t_connector}"
                    f"{child_icon} {child_short} [{t_type}]{root_tag}"
                )

                # Recurse into child's children
                grandchildren = children_map.get(child_id, [])
                gc_prefix = child_prefix + (
                    "    " if is_child_last else "│   "
                )
                for j, gc_id in enumerate(grandchildren):
                    render_node(gc_id, gc_prefix, j == len(grandchildren) - 1)

        # Start rendering from root node
        root_meta = meta_map.get(node_id, {})
        root_service = root_meta.get("service", "unknown")
        root_icon = icons.get(root_service, "🎯")
        root_short = node_id.split(":", 2)[-1] if ":" in node_id else node_id
        lines.append(f"{root_icon} {root_short}")

        # Render first level of children
        root_children = children_map.get(node_id, [])
        for i, child_id in enumerate(root_children):
            is_last_child = (i == len(root_children) - 1)
            t_type = transform_map.get((node_id, child_id), "?")
            child_meta = meta_map.get(child_id, {})
            child_service = child_meta.get("service", "unknown")
            child_icon = icons.get(child_service, "⚪")
            child_short = child_id.split(":", 2)[-1] if ":" in child_id else child_id

            connector = "└── " if is_last_child else "├── "
            lines.append(f"{connector}{child_icon} {child_short} [{t_type}]")

            grandchildren = children_map.get(child_id, [])
            gc_prefix = "    " if is_last_child else "│   "
            for j, gc_id in enumerate(grandchildren):
                render_node(gc_id, gc_prefix, j == len(grandchildren) - 1)

        return "\n".join(lines)

    def export_json_graph(self, node_id, direction="upstream", max_depth=10):
        """
        Export lineage as JSON graph for web UI consumption
        
        Format compatible with D3.js force-directed graph:
        {
            "nodes": [{"id": "...", "service": "...", "group": 0}],
            "links": [{"source": "...", "target": "...", "transform": "..."}]
        }
        """
        if direction == "upstream":
            chain = self.traverser.trace_upstream(node_id, max_depth)
            edge_key = "upstream_edges"
            src_key = "source_id"
        else:
            chain = self.traverser.trace_downstream(node_id, max_depth)
            edge_key = "downstream_edges"
            src_key = "target_id"

        nodes = []
        links = []
        seen_nodes = set()

        # Service → color group mapping
        service_groups = {
            "mariadb": 0, "glue": 1, "athena": 2,
            "redshift": 3, "s3": 4
        }

        for hop in chain:
            nid = hop["node_id"]
            if nid not in seen_nodes:
                nodes.append({
                    "id": nid,
                    "short_name": nid.split(":", 2)[-1] if ":" in nid else nid,
                    "service": hop.get("service", "unknown"),
                    "database": hop.get("database", ""),
                    "table": hop.get("table", ""),
                    "column": hop.get("column", ""),
                    "data_type": hop.get("data_type", ""),
                    "tags": hop.get("tags", []),
                    "depth": hop["depth"],
                    "group": service_groups.get(
                        hop.get("service", ""), 5
                    )
                })
                seen_nodes.add(nid)

            for edge in hop.get(edge_key, []):
                other_id = edge[src_key]
                try:
                    transform = json.loads(edge.get("transform", "{}"))
                except (json.JSONDecodeError, TypeError):
                    transform = {"raw": edge.get("transform", "")}

                link = {
                    "source": other_id if direction == "upstream" else nid,
                    "target": nid if direction == "upstream" else other_id,
                    "transform_type": transform.get("type", "UNKNOWN"),
                    "transform_detail": transform,
                    "job_name": edge.get("job_name", ""),
                    "edge_id": edge.get("edge_id", "")
                }
                links.append(link)

        return {
            "center_node": node_id,
            "direction": direction,
            "nodes": nodes,
            "links": links,
            "total_nodes": len(nodes),
            "total_links": len(links)
        }

    def export_dot_format(self, node_id, direction="upstream", max_depth=10):
        """
        Export as Graphviz DOT format for professional diagram rendering
        
        Usage: 
        python -c "..." > lineage.dot
        dot -Tpng lineage.dot -o lineage.png
        """
        graph_data = self.export_json_graph(node_id, direction, max_depth)

        service_colors = {
            "mariadb": "#4479A1",
            "glue": "#FF9900",
            "athena": "#232F3E",
            "redshift": "#205B98",
            "s3": "#569A31"
        }

        lines = ["digraph DataLineage {"]
        lines.append('  rankdir=BT;')  # Bottom to top
        lines.append('  node [shape=box, style=filled, fontsize=10];')
        lines.append('  edge [fontsize=8];')
        lines.append("")

        # Nodes
        for node in graph_data["nodes"]:
            color = service_colors.get(node["service"], "#CCCCCC")
            label = node["short_name"].replace(".", "\\n")
            safe_id = node["id"].replace(":", "_").replace(".", "_")
            lines.append(
                f'  "{safe_id}" '
                f'[label="{label}", fillcolor="{color}", '
                f'fontcolor="white"];'
            )

        lines.append("")

        # Edges
        for link in graph_data["links"]:
            src_safe = link["source"].replace(":", "_").replace(".", "_")
            tgt_safe = link["target"].replace(":", "_").replace(".", "_")
            t_type = link.get("transform_type", "")
            lines.append(
                f'  "{src_safe}" -> "{tgt_safe}" '
                f'[label="{t_type}"];'
            )

        lines.append("}")
        return "\n".join(lines)


if __name__ == "__main__":
    viz = LineageVisualizer()

    target_node = (
        "333333333333:redshift:gold.finance_summary"
        ".total_revenue_adjusted"
    )

    # ASCII rendering
    print("\n📊 ASCII LINEAGE DAG:")
    print("=" * 70)
    print(viz.render_ascii_dag(target_node, "upstream", 10))

    # JSON export
    print("\n📊 JSON GRAPH EXPORT:")
    print("=" * 70)
    graph = viz.export_json_graph(target_node, "upstream", 10)
    print(json.dumps(graph, indent=2))

    # DOT export
    print("\n📊 GRAPHVIZ DOT FORMAT:")
    print("=" * 70)
    dot = viz.export_dot_format(target_node, "upstream", 10)
    print(dot)

    # Save DOT to file
    with open("/tmp/lineage.dot", "w") as f:
        f.write(dot)
    print("\n💾 DOT file saved to /tmp/lineage.dot")
    print("   Render: dot -Tpng /tmp/lineage.dot -o /tmp/lineage.png")