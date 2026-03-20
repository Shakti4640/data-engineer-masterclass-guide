# file: lineage/02_lineage_emitter.py
# Purpose: Library that Glue ETL jobs import to emit lineage events

import boto3
import json
import uuid
import logging
from datetime import datetime, timezone
from botocore.exceptions import ClientError

logger = logging.getLogger("lineage")

REGION = "us-east-2"
LINEAGE_TABLE = "DataLineage"
LINEAGE_BUCKET = "quickcart-lineage-archive-prod"


class LineageNode:
    """Represents a data asset (table or column)"""

    def __init__(self, account_id, service, database, table, column=None,
                 data_type=None, tags=None):
        self.account_id = account_id
        self.service = service          # "glue", "redshift", "mariadb", "athena"
        self.database = database
        self.table = table
        self.column = column            # None for table-level nodes
        self.data_type = data_type
        self.tags = tags or []

    @property
    def node_id(self):
        """
        Unique identifier for this data asset
        Format: {account}:{service}:{database}.{table}[.{column}]
        """
        base = f"{self.account_id}:{self.service}:{self.database}.{self.table}"
        if self.column:
            base += f".{self.column}"
        return base

    @property
    def asset_path(self):
        """For GSI1 — queryable path without account prefix"""
        base = f"{self.service}#{self.database}#{self.table}"
        if self.column:
            base += f"#{self.column}"
        return base

    def to_dict(self):
        return {
            "node_id": self.node_id,
            "account_id": self.account_id,
            "service": self.service,
            "database": self.database,
            "table": self.table,
            "column": self.column,
            "data_type": self.data_type,
            "tags": self.tags
        }


class LineageEdge:
    """Represents a transformation relationship between two nodes"""

    def __init__(self, source_node, target_node, transform_description,
                 job_name, job_run_id):
        self.edge_id = f"e-{uuid.uuid4().hex[:12]}"
        self.source = source_node
        self.target = target_node
        self.transform = transform_description
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.created_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "edge_id": self.edge_id,
            "source_id": self.source.node_id,
            "target_id": self.target.node_id,
            "transform": self.transform,
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "created_at": self.created_at
        }


class LineageEmitter:
    """
    Main lineage emission engine
    
    Used inside Glue ETL jobs to record:
    → What data was read (input nodes)
    → What transformations were applied
    → What data was written (output nodes)
    → Which job run created these relationships
    
    DESIGN PRINCIPLES:
    → Fire-and-forget: never block the ETL job
    → Idempotent: re-running same job overwrites same edges
    → Immutable archive: S3 events never deleted
    """

    def __init__(self, job_name, job_run_id, account_id):
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.account_id = account_id
        self.dynamodb = boto3.resource("dynamodb", region_name=REGION)
        self.table = self.dynamodb.Table(LINEAGE_TABLE)
        self.s3 = boto3.client("s3", region_name=REGION)
        self.edges_emitted = []
        self.nodes_registered = set()

    def register_node(self, node):
        """
        Register a data asset in the lineage graph
        Writes META record for the node
        """
        if node.node_id in self.nodes_registered:
            return  # Already registered in this session

        try:
            self.table.put_item(
                Item={
                    "PK": f"NODE#{node.node_id}",
                    "SK": "META",
                    "account_id": node.account_id,
                    "asset_path": node.asset_path,
                    "job_name": self.job_name,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "node_type": "COLUMN" if node.column else "TABLE",
                    "service": node.service,
                    "database": node.database,
                    "table_name": node.table,
                    "column_name": node.column or "N/A",
                    "data_type": node.data_type or "unknown",
                    "tags": node.tags
                }
            )
            self.nodes_registered.add(node.node_id)
            logger.info(f"Registered node: {node.node_id}")

        except ClientError as e:
            # Fire-and-forget: log but don't crash
            logger.warning(f"Failed to register node {node.node_id}: {e}")

    def emit_edge(self, source_node, target_node, transform_description):
        """
        Record a lineage relationship:
        source_node → [transform] → target_node
        
        Writes 3 DynamoDB items:
        1. UPSTREAM record on target (for backward traversal)
        2. DOWNSTREAM record on source (for forward traversal)
        3. EDGE record on job run (for audit)
        """
        # Ensure both nodes are registered
        self.register_node(source_node)
        self.register_node(target_node)

        edge = LineageEdge(
            source_node=source_node,
            target_node=target_node,
            transform_description=transform_description,
            job_name=self.job_name,
            job_run_id=self.job_run_id
        )

        try:
            # --- WRITE 1: Upstream edge on target node ---
            # "target was derived FROM source"
            self.table.put_item(
                Item={
                    "PK": f"NODE#{target_node.node_id}",
                    "SK": f"UPSTREAM#{source_node.node_id}",
                    "account_id": target_node.account_id,
                    "asset_path": target_node.asset_path,
                    "job_name": self.job_name,
                    "created_at": edge.created_at,
                    "edge_id": edge.edge_id,
                    "source_id": source_node.node_id,
                    "target_id": target_node.node_id,
                    "transform": transform_description,
                    "job_run_id": self.job_run_id
                }
            )

            # --- WRITE 2: Downstream edge on source node ---
            # "source feeds INTO target"
            self.table.put_item(
                Item={
                    "PK": f"NODE#{source_node.node_id}",
                    "SK": f"DOWNSTREAM#{target_node.node_id}",
                    "account_id": source_node.account_id,
                    "asset_path": source_node.asset_path,
                    "job_name": self.job_name,
                    "created_at": edge.created_at,
                    "edge_id": edge.edge_id,
                    "source_id": source_node.node_id,
                    "target_id": target_node.node_id,
                    "transform": transform_description,
                    "job_run_id": self.job_run_id
                }
            )

            # --- WRITE 3: Edge record under job run ---
            self.table.put_item(
                Item={
                    "PK": f"JOB_RUN#{self.job_run_id}",
                    "SK": f"EDGE#{edge.edge_id}",
                    "account_id": self.account_id,
                    "asset_path": "N/A",
                    "job_name": self.job_name,
                    "created_at": edge.created_at,
                    "source_id": source_node.node_id,
                    "target_id": target_node.node_id,
                    "transform": transform_description
                }
            )

            self.edges_emitted.append(edge)
            logger.info(
                f"Lineage edge: {source_node.node_id} → {target_node.node_id}"
            )

        except ClientError as e:
            # CRITICAL: never crash the ETL job due to lineage failure
            logger.warning(f"Failed to emit edge: {e}")

    def emit_column_mapping(self, source_node, target_node, mapping):
        """
        Convenience method for common column transformations
        
        mapping: dict describing the transformation
        Example: {"type": "RENAME", "from": "qty", "to": "quantity"}
        Example: {"type": "CAST", "from_type": "string", "to_type": "int"}
        Example: {"type": "EXPRESSION", "expr": "qty * unit_price"}
        """
        transform_str = json.dumps(mapping)
        self.emit_edge(source_node, target_node, transform_str)

    def archive_to_s3(self):
        """
        Write all emitted edges to S3 for historical archive
        Partitioned by date for Athena queryability
        """
        if not self.edges_emitted:
            logger.info("No edges to archive")
            return

        now = datetime.now(timezone.utc)
        partition_path = (
            f"events/year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}"
        )
        filename = f"{self.job_name}_{self.job_run_id}.json"
        s3_key = f"{partition_path}/{filename}"

        event_payload = {
            "job_name": self.job_name,
            "job_run_id": self.job_run_id,
            "account_id": self.account_id,
            "timestamp": now.isoformat(),
            "edges": [e.to_dict() for e in self.edges_emitted],
            "nodes": [
                {"node_id": nid} for nid in self.nodes_registered
            ]
        }

        try:
            self.s3.put_object(
                Bucket=LINEAGE_BUCKET,
                Key=s3_key,
                Body=json.dumps(event_payload, indent=2),
                ContentType="application/json",
                ServerSideEncryption="AES256"
            )
            logger.info(
                f"Archived {len(self.edges_emitted)} edges to "
                f"s3://{LINEAGE_BUCKET}/{s3_key}"
            )
        except ClientError as e:
            logger.warning(f"Failed to archive lineage to S3: {e}")

    def finalize(self):
        """Call at end of ETL job to flush all lineage"""
        self.archive_to_s3()
        logger.info(
            f"Lineage finalized: {len(self.nodes_registered)} nodes, "
            f"{len(self.edges_emitted)} edges"
        )