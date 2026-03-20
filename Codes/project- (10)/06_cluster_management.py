# file: 06_cluster_management.py
# Purpose: Pause, resume, resize, snapshot Redshift cluster
# Cost optimization: pause when not in use

import boto3
import time
from botocore.exceptions import ClientError

REGION = "us-east-2"
CLUSTER_ID = "quickcart-analytics"


def get_cluster_status():
    """Get current cluster state"""
    client = boto3.client("redshift", region_name=REGION)
    
    try:
        response = client.describe_clusters(ClusterIdentifier=CLUSTER_ID)
        cluster = response["Clusters"][0]
        
        print(f"\n📊 CLUSTER STATUS: {CLUSTER_ID}")
        print(f"   Status:        {cluster['ClusterStatus']}")
        print(f"   Node Type:     {cluster['NodeType']}")
        print(f"   Nodes:         {cluster['NumberOfNodes']}")
        print(f"   Encrypted:     {cluster['Encrypted']}")
        print(f"   VPC Routing:   {cluster['EnhancedVpcRouting']}")
        
        if cluster['ClusterStatus'] == 'available':
            endpoint = cluster['Endpoint']
            print(f"   Endpoint:      {endpoint['Address']}:{endpoint['Port']}")
        
        return cluster['ClusterStatus']
        
    except ClientError as e:
        print(f"❌ Error: {e}")
        return None


def pause_cluster():
    """
    Pause cluster to save money when not in use
    
    WHY:
    → dc2.large costs $0.25/hour = $6/day
    → Pause overnight (12 hours) = save $3/day = $90/month
    → Paused cluster: no compute charges
    → Resume takes 2-5 minutes
    
    WHEN:
    → No dashboards needed overnight
    → Weekend analytics not required
    → Development cluster after hours
    """
    client = boto3.client("redshift", region_name=REGION)
    
    print(f"\n⏸️  Pausing cluster: {CLUSTER_ID}")
    
    try:
        client.pause_cluster(ClusterIdentifier=CLUSTER_ID)
        print(f"   ✅ Pause initiated — cluster will be unavailable in ~1 minute")
        print(f"   💰 No compute charges while paused")
        print(f"   📌 Resume with: python 06_cluster_management.py resume")
    except ClientError as e:
        if "InvalidClusterState" in str(e):
            print(f"   ℹ️  Cluster cannot be paused in current state")
            print(f"   Current state: {get_cluster_status()}")
        else:
            print(f"   ❌ Error: {e}")


def resume_cluster():
    """Resume paused cluster"""
    client = boto3.client("redshift", region_name=REGION)
    
    print(f"\n▶️  Resuming cluster: {CLUSTER_ID}")
    
    try:
        client.resume_cluster(ClusterIdentifier=CLUSTER_ID)
        print(f"   ✅ Resume initiated — cluster available in 2-5 minutes")
        
        # Wait for available
        print(f"   ⏳ Waiting for cluster...")
        waiter = client.get_waiter("cluster_available")
        waiter.wait(
            ClusterIdentifier=CLUSTER_ID,
            WaiterConfig={"Delay": 30, "MaxAttempts": 20}
        )
        print(f"   ✅ Cluster is AVAILABLE")
        get_cluster_status()
        
    except ClientError as e:
        print(f"   ❌ Error: {e}")


def create_snapshot():
    """
    Create manual snapshot for backup before changes
    
    WHY:
    → Before resize operations
    → Before schema changes
    → Before large data loads
    → Snapshots stored in S3 (managed by Redshift)
    → Can restore to new cluster in same or different region
    """
    client = boto3.client("redshift", region_name=REGION)
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    snapshot_id = f"{CLUSTER_ID}-manual-{timestamp}"
    
    print(f"\n📸 Creating snapshot: {snapshot_id}")
    
    try:
        client.create_cluster_snapshot(
            SnapshotIdentifier=snapshot_id,
            ClusterIdentifier=CLUSTER_ID,
            ManualSnapshotRetentionPeriod=30,       # Keep for 30 days
            Tags=[
                {"Key": "Type", "Value": "Manual"},
                {"Key": "Reason", "Value": "Pre-change backup"}
            ]
        )
        print(f"   ✅ Snapshot creation initiated")
        print(f"   Snapshot ID: {snapshot_id}")
        print(f"   Retention: 30 days")
        
    except ClientError as e:
        print(f"   ❌ Error: {e}")


def resize_cluster(target_nodes=2):
    """
    Resize from 1 node to 2 nodes (or vice versa)
    
    CLASSIC RESIZE:
    → Creates new cluster with target size
    → Copies data from old to new
    → Takes 1-4 hours depending on data size
    → Old cluster read-only during resize
    
    ELASTIC RESIZE (preferred):
    → Adds/removes nodes in minutes
    → Brief unavailability (~10-15 minutes)
    → Data redistributed across new node count
    """
    client = boto3.client("redshift", region_name=REGION)
    
    print(f"\n📐 Resizing cluster to {target_nodes} nodes...")
    
    try:
        client.resize_cluster(
            ClusterIdentifier=CLUSTER_ID,
            ClusterType="multi-node" if target_nodes > 1 else "single-node",
            NodeType="dc2.large",
            NumberOfNodes=target_nodes
        )
        print(f"   ✅ Elastic resize initiated")
        print(f"   Target: {target_nodes} × dc2.large")
        print(f"   Expected time: 10-15 minutes")
        print(f"   Cost impact: ${target_nodes * 0.25:.2f}/hour = ${target_nodes * 183:.0f}/month")
        
    except ClientError as e:
        if "InvalidClusterState" in str(e):
            print(f"   ℹ️  Cannot resize in current state")
        elif "elastic resize is not available" in str(e).lower():
            print(f"   ℹ️  Elastic resize not available — will use classic resize")
        else:
            print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        get_cluster_status()
        print(f"\nUsage: python 06_cluster_management.py [status|pause|resume|snapshot|resize]")
        sys.exit(0)
    
    action = sys.argv[1].lower()
    
    if action == "status":
        get_cluster_status()
    elif action == "pause":
        pause_cluster()
    elif action == "resume":
        resume_cluster()
    elif action == "snapshot":
        create_snapshot()
    elif action == "resize":
        target = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        resize_cluster(target)
    else:
        print(f"Unknown action: {action}")