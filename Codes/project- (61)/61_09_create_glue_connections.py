# file: 61_09_create_glue_connections.py
# Run from: Account B (222222222222)
# Purpose: Create Glue JDBC connection to Account A's MariaDB
#          AND Glue JDBC connection to Account B's Redshift
# Builds on: Project 31 (Glue JDBC Connection fundamentals)

import boto3

REGION = "us-east-2"

# Network config (Account B)
VPC_B = "vpc-0bbb2222bbbb2222b"
GLUE_SUBNET = "subnet-0bbb2222glue"          # 10.2.6.0/24
GLUE_SG = "sg-0bbb2222glueeni"               # Created in Part 4
AVAILABILITY_ZONE = "us-east-2a"

# MariaDB (Account A — reachable via VPC Peering)
MARIADB_HOST = "10.1.3.50"
MARIADB_PORT = "3306"
MARIADB_DB = "quickcart"
MARIADB_USER = "glue_reader"
MARIADB_PASS = "GlueR3ad0nly#2025"

# Redshift (Account B)
REDSHIFT_HOST = "quickcart-analytics-cluster.cdefghijk.us-east-2.redshift.amazonaws.com"
REDSHIFT_PORT = "5439"
REDSHIFT_DB = "analytics"
REDSHIFT_USER = "etl_writer"
REDSHIFT_PASS = "R3dsh1ftWr1t3r#2025"


def create_mariadb_connection():
    """
    Glue Connection to MariaDB in Account A
    
    HOW THIS WORKS INTERNALLY:
    → Glue creates an ENI in the specified subnet (10.2.6.0/24)
    → ENI gets a private IP (e.g., 10.2.6.10)
    → ENI uses the specified security group
    → Subnet's route table routes 10.1.0.0/16 → VPC Peering → Account A
    → Account A's SG allows 10.2.6.0/24 on port 3306
    → TCP handshake → MySQL protocol handshake → authenticated
    
    JDBC URL FORMAT for MariaDB:
    → jdbc:mysql://host:port/database
    → Note: "mysql" not "mariadb" — MariaDB uses MySQL protocol
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_connection(
            ConnectionInput={
                "Name": "conn-cross-account-mariadb",
                "Description": "Account A MariaDB via VPC Peering",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:mysql://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"
                    ),
                    "USERNAME": MARIADB_USER,
                    "PASSWORD": MARIADB_PASS,
                    # Optional: enforce SSL
                    # "JDBC_ENFORCE_SSL": "true"
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": GLUE_SUBNET,
                    "SecurityGroupIdList": [GLUE_SG],
                    "AvailabilityZone": AVAILABILITY_ZONE
                }
            }
        )
        print("✅ Glue Connection created: conn-cross-account-mariadb")
        print(f"   JDBC URL: jdbc:mysql://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}")
        print(f"   Subnet: {GLUE_SUBNET} (10.2.6.0/24)")
        print(f"   SG: {GLUE_SG}")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Connection already exists: conn-cross-account-mariadb")
        # Update if needed
        glue.update_connection(
            Name="conn-cross-account-mariadb",
            ConnectionInput={
                "Name": "conn-cross-account-mariadb",
                "Description": "Account A MariaDB via VPC Peering (updated)",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:mysql://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"
                    ),
                    "USERNAME": MARIADB_USER,
                    "PASSWORD": MARIADB_PASS,
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": GLUE_SUBNET,
                    "SecurityGroupIdList": [GLUE_SG],
                    "AvailabilityZone": AVAILABILITY_ZONE
                }
            }
        )
        print("ℹ️  Connection updated")


def create_redshift_connection():
    """
    Glue Connection to Redshift in Account B (same VPC)
    
    SIMPLER than MariaDB:
    → Same VPC — no peering needed
    → Local route handles 10.2.0.0/16
    → Just need SG to allow 5439
    
    Already covered in Project 32 — abbreviated here
    """
    glue = boto3.client("glue", region_name=REGION)

    try:
        glue.create_connection(
            ConnectionInput={
                "Name": "conn-redshift-analytics",
                "Description": "Account B Redshift analytics cluster",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": (
                        f"jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
                    ),
                    "USERNAME": REDSHIFT_USER,
                    "PASSWORD": REDSHIFT_PASS,
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": GLUE_SUBNET,
                    "SecurityGroupIdList": [GLUE_SG],
                    "AvailabilityZone": AVAILABILITY_ZONE
                }
            }
        )
        print("✅ Glue Connection created: conn-redshift-analytics")
        print(f"   JDBC URL: jdbc:redshift://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}")

    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Connection already exists: conn-redshift-analytics")


def test_connections():
    """
    Test both connections via Glue API
    
    IMPORTANT: This is asynchronous — returns immediately
    → Must poll for result
    → Glue spins up ENI → attempts JDBC handshake → reports pass/fail
    → Takes 1-3 minutes
    """
    glue = boto3.client("glue", region_name=REGION)

    for conn_name in ["conn-cross-account-mariadb", "conn-redshift-analytics"]:
        print(f"\n🧪 Testing connection: {conn_name}")
        try:
            # NOTE: TestConnection is not available in all SDK versions
            # Use Console: Glue → Connections → Test Connection
            # Or use this workaround: try creating a small crawler
            response = glue.get_connection(Name=conn_name)
            props = response["Connection"]["ConnectionProperties"]
            jdbc_url = props.get("JDBC_CONNECTION_URL", "N/A")
            print(f"   JDBC URL: {jdbc_url}")
            print(f"   Status: Connection definition valid")
            print(f"   → Test via Console: Glue → Connections → Test Connection")
        except Exception as e:
            print(f"   ❌ Error: {e}")


if __name__ == "__main__":
    print("=" * 60)
    print("📡 CREATING GLUE CONNECTIONS")
    print("=" * 60)

    create_mariadb_connection()
    print()
    create_redshift_connection()
    print()
    test_connections()