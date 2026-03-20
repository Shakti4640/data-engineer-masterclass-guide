# file: 04_create_users.py
# Purpose: Create separate users for different access patterns
# Same principle as MariaDB users (Project 8) — least privilege

import psycopg2

REDSHIFT_CONFIG = {
    "host": "quickcart-analytics.xxxxxxxxxxxx.us-east-2.redshift.amazonaws.com",
    "port": 5439,
    "dbname": "quickcart_dw",
    "user": "admin_user",
    "password": [REDACTED:PASSWORD]!"
}


def create_users():
    """
    Create role-based users for Redshift
    
    USER STRATEGY:
    → admin_user: superuser (cluster creation — already exists)
    → etl_loader: can INSERT, COPY, DELETE in analytics schema
    → analyst_ro: read-only SELECT on analytics schema
    → bi_service: read-only for BI tool connections (Tableau, Looker)
    
    WHY SEPARATE USERS:
    → ETL can't accidentally DROP tables
    → Analysts can't modify data
    → BI tool credentials are separate — rotate without affecting ETL
    → Audit trail: WHO ran WHAT query
    """
    conn = psycopg2.connect(**REDSHIFT_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("=" * 60)
    print("👤 CREATING REDSHIFT USERS & GROUPS")
    print("=" * 60)
    
    # --- CREATE GROUPS ---
    groups_sql = [
        """
        CREATE GROUP etl_group;
        """,
        """
        CREATE GROUP analyst_group;
        """,
        """
        CREATE GROUP bi_group;
        """
    ]
    
    for sql in groups_sql:
        try:
            cursor.execute(sql)
            print(f"✅ {sql.strip()}")
        except psycopg2.Error as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Group already exists")
            else:
                print(f"❌ {e}")
    
    # --- CREATE USERS ---
    users = [
        {
            "name": "etl_loader",
            "password": "EtlLoader_DW_2025!",
            "group": "etl_group",
            "purpose": "ETL jobs (Glue, Python scripts)"
        },
        {
            "name": "analyst_ro",
            "password":[REDACTED:PASSWORD]5!",
            "group": "analyst_group",
            "purpose": "Data analysts — read only"
        },
        {
            "name": "bi_service",
            "password": "BiService_DW_2025!",
            "group": "bi_group",
            "purpose": "BI tools (Tableau, Looker, QuickSight)"
        }
    ]
    
    for user in users:
        try:
            cursor.execute(f"""
                CREATE USER {user['name']} 
                PASSWORD '{user['password']}'
                IN GROUP {user['group']}
                NOCREATEDB NOCREATEUSER;
            """)
            print(f"\n✅ User '{user['name']}' created ({user['purpose']})")
        except psycopg2.Error as e:
            if "already exists" in str(e).lower():
                print(f"\nℹ️  User '{user['name']}' already exists")
            else:
                print(f"\n❌ Error creating {user['name']}: {e}")
    
    # --- GRANT PERMISSIONS ---
    print(f"\n🔑 Granting permissions...")
    
    grants = [
        # ETL group: full DML on analytics schema
        ("GRANT USAGE ON SCHEMA analytics TO GROUP etl_group",
         "etl_group: USAGE on analytics schema"),
        ("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics TO GROUP etl_group",
         "etl_group: SELECT, INSERT, UPDATE, DELETE on all tables"),
        ("ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP etl_group",
         "etl_group: default privileges for future tables"),
        
        # Analyst group: read-only on analytics schema
        ("GRANT USAGE ON SCHEMA analytics TO GROUP analyst_group",
         "analyst_group: USAGE on analytics schema"),
        ("GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO GROUP analyst_group",
         "analyst_group: SELECT on all tables"),
        ("ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO GROUP analyst_group",
         "analyst_group: default SELECT for future tables"),
        
        # BI group: read-only (same as analyst but separate credentials)
        ("GRANT USAGE ON SCHEMA analytics TO GROUP bi_group",
         "bi_group: USAGE on analytics schema"),
        ("GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO GROUP bi_group",
         "bi_group: SELECT on all tables"),
        ("ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO GROUP bi_group",
         "bi_group: default SELECT for future tables"),
    ]
    
    for sql, description in grants:
        try:
            cursor.execute(sql)
            print(f"   ✅ {description}")
        except psycopg2.Error as e:
            print(f"   ❌ {description}: {e}")
    
    # --- VERIFY ---
    print(f"\n🔍 Verification:")
    
    cursor.execute("""
        SELECT 
            usename AS username,
            usecreatedb AS can_create_db,
            usesuper AS is_superuser,
            groname AS group_name
        FROM pg_user u
        LEFT JOIN pg_group g ON u.usesysid = ANY(g.grolist)
        WHERE usename NOT LIKE 'rds%'
        ORDER BY usename
    """)
    
    columns = [desc[0] for desc in cursor.description]
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row))
        print(f"   {row_dict}")
    
    conn.close()
    print(f"\n🔌 Connection closed")
    
    print(f"""
{'='*60}
✅ USERS & PERMISSIONS CONFIGURED
{'='*60}

CONNECTION STRINGS:
─────────────────
ETL Jobs:
  user=etl_loader password=EtlLoader_DW_2025!

Analysts:
  user=analyst_ro password=Analyst_DW_2025!

BI Tools:
  user=bi_service password=BiService_DW_2025!

NOTE: In production, use AWS Secrets Manager (Project 70)
      to store and rotate these passwords automatically.
    """)


if __name__ == "__main__":
    create_users()