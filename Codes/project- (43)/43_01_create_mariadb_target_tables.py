# file: 43_01_create_mariadb_target_tables.py
# Purpose: Create the target tables in MariaDB for reverse ETL
# These tables serve the mobile app

import pymysql

MARIADB_CONFIG = {
    "host": "10.0.1.50",
    "port": 3306,
    "user": "quickcart_app",
    "password": "[REDACTED:PASSWORD]",
    "database": "quickcart"
}


def create_target_tables():
    conn = pymysql.connect(**MARIADB_CONFIG)
    cursor = conn.cursor()

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TABLE 1: customer_metrics (UPSERT strategy)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_metrics (
            customer_id     VARCHAR(30)   PRIMARY KEY,
            lifetime_value  DECIMAL(12,2) NOT NULL DEFAULT 0.00,
            churn_risk      DECIMAL(5,4)  NOT NULL DEFAULT 0.0000
                            COMMENT 'Probability 0.0000 to 1.0000',
            segment         VARCHAR(20)   NOT NULL DEFAULT 'bronze'
                            COMMENT 'bronze/silver/gold/platinum',
            total_orders    INT           NOT NULL DEFAULT 0,
            avg_order_value DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            computed_at     TIMESTAMP     NOT NULL
                            COMMENT 'When Redshift computed these metrics',
            loaded_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
                            COMMENT 'When reverse ETL loaded this row',

            INDEX idx_segment (segment),
            INDEX idx_churn_risk (churn_risk),
            INDEX idx_computed_at (computed_at)
        ) ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
        COMMENT='Customer analytics metrics — reverse ETL from Redshift'
    """)
    print("✅ Created: customer_metrics")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TABLE 2: customer_recommendations (FULL REPLACE strategy)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_recommendations (
            customer_id     VARCHAR(30)   NOT NULL,
            product_id      VARCHAR(30)   NOT NULL,
            score           DECIMAL(5,4)  NOT NULL
                            COMMENT 'Recommendation score 0.0000 to 1.0000',
            rank            TINYINT       NOT NULL
                            COMMENT 'Rank 1-5 per customer',
            computed_at     TIMESTAMP     NOT NULL
                            COMMENT 'When Redshift computed this score',

            PRIMARY KEY (customer_id, rank),
            INDEX idx_customer_lookup (customer_id, rank),
            INDEX idx_product (product_id),
            INDEX idx_computed_at (computed_at)
        ) ENGINE=InnoDB
        DEFAULT CHARSET=utf8mb4
        COMMENT='Product recommendations — reverse ETL from Redshift (full replace)'
    """)
    print("✅ Created: customer_recommendations")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TABLE 3: Temp table for swap pattern (same schema as recommendations)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    cursor.execute("DROP TABLE IF EXISTS customer_recommendations_new")
    cursor.execute("""
        CREATE TABLE customer_recommendations_new
        LIKE customer_recommendations
    """)
    print("✅ Created: customer_recommendations_new (swap target)")

    conn.commit()

    # Verify
    cursor.execute("SHOW TABLES LIKE 'customer%'")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"\n📋 Customer tables: {tables}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    create_target_tables()