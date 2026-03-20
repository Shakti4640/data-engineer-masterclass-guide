#!/bin/bash
# file: 31_04_setup_mariadb_user.sh
# Purpose: Create read-only user for Glue on MariaDB
# Run ON: The MariaDB EC2 instance (Project 8)

echo "Creating Glue reader user in MariaDB..."

mysql -u root -p <<'EOF'

-- ═══════════════════════════════════════════════════════════════
-- Create dedicated read-only user for Glue extraction
-- ═══════════════════════════════════════════════════════════════

-- '10.0.%' restricts connections to VPC CIDR only
-- Even if password leaks, cannot connect from outside VPC
CREATE USER IF NOT EXISTS 'glue_reader'@'10.0.%' 
    IDENTIFIED BY 'Gl!u3R3ad0nly2025';

-- Grant SELECT only — Glue should never write to source
GRANT SELECT ON quickcart_shop.* TO 'glue_reader'@'10.0.%';

-- Grant SHOW VIEW for view extraction (if needed)
GRANT SHOW VIEW ON quickcart_shop.* TO 'glue_reader'@'10.0.%';

-- IMPORTANT: Set resource limits to prevent Glue from overloading MariaDB
ALTER USER 'glue_reader'@'10.0.%' 
    WITH MAX_USER_CONNECTIONS 20     -- Max 20 concurrent connections
         MAX_QUERIES_PER_HOUR 0      -- Unlimited queries (within connection limit)
         MAX_UPDATES_PER_HOUR 0;     -- No updates allowed anyway

FLUSH PRIVILEGES;

-- Verify
SHOW GRANTS FOR 'glue_reader'@'10.0.%';

-- Verify table row counts (for post-extraction validation)
SELECT 'orders' as tbl, COUNT(*) as cnt FROM quickcart_shop.orders
UNION ALL
SELECT 'customers', COUNT(*) FROM quickcart_shop.customers
UNION ALL
SELECT 'products', COUNT(*) FROM quickcart_shop.products;

EOF

echo "Done! Glue reader user created."