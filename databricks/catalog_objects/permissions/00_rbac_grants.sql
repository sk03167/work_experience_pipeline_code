-- ==============================================================================
-- Unity Catalog Role-Based Access Control (RBAC) & Governance Grants
-- Engine: Databricks Unity Catalog
-- Hierarchy: ${catalog_name} -> [silver, gold, control]
-- ==============================================================================

-- 1. Catalog Level Access
GRANT USE CATALOG ON CATALOG ${catalog_name} TO `account users`;

-- 2. Schema Level Inheritance (Inherited by all present & future tables)
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog_name}.silver TO `account users`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog_name}.gold TO `account users`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog_name}.control TO `account users`;

-- 3. Automated Data Factory Managed Identity Permissions (DML Read/Write)
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA ${catalog_name}.silver TO `account users`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA ${catalog_name}.gold TO `account users`;
GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA ${catalog_name}.control TO `account users`;
