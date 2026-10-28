-- ==============================================================================
-- Unity Catalog Role-Based Access Control (RBAC) & Governance Grants
-- Engine: Databricks Unity Catalog
-- Hierarchy: Catalog -> Schema -> Tables
-- ==============================================================================

-- 1. Catalog Level Inheritance
GRANT USE CATALOG ON CATALOG `${catalog_name}` TO `account users`;

-- 2. Schema Level Inheritance (All current and future tables inherit these permissions)
GRANT USE SCHEMA ON SCHEMA `${catalog_name}`.`${schema_name}` TO `account users`;
GRANT SELECT ON SCHEMA `${catalog_name}`.`${schema_name}` TO `account users`;

-- 3. Explicit Table Grants for Silver Cleansed Tables
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.silver_ratecard_rules TO `account users`;
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.silver_ratecard_tier_rules TO `account users`;

-- 4. Explicit Table Grants for Gold Star Schema Tables
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.dim_customer TO `account users`;
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.dim_part TO `account users`;
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.dim_pricing_rule TO `account users`;
GRANT SELECT ON TABLE `${catalog_name}`.`${schema_name}`.dim_ratecard_tiers TO `account users`;

