-- ==============================================================================
-- Unity Catalog Creation DDL
-- Engine: Databricks Unity Catalog
-- Target: ${catalog_name}
-- ==============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog_name}
COMMENT 'Enterprise Lakehouse Catalog for Subscription Ratecard Pipeline (${env})';

