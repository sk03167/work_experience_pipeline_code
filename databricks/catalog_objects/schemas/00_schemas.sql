-- ==============================================================================
-- Medallion & Governance Schemas Definition
-- Engine: Databricks Unity Catalog
-- Hierarchy: ${catalog_name}.<schema>
-- ==============================================================================

-- 1. Silver Layer Schema (Cleansed, Structured & Exploded Tiers)
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.silver
COMMENT 'Silver Layer: Curated, structured, and exploded tier tables';

-- 2. Gold Layer Schema (Dimensional Serving / Star Schema)
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.gold
COMMENT 'Gold Layer: Business dimensional model, star schema, and aggregated metrics';

-- 3. Control & Metadata Schema (Audits & Incremental Watermarks)
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.control
COMMENT 'Control Layer: Pipeline execution logs, ACID watermarks, and data quality metrics';

