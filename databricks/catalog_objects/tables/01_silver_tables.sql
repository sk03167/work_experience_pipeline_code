-- ==============================================================================
-- Silver Layer Table Definitions (Cleansed, Structured & Exploded Tiers)
-- Engine: Databricks Unity Catalog / Delta Lake
-- Target: ${catalog_name}.silver.<table>
-- ==============================================================================

-- 1. Cleansed Ratecard Rules Header Table
CREATE TABLE IF NOT EXISTS ${catalog_name}.silver.silver_ratecard_rules (
    pricing_rule_id STRING NOT NULL COMMENT 'Natural primary key from OLTP pricing rule',
    customer_id STRING NOT NULL COMMENT 'Customer identifier for dimensional joins',
    part_number STRING NOT NULL COMMENT 'Oracle/ERP Product part number',
    subscription_id STRING COMMENT 'Upstream subscription contract ID',
    subscription_line_id STRING COMMENT 'Specific line item within subscription',
    date_start DATE NOT NULL COMMENT 'Effective start date of ratecard rule',
    date_end DATE NOT NULL COMMENT 'Effective end date of ratecard rule',
    updated_at TIMESTAMP COMMENT 'Source OLTP last update timestamp for watermark tracking'
)
USING DELTA
COMMENT 'Silver Layer: Cleansed and deduplicated subscription ratecard headers';

-- 2. Exploded Ratecard Tier Rules Table
CREATE TABLE IF NOT EXISTS ${catalog_name}.silver.silver_ratecard_tier_rules (
    tier_rule_id STRING NOT NULL COMMENT 'Surrogate SHA-256 hash (pricing_rule_id || tier_number)',
    pricing_rule_id STRING NOT NULL COMMENT 'Foreign key to silver_ratecard_rules',
    tier_number INT NOT NULL COMMENT 'Sequential tier index (1, 2, 3...)',
    min_usage DECIMAL(18, 4) NOT NULL COMMENT 'Inclusive lower bound of tier usage volume',
    max_usage DECIMAL(18, 4) NOT NULL COMMENT 'Upper bound of tier volume (999999999999.0 for unbounded final tier)',
    unit_price DECIMAL(18, 6) NOT NULL COMMENT 'Unit rate within this usage boundary',
    overage_price DECIMAL(18, 6) COMMENT 'Rate applied when usage exceeds tier bound if applicable',
    is_overage_applicable BOOLEAN COMMENT 'Flag indicating if overage pricing applies',
    silver_processed_at TIMESTAMP COMMENT 'Databricks ingestion timestamp'
)
USING DELTA
COMMENT 'Silver Layer: Exploded and normalized volume pricing tier rules';
