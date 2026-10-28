-- ==============================================================================
-- Gold Layer Table Definitions (Dimensional Serving / Star Schema)
-- Engine: Databricks Unity Catalog / Delta Lake
-- Target: ${catalog_name}.gold.<table>
-- ==============================================================================

-- 1. Conformed Dimension: Customer
CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.dim_customer (
    customer_key INT NOT NULL COMMENT 'Surrogate dimensional primary key (-1 represents Unknown)',
    customer_id STRING NOT NULL COMMENT 'Business natural key',
    customer_name STRING COMMENT 'Full customer organization name',
    customer_tier STRING COMMENT 'Enterprise / Mid-Market / Standard tier',
    billing_currency STRING COMMENT 'ISO currency code (USD, EUR, etc.)'
)
USING DELTA
COMMENT 'Gold Layer: Conformed Customer dimension for cross-domain analytics';

-- 2. Conformed Dimension: Product Part
CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.dim_part (
    part_key INT NOT NULL COMMENT 'Surrogate dimensional primary key (-1 represents Unknown)',
    part_number STRING NOT NULL COMMENT 'Oracle ERP part number',
    product_name STRING COMMENT 'Commercial product name',
    uom STRING COMMENT 'Unit of measure (GB-Month, API-Calls, Seats)',
    service_category STRING COMMENT 'Cloud Compute / Storage / Database service category'
)
USING DELTA
COMMENT 'Gold Layer: Conformed Product/Part dimension for rating and billing';

-- 3. Dimension: Pricing Rule (Header)
CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.dim_pricing_rule (
    pricing_rule_key STRING NOT NULL COMMENT 'Surrogate key (SHA-256 hash)',
    pricing_rule_id STRING NOT NULL COMMENT 'Source pricing rule identifier',
    customer_key INT NOT NULL COMMENT 'Foreign key to dim_customer (-1 fallback for late-arriving dimensions)',
    part_key INT NOT NULL COMMENT 'Foreign key to dim_part (-1 fallback for unmapped parts)',
    subscription_id STRING COMMENT 'Subscription contract reference',
    subscription_line_id STRING COMMENT 'Subscription line item reference',
    date_start DATE NOT NULL COMMENT 'Rule validity start date',
    date_end DATE NOT NULL COMMENT 'Rule validity end date',
    is_current BOOLEAN NOT NULL COMMENT 'Dynamic SCD flag: CURRENT_DATE BETWEEN date_start AND date_end',
    gold_created_at TIMESTAMP COMMENT 'ETL ingestion audit timestamp'
)
USING DELTA
COMMENT 'Gold Layer: Pricing Rule Dimension with conformed dimension lookups';

-- 4. Outrigger Dimension: Ratecard Tiers
CREATE TABLE IF NOT EXISTS ${catalog_name}.gold.dim_ratecard_tiers (
    tier_rule_key STRING NOT NULL COMMENT 'Unique surrogate tier identifier',
    pricing_rule_key STRING NOT NULL COMMENT 'Foreign key to dim_pricing_rule',
    pricing_rule_id STRING NOT NULL COMMENT 'Logical reference to pricing rule',
    tier_number INT NOT NULL COMMENT 'Tier sequence number',
    min_usage DECIMAL(18, 4) NOT NULL COMMENT 'Lower volume boundary',
    max_usage DECIMAL(18, 4) NOT NULL COMMENT 'Upper volume boundary (unbounded sentinel: 999999999999.0)',
    unit_price DECIMAL(18, 6) NOT NULL COMMENT 'Per-unit rate',
    overage_price DECIMAL(18, 6) COMMENT 'Overage unit rate',
    is_overage_applicable BOOLEAN COMMENT 'Overage applicability indicator',
    dw_inserted_at TIMESTAMP COMMENT 'Data warehouse insert timestamp'
)
USING DELTA
COMMENT 'Gold Layer: Exploded ratecard tier outrigger dimension for metered rating calculations';
