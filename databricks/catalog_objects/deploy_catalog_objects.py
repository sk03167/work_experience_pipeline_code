# Databricks notebook source
# MAGIC %md
# MAGIC # deploy_catalog_objects: Upfront Lakehouse Schema & RBAC Provisioning
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Purpose:** Automated DDL and RBAC schema migrations decoupled from ETL runtime pipelines.  
# MAGIC **Execution:** Run as part of CI/CD deployment or initial platform bootstrap.
# MAGIC
# MAGIC ### Objects Provisioned:
# MAGIC 1. **Silver Layer:** `silver_ratecard_rules`, `silver_ratecard_tier_rules`
# MAGIC 2. **Gold Layer:** `dim_customer`, `dim_part`, `dim_pricing_rule`, `dim_ratecard_tiers`
# MAGIC 3. **Governance:** Schema-level inheritance grants (`SELECT`, `USE SCHEMA`)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Environment Parameters

# COMMAND ----------

import os

try:
    dbutils.widgets.text("catalog_name", "adb_ratecard_dev_001", "Unity Catalog Name")
    dbutils.widgets.text("schema_name", "default", "Schema / Database Name")
    dbutils.widgets.text("env", "dev", "Environment")
    
    catalog_name = dbutils.widgets.get("catalog_name")
    schema_name = dbutils.widgets.get("schema_name")
    env = dbutils.widgets.get("env")
except Exception:
    catalog_name = "adb_ratecard_dev_001"
    schema_name = "default"
    env = "dev"

print(f"Deploying Catalog Objects | Catalog: {catalog_name} | Schema: {schema_name} | Env: {env}")

# Set current active namespace
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Deploy Silver Tables DDL

# COMMAND ----------

silver_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.silver_ratecard_rules (
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
"""
spark.sql(silver_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.silver_ratecard_rules")

silver_tiers_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.silver_ratecard_tier_rules (
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
"""
spark.sql(silver_tiers_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.silver_ratecard_tier_rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Deploy Gold Star Schema Tables DDL

# COMMAND ----------

dim_cust_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.dim_customer (
    customer_key INT NOT NULL COMMENT 'Surrogate dimensional primary key (-1 represents Unknown)',
    customer_id STRING NOT NULL COMMENT 'Business natural key',
    customer_name STRING COMMENT 'Full customer organization name',
    customer_tier STRING COMMENT 'Enterprise / Mid-Market / Standard tier',
    billing_currency STRING COMMENT 'ISO currency code (USD, EUR, etc.)'
)
USING DELTA
COMMENT 'Gold Layer: Conformed Customer dimension for cross-domain analytics';
"""
spark.sql(dim_cust_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.dim_customer")

dim_part_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.dim_part (
    part_key INT NOT NULL COMMENT 'Surrogate dimensional primary key (-1 represents Unknown)',
    part_number STRING NOT NULL COMMENT 'Oracle ERP part number',
    product_name STRING COMMENT 'Commercial product name',
    uom STRING COMMENT 'Unit of measure (GB-Month, API-Calls, Seats)',
    service_category STRING COMMENT 'Cloud Compute / Storage / Database service category'
)
USING DELTA
COMMENT 'Gold Layer: Conformed Product/Part dimension for rating and billing';
"""
spark.sql(dim_part_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.dim_part")

dim_pricing_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.dim_pricing_rule (
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
"""
spark.sql(dim_pricing_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.dim_pricing_rule")

dim_tiers_ddl = f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.dim_ratecard_tiers (
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
"""
spark.sql(dim_tiers_ddl)
print(f"Provisioned table: {catalog_name}.{schema_name}.dim_ratecard_tiers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Apply Schema-Level RBAC Inheritance Grants

# COMMAND ----------

# Grant catalog and schema-level inheritance
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `account users`")

print(f"Successfully applied schema-level RBAC grants on {catalog_name}.{schema_name} to `account users`")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify tables and privileges
# MAGIC SHOW TABLES IN ${catalog_name}.${schema_name};
