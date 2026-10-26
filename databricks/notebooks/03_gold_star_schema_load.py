# Databricks notebook source
# MAGIC %md
# MAGIC # 03_gold_star_schema_load: Conformed Dimension Lookups & Star Schema
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Gold (Dimensional Serving / Star Schema)  
# MAGIC
# MAGIC ### Overview:
# MAGIC 1. Ingests cleansed rule headers and exploded tiers from Silver Delta.
# MAGIC 2. Performs surrogate key lookups against pre-existing Conformed Dimensions:
# MAGIC    - `dim_customer` (`customer_id` -> `customer_key`)
# MAGIC    - `dim_part` (`part_number` -> `part_key`)
# MAGIC 3. Implements **Late-Arriving Dimension Handling**: Unmatched keys map to `-1 (UNKNOWN)`.
# MAGIC 4. Computes business validity flags (`is_current` based on `CURRENT_DATE BETWEEN date_start AND date_end`).
# MAGIC 5. Generates Gold OLAP tables:
# MAGIC    - `dim_pricing_rule` (Header dimension with surrogate keys)
# MAGIC    - `dim_ratecard_tiers` (Exploded tier outrigger dimension)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Pipeline Parameters (Widgets)

# COMMAND ----------

import os
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
    dbutils.widgets.text("env", "dev", "Environment (dev/qa/prod)")
    dbutils.widgets.text("storage_base_path", "./data/lakehouse", "Storage Base Path")
    
    execution_date = dbutils.widgets.get("execution_date")
    env = dbutils.widgets.get("env")
    storage_base_path = dbutils.widgets.get("storage_base_path")
except NameError:
    execution_date = os.getenv("EXECUTION_DATE", "2024-10-24")
    env = os.getenv("ENVIRONMENT", "dev")
    storage_base_path = os.getenv("STORAGE_BASE_PATH", "./data/lakehouse")

print(f"Executing Gold Dimensional Load | Env: {env} | Date: {execution_date}")

# Paths
silver_rules_path = f"{storage_base_path}/silver/ratecard_rules"
silver_tiers_path = f"{storage_base_path}/silver/ratecard_tier_rules"
gold_dim_cust_path = f"{storage_base_path}/gold/dim_customer"
gold_dim_part_path = f"{storage_base_path}/gold/dim_part"
gold_dim_pricing_rule_path = f"{storage_base_path}/gold/dim_pricing_rule"
gold_dim_ratecard_tiers_path = f"{storage_base_path}/gold/dim_ratecard_tiers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Load Silver Delta & Conformed Dimension Tables

# COMMAND ----------

# Read Silver Inputs
df_silver_rules = spark.read.format("delta").load(silver_rules_path)
df_silver_tiers = spark.read.format("delta").load(silver_tiers_path)

# Read Pre-existing Conformed Dimensions
df_dim_customer = spark.read.format("delta").load(gold_dim_cust_path)
df_dim_part = spark.read.format("delta").load(gold_dim_part_path)

print(f"Silver Rules: {df_silver_rules.count()} | Silver Tiers: {df_silver_tiers.count()}")
print(f"Conformed Customers: {df_dim_customer.count()} | Conformed Parts: {df_dim_part.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: SQL Exploration of Conformed Dimension Joins
# MAGIC *Interactive verification of customer and part lookups.*

# COMMAND ----------

df_silver_rules.createOrReplaceTempView("vw_silver_rules")
df_dim_customer.createOrReplaceTempView("vw_dim_customer")
df_dim_part.createOrReplaceTempView("vw_dim_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test conformed lookup in SQL with -1 UNKNOWN fallback
# MAGIC SELECT 
# MAGIC     r.pricing_rule_id,
# MAGIC     r.customer_id,
# MAGIC     COALESCE(c.customer_key, -1) AS customer_key,
# MAGIC     c.customer_name,
# MAGIC     r.part_number,
# MAGIC     COALESCE(p.part_key, -1) AS part_key,
# MAGIC     p.product_name,
# MAGIC     r.subscription_id,
# MAGIC     r.subscription_line_id,
# MAGIC     r.date_start,
# MAGIC     r.date_end,
# MAGIC     CASE 
# MAGIC         WHEN current_date() BETWEEN r.date_start AND r.date_end THEN TRUE 
# MAGIC         ELSE FALSE 
# MAGIC     END AS is_currently_active
# MAGIC FROM vw_silver_rules r
# MAGIC LEFT JOIN vw_dim_customer c ON r.customer_id = c.customer_id
# MAGIC LEFT JOIN vw_dim_part p ON r.part_number = p.part_number
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Production PySpark Dimensional Load

# COMMAND ----------

UNKNOWN_SK = -1

# 1. Lookup Customer Surrogate Key
df_with_cust = df_silver_rules.join(
    df_dim_customer.select("customer_id", "customer_key"),
    on="customer_id",
    how="left"
).fillna({"customer_key": UNKNOWN_SK})

# 2. Lookup Part Surrogate Key
df_with_dims = df_with_cust.join(
    df_dim_part.select("part_number", "part_key"),
    on="part_number",
    how="left"
).fillna({"part_key": UNKNOWN_SK})

# 3. Build Gold Pricing Rule Dimension
dim_pricing_rule = df_with_dims.select(
    F.sha2(F.col("pricing_rule_id"), 256).alias("pricing_rule_key"),
    F.col("pricing_rule_id"),
    F.col("customer_key"),
    F.col("part_key"),
    F.col("subscription_id"),
    F.col("subscription_line_id"),
    F.col("date_start"),
    F.col("date_end"),
    F.when(
        (F.col("date_start") <= F.current_date()) & (F.col("date_end") >= F.current_date()),
        F.lit(True)
    ).otherwise(F.lit(False)).alias("is_current"),
    F.current_timestamp().alias("dw_inserted_at")
)

# 4. Build Gold Ratecard Tier Outrigger Dimension
dim_ratecard_tiers = df_silver_tiers.join(
    dim_pricing_rule.select("pricing_rule_id", "pricing_rule_key"),
    on="pricing_rule_id",
    how="inner"
).select(
    F.col("tier_rule_id").alias("tier_rule_key"),
    F.col("pricing_rule_key"),
    F.col("pricing_rule_id"),
    F.col("tier_number"),
    F.col("min_usage"),
    F.col("max_usage"),
    F.col("unit_price"),
    F.col("overage_price"),
    F.col("is_overage_applicable"),
    F.current_timestamp().alias("dw_inserted_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Persist Gold Star Schema Delta Tables

# COMMAND ----------

# Save dim_pricing_rule
(
    dim_pricing_rule.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_pricing_rule_path)
)
print(f"Persisted dim_pricing_rule: {dim_pricing_rule.count()} rows -> {gold_dim_pricing_rule_path}")

# Save dim_ratecard_tiers
(
    dim_ratecard_tiers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_ratecard_tiers_path)
)
print(f"Persisted dim_ratecard_tiers: {dim_ratecard_tiers.count()} rows -> {gold_dim_ratecard_tiers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6: Dimensional Integrity Audit Query
# MAGIC *Verify no orphaned records and validate surrogate key resolution.*

# COMMAND ----------

dim_pricing_rule.createOrReplaceTempView("vw_dim_pricing_rule")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick audit summary for pipeline monitoring
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_rules,
# MAGIC     SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unmapped_customers,
# MAGIC     SUM(CASE WHEN part_key = -1 THEN 1 ELSE 0 END) AS unmapped_parts,
# MAGIC     SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS currently_active_rules
# MAGIC FROM vw_dim_pricing_rule;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: SQL Exploration of Conformed Dimension Joins
# MAGIC *Interactive verification of customer and part lookups.*

# COMMAND ----------

df_silver_rules.createOrReplaceTempView("vw_silver_rules")
df_dim_customer.createOrReplaceTempView("vw_dim_customer")
df_dim_part.createOrReplaceTempView("vw_dim_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test conformed lookup in SQL with -1 UNKNOWN fallback
# MAGIC SELECT 
# MAGIC     r.pricing_rule_id,
# MAGIC     r.customer_id,
# MAGIC     COALESCE(c.customer_key, -1) AS customer_key,
# MAGIC     c.customer_name,
# MAGIC     r.part_number,
# MAGIC     COALESCE(p.part_key, -1) AS part_key,
# MAGIC     p.product_name,
# MAGIC     r.subscription_id,
# MAGIC     r.subscription_line_id,
# MAGIC     r.date_start,
# MAGIC     r.date_end,
# MAGIC     CASE 
# MAGIC         WHEN current_date() BETWEEN r.date_start AND r.date_end THEN TRUE 
# MAGIC         ELSE FALSE 
# MAGIC     END AS is_currently_active
# MAGIC FROM vw_silver_rules r
# MAGIC LEFT JOIN vw_dim_customer c ON r.customer_id = c.customer_id
# MAGIC LEFT JOIN vw_dim_part p ON r.part_number = p.part_number
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Production PySpark Dimensional Load

# COMMAND ----------

UNKNOWN_SK = -1

# 1. Lookup Customer Surrogate Key
df_with_cust = df_silver_rules.join(
    df_dim_customer.select("customer_id", "customer_key"),
    on="customer_id",
    how="left"
).fillna({"customer_key": UNKNOWN_SK})

# 2. Lookup Part Surrogate Key
df_with_dims = df_with_cust.join(
    df_dim_part.select("part_number", "part_key"),
    on="part_number",
    how="left"
).fillna({"part_key": UNKNOWN_SK})

# 3. Build Gold Pricing Rule Dimension
dim_pricing_rule = df_with_dims.select(
    F.sha2(F.col("pricing_rule_id"), 256).alias("pricing_rule_key"),
    F.col("pricing_rule_id"),
    F.col("customer_key"),
    F.col("part_key"),
    F.col("subscription_id"),
    F.col("subscription_line_id"),
    F.col("date_start"),
    F.col("date_end"),
    F.when(
        (F.col("date_start") <= F.current_date()) & (F.col("date_end") >= F.current_date()),
        F.lit(True)
    ).otherwise(F.lit(False)).alias("is_current"),
    F.current_timestamp().alias("dw_inserted_at")
)

# 4. Build Gold Ratecard Tier Outrigger Dimension
dim_ratecard_tiers = df_silver_tiers.join(
    dim_pricing_rule.select("pricing_rule_id", "pricing_rule_key"),
    on="pricing_rule_id",
    how="inner"
).select(
    F.col("tier_rule_id").alias("tier_rule_key"),
    F.col("pricing_rule_key"),
    F.col("pricing_rule_id"),
    F.col("tier_number"),
    F.col("min_usage"),
    F.col("max_usage"),
    F.col("unit_price"),
    F.col("overage_price"),
    F.col("is_overage_applicable"),
    F.current_timestamp().alias("dw_inserted_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Persist Gold Star Schema Delta Tables

# COMMAND ----------

# Save dim_pricing_rule
(
    dim_pricing_rule.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_pricing_rule_path)
)
print(f"Persisted dim_pricing_rule: {dim_pricing_rule.count()} rows -> {gold_dim_pricing_rule_path}")

# Save dim_ratecard_tiers
(
    dim_ratecard_tiers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(gold_dim_ratecard_tiers_path)
)
print(f"Persisted dim_ratecard_tiers: {dim_ratecard_tiers.count()} rows -> {gold_dim_ratecard_tiers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6: Dimensional Integrity Audit Query
# MAGIC *Verify no orphaned records and validate surrogate key resolution.*

# COMMAND ----------

dim_pricing_rule.createOrReplaceTempView("vw_dim_pricing_rule")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick audit summary for pipeline monitoring
# MAGIC SELECT 
# MAGIC     COUNT(*) AS total_rules,
# MAGIC     SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unmapped_customers,
# MAGIC     SUM(CASE WHEN part_key = -1 THEN 1 ELSE 0 END) AS unmapped_parts,
# MAGIC     SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS currently_active_rules
# MAGIC FROM vw_dim_pricing_rule;

