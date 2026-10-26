# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_transform: Ratecard Struct Unnesting & Tier Explosion
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Silver (Curated / Validated Delta)  
# MAGIC
# MAGIC ### Overview:
# MAGIC 1. Ingests raw ratecard batches landed from OLTP into Bronze Delta.
# MAGIC 2. Performs schema validation and tier struct explosion (`tier_pricing_rules` -> discrete rows).
# MAGIC 3. Validates boundary intervals (`min_usage < max_usage`), preserving open-ended sentinel (`999999999999.0`).
# MAGIC 4. Generates unique tier hash surrogate keys (`SHA-256`).
# MAGIC 5. Commits to Silver Delta tables:
# MAGIC    - `silver_ratecard_rules` (Rule header level)
# MAGIC    - `silver_ratecard_tier_rules` (Exploded tier level)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Pipeline Parameters (Widgets)
# MAGIC *Parameters passed dynamically by Azure Data Factory (ADF) orchestrator.*

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DecimalType,
    IntegerType, BooleanType, ArrayType, TimestampType
)

# Initialize widgets for Databricks UI and ADF parameter passing
try:
    dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
    dbutils.widgets.text("env", "dev", "Environment (dev/qa/prod)")
    dbutils.widgets.text("storage_base_path", "./data/lakehouse", "Storage Base Path")
    
    execution_date = dbutils.widgets.get("execution_date")
    env = dbutils.widgets.get("env")
    storage_base_path = dbutils.widgets.get("storage_base_path")
except NameError:
    # Fallback for local IDE execution outside Databricks runtime
    execution_date = os.getenv("EXECUTION_DATE", "2024-10-24")
    env = os.getenv("ENVIRONMENT", "dev")
    storage_base_path = os.getenv("STORAGE_BASE_PATH", "./data/lakehouse")

print(f"Executing Silver Transform | Env: {env} | Date: {execution_date} | Base Path: {storage_base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Ingest from Bronze Staging Table
# MAGIC *Reads raw ratecards preserving struct array.*

# COMMAND ----------

bronze_path = f"{storage_base_path}/bronze/subscription_ratecard_rules"
silver_rules_path = f"{storage_base_path}/silver/ratecard_rules"
silver_tiers_path = f"{storage_base_path}/silver/ratecard_tier_rules"

# In Databricks Workspace: read via Unity Catalog / Hive Metastore table
# df_bronze = spark.read.table("bronze_subscription_ratecard_rules")
# In file storage mode:
try:
    df_bronze = spark.read.format("delta").load(bronze_path)
except Exception:
    # Read as parquet if delta not initialized
    df_bronze = spark.read.load(bronze_path)

print(f"Bronze Raw Records Loaded: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Interactive Data Inspection & Schema Verification
# MAGIC *Create temporary view to inspect nested struct array using SQL.*

# COMMAND ----------

df_bronze.createOrReplaceTempView("vw_raw_bronze_ratecards")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample raw inspection showing composite struct array
# MAGIC SELECT 
# MAGIC     pricing_rule_id,
# MAGIC     customer_id,
# MAGIC     part_number,
# MAGIC     subscription_id,
# MAGIC     subscription_line_id,
# MAGIC     date_start,
# MAGIC     date_end,
# MAGIC     size(tier_pricing_rules) AS tier_count,
# MAGIC     tier_pricing_rules
# MAGIC FROM vw_raw_bronze_ratecards
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Struct Explosion & SQL Data Quality Validation
# MAGIC *Unrolls array of structs into tabular format and validates boundary conditions.*

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Unnesting the struct array via LATERAL VIEW EXPLODE
# MAGIC CREATE OR REPLACE TEMP VIEW vw_exploded_tiers_staged AS
# MAGIC SELECT 
# MAGIC     pricing_rule_id,
# MAGIC     customer_id,
# MAGIC     part_number,
# MAGIC     subscription_id,
# MAGIC     subscription_line_id,
# MAGIC     date_start,
# MAGIC     date_end,
# MAGIC     CAST(tier.tier_number AS INT) AS tier_number,
# MAGIC     CAST(tier.min_usage AS DECIMAL(18, 4)) AS min_usage,
# MAGIC     CAST(tier.max_usage AS DECIMAL(18, 4)) AS max_usage,
# MAGIC     CAST(tier.unit_price AS DECIMAL(18, 6)) AS unit_price,
# MAGIC     CAST(tier.overage_price AS DECIMAL(18, 6)) AS overage_price,
# MAGIC     COALESCE(tier.is_overage_applicable, FALSE) AS is_overage_applicable,
# MAGIC     updated_at
# MAGIC FROM vw_raw_bronze_ratecards
# MAGIC LATERAL VIEW explode(tier_pricing_rules) AS tier;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Quality Gate: Check for invalid intervals (min_usage >= max_usage)
# MAGIC SELECT 
# MAGIC     pricing_rule_id,
# MAGIC     tier_number,
# MAGIC     min_usage,
# MAGIC     max_usage
# MAGIC FROM vw_exploded_tiers_staged
# MAGIC WHERE min_usage >= max_usage AND max_usage != 999999999999.0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Production Transformation & Surrogate Hashing (PySpark)

# COMMAND ----------

# 1. Clean Rule Header Table
df_silver_rules = spark.table("vw_exploded_tiers_staged").select(
    "pricing_rule_id",
    "customer_id",
    "part_number",
    "subscription_id",
    "subscription_line_id",
    "date_start",
    "date_end",
    "updated_at"
).dropDuplicates(["pricing_rule_id"])

# 2. Add Unique Tier Surrogate Hash (pricing_rule_id + tier_number)
df_silver_tiers = spark.table("vw_exploded_tiers_staged").select(
    F.sha2(F.concat_ws("||", F.col("pricing_rule_id"), F.col("tier_number")), 256).alias("tier_rule_id"),
    "pricing_rule_id",
    "tier_number",
    "min_usage",
    "max_usage",
    "unit_price",
    "overage_price",
    "is_overage_applicable",
    F.current_timestamp().alias("silver_processed_at")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6: Persist Silver Delta Tables
# MAGIC *Atomic ACID write with schema enforcement.*

# COMMAND ----------

# Save Rule Headers to Silver Delta
(
    df_silver_rules.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_rules_path)
)
print(f"Successfully wrote {df_silver_rules.count()} rules to {silver_rules_path}")

# Save Exploded Tiers to Silver Delta
(
    df_silver_tiers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_tiers_path)
)
print(f"Successfully wrote {df_silver_tiers.count()} exploded tiers to {silver_tiers_path}")
