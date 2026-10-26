# Databricks notebook source
# MAGIC %md
# MAGIC # 02_silver_transform: Ratecard Struct Unnesting & Tier Explosion
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Silver (Curated / Metastore Catalog Tables)  
# MAGIC
# MAGIC ### Overview:
# MAGIC 1. Ingests raw ratecard batches landed from OLTP into ADLS Gen2 Bronze container.
# MAGIC 2. Parses serialized JSON struct strings into native Spark ArrayType(StructType).
# MAGIC 3. Unrolls / Explodes tiers into discrete interval records with surrogate hashing (`SHA-256`).
# MAGIC 4. Validates boundary conditions (`min_usage < max_usage`) and preserves unbounded tier sentinels (`999999999999.0`).
# MAGIC 5. Commits to Databricks Metastore Catalog tables:
# MAGIC    - `silver_ratecard_rules`
# MAGIC    - `silver_ratecard_tier_rules`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Parameters & Schema Definitions

# COMMAND ----------

import os
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DecimalType,
    IntegerType, BooleanType, ArrayType, TimestampType
)

# Widgets passed dynamically by ADF orchestrator
try:
    dbutils.widgets.text("execution_date", "", "Execution Date")
    dbutils.widgets.text("env", "dev", "Environment")
    execution_date = dbutils.widgets.get("execution_date")
    env = dbutils.widgets.get("env")
except Exception:
    execution_date = "2024-10-26"
    env = "dev"

print(f"Running Silver Transform | Env: {env} | Date: {execution_date}")

# Tier Struct Schema for parsing serialized JSON
TierPricingStructSchema = ArrayType(StructType([
    StructField("tier_number", IntegerType(), False),
    StructField("min_usage", DecimalType(18, 4), False),
    StructField("max_usage", DecimalType(18, 4), False),
    StructField("unit_price", DecimalType(18, 6), False),
    StructField("overage_price", DecimalType(18, 6), True),
    StructField("is_overage_applicable", BooleanType(), True)
]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Read Raw Parquet from ADLS Gen2 Bronze

# COMMAND ----------

# ADLS Gen2 Bronze landing path
STORAGE_ACCOUNT = "stlakehousedevsk01"
bronze_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/subscription_ratecard_rules"

try:
    df_bronze_raw = spark.read.parquet(bronze_path)
    print(f"Successfully read from ADLS Gen2 Bronze: {df_bronze_raw.count()} records.")
except Exception as e:
    print(f"Direct ABFSS read error: {e}. Trying DBFS / Staging fallback...")
    df_bronze_raw = spark.read.parquet("/mnt/bronze/subscription_ratecard_rules")

display(df_bronze_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Parse JSON String into Structs & Create Staging View

# COMMAND ----------

# Parse JSON text back into native Spark Array<Struct>
df_bronze_parsed = df_bronze_raw.withColumn(
    "tier_pricing_rules_parsed",
    F.from_json(F.col("tier_pricing_rules"), TierPricingStructSchema)
)

df_bronze_parsed.createOrReplaceTempView("vw_raw_bronze_ratecards")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify parsed structs
# MAGIC SELECT 
# MAGIC     pricing_rule_id,
# MAGIC     customer_id,
# MAGIC     part_number,
# MAGIC     subscription_id,
# MAGIC     subscription_line_id,
# MAGIC     date_start,
# MAGIC     date_end,
# MAGIC     size(tier_pricing_rules_parsed) AS tier_count,
# MAGIC     tier_pricing_rules_parsed
# MAGIC FROM vw_raw_bronze_ratecards
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 4: Explode Tiers & Data Quality Validation

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
# MAGIC LATERAL VIEW explode(tier_pricing_rules_parsed) AS tier;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data Quality Check: Verify no invalid intervals (min_usage >= max_usage)
# MAGIC SELECT 
# MAGIC     pricing_rule_id,
# MAGIC     tier_number,
# MAGIC     min_usage,
# MAGIC     max_usage
# MAGIC FROM vw_exploded_tiers_staged
# MAGIC WHERE min_usage >= max_usage AND max_usage != 999999999999.0;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Production Transformation & Surrogate Hashing

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
# MAGIC ### Cell 6: Persist as Managed Catalog Tables

# COMMAND ----------

# Save Rule Headers to Metastore
(
    df_silver_rules.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_ratecard_rules")
)
print(f"Successfully saved {df_silver_rules.count()} rows to catalog table: silver_ratecard_rules")

# Save Exploded Tiers to Metastore
(
    df_silver_tiers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_ratecard_tier_rules")
)
print(f"Successfully saved {df_silver_tiers.count()} rows to catalog table: silver_ratecard_tier_rules")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visual audit query
# MAGIC SELECT 
# MAGIC     r.pricing_rule_id,
# MAGIC     r.customer_id,
# MAGIC     r.part_number,
# MAGIC     t.tier_number,
# MAGIC     t.min_usage,
# MAGIC     t.max_usage,
# MAGIC     t.unit_price,
# MAGIC     t.overage_price
# MAGIC FROM silver_ratecard_rules r
# MAGIC JOIN silver_ratecard_tier_rules t ON r.pricing_rule_id = t.pricing_rule_id
# MAGIC ORDER BY r.pricing_rule_id, t.tier_number
# MAGIC LIMIT 10;
