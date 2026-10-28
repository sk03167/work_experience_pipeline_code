# Databricks notebook source
# MAGIC %md
# MAGIC # 03_gold_star_schema_load: Conformed Dimension Lookups & Star Schema
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Gold (Dimensional Serving / Star Schema)  
# MAGIC **Namespace:** `${catalog_name}.gold.<table>`
# MAGIC
# MAGIC ### Overview:
# MAGIC 1. Ingests cleansed rule headers and exploded tiers from `${catalog_name}.silver`.
# MAGIC 2. Performs surrogate key lookups against pre-existing Conformed Dimensions in `${catalog_name}.gold`:
# MAGIC    - `dim_customer` (`customer_id` -> `customer_key`)
# MAGIC    - `dim_part` (`part_number` -> `part_key`)
# MAGIC 3. Implements **Late-Arriving Dimension Handling**: Unmatched keys map to `-1 (UNKNOWN)`.
# MAGIC 4. Computes business validity flags (`is_current` based on `CURRENT_DATE BETWEEN date_start AND date_end`).
# MAGIC 5. Generates Gold OLAP tables:
# MAGIC    - `${catalog_name}.gold.dim_pricing_rule` (Header dimension with surrogate keys)
# MAGIC    - `${catalog_name}.gold.dim_ratecard_tiers` (Exploded tier outrigger dimension)
# MAGIC 6. Commits ACID delta watermark to `${catalog_name}.control.pipeline_watermark_control` and ADLS checkpoints.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 1: Pipeline Parameters (Widgets)

# COMMAND ----------

import os
import json
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("execution_date", "", "Execution Date (YYYY-MM-DD)")
    dbutils.widgets.text("env", "dev", "Environment (dev/qa/prod)")
    dbutils.widgets.text("catalog_name", "adb_ratecard_dev_001", "Unity Catalog Name")
    
    execution_date = dbutils.widgets.get("execution_date")
    env = dbutils.widgets.get("env")
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    execution_date = "2024-10-26"
    env = "dev"
    catalog_name = "adb_ratecard_dev_001"

print(f"Executing Gold Load | Catalog: {catalog_name} | Env: {env} | Date: {execution_date}")

# Fully qualified 3-level table references
tbl_silver_rules = f"{catalog_name}.silver.silver_ratecard_rules"
tbl_silver_tiers = f"{catalog_name}.silver.silver_ratecard_tier_rules"
tbl_dim_cust = f"{catalog_name}.gold.dim_customer"
tbl_dim_part = f"{catalog_name}.gold.dim_part"
tbl_gold_pricing_rule = f"{catalog_name}.gold.dim_pricing_rule"
tbl_gold_ratecard_tiers = f"{catalog_name}.gold.dim_ratecard_tiers"
tbl_control_watermark = f"{catalog_name}.control.pipeline_watermark_control"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 2: Load Silver Inputs & Conformed Dimensions

# COMMAND ----------

# Read Silver Inputs
df_silver_rules = spark.table(tbl_silver_rules)
df_silver_tiers = spark.table(tbl_silver_tiers)

# Read Pre-existing Conformed Dimensions
df_dim_customer = spark.table(tbl_dim_cust)
df_dim_part = spark.table(tbl_dim_part)

print(f"Silver Rules: {df_silver_rules.count()} | Silver Tiers: {df_silver_tiers.count()}")
print(f"Conformed Customers: {df_dim_customer.count()} | Conformed Parts: {df_dim_part.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 3: Dimensional Joins & Late-Arriving Dimension Handling

# COMMAND ----------

UNKNOWN_SK = -1

# 1. Lookup Customer Surrogate Key (-1 Fallback)
df_with_cust = df_silver_rules.join(
    df_dim_customer.select("customer_id", "customer_key"),
    on="customer_id",
    how="left"
).fillna({"customer_key": UNKNOWN_SK})

# 2. Lookup Part Surrogate Key (-1 Fallback)
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
    F.current_timestamp().alias("gold_created_at")
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
# MAGIC ### Cell 4: Persist Gold Star Schema Tables (Pure DML)

# COMMAND ----------

# Save dim_pricing_rule
(
    dim_pricing_rule.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(tbl_gold_pricing_rule)
)
print(f"Successfully persisted: {dim_pricing_rule.count()} rows -> {tbl_gold_pricing_rule}")

# Save dim_ratecard_tiers
(
    dim_ratecard_tiers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(tbl_gold_ratecard_tiers)
)
print(f"Successfully persisted: {dim_ratecard_tiers.count()} rows -> {tbl_gold_ratecard_tiers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 5: Commit ACID Delta Watermark & Checkpoint File

# COMMAND ----------

# Extract max source watermark from ingested silver batch
max_watermark_row = df_silver_rules.select(F.max("updated_at")).collect()
new_watermark = max_watermark_row[0][0] if max_watermark_row and max_watermark_row[0][0] else None

if new_watermark:
    # 1. ACID Delta MERGE into Control Table
    spark.sql(f"""
    MERGE INTO {tbl_control_watermark} t
    USING (
        SELECT 'pl_ratecard_master_orchestrator' AS pipeline_name,
               'billing_oltp.subscription_ratecard_rules' AS source_table,
               'updated_at' AS watermark_column,
               TIMESTAMP '{new_watermark}' AS last_processed_watermark,
               {dim_pricing_rule.count()} AS rows_processed,
               'SUCCESS' AS last_run_status,
               current_timestamp() AS updated_at
    ) s
    ON t.pipeline_name = s.pipeline_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Committed ACID Watermark to {tbl_control_watermark}: {new_watermark}")

    # 2. Update Checkpoint JSON for ADF Lookup
    try:
        STORAGE_ACCOUNT = "stlakehousedevsk01"
        checkpoint_path = f"abfss://checkpoints@{STORAGE_ACCOUNT}.dfs.core.windows.net/watermarks/ratecard_watermark.json"
        checkpoint_payload = json.dumps({
            "last_watermark": str(new_watermark),
            "pipeline_name": "pl_ratecard_master_orchestrator",
            "updated_at": str(execution_date)
        })
        dbutils.fs.put(checkpoint_path, checkpoint_payload, overwrite=True)
        print(f"Updated ADF checkpoint JSON at: {checkpoint_path}")
    except Exception as e:
        print(f"Checkpoint file write note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 6: Dimensional Audit Summary

# COMMAND ----------

df_audit = spark.sql(f"""
SELECT 
    COUNT(*) AS total_rules,
    SUM(CASE WHEN customer_key = -1 THEN 1 ELSE 0 END) AS unmapped_customers,
    SUM(CASE WHEN part_key = -1 THEN 1 ELSE 0 END) AS unmapped_parts,
    SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) AS currently_active_rules
FROM {tbl_gold_pricing_rule}
""")
display(df_audit)
