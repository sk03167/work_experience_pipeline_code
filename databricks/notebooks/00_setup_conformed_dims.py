# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup_conformed_dims: Seed Conformed Dimensions in Databricks Catalog
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Gold (Dimensional Serving / Catalog Tables)  
# MAGIC **Target:** `${catalog_name}.gold.dim_customer`, `${catalog_name}.gold.dim_part`
# MAGIC
# MAGIC Creates and seeds the pre-existing enterprise conformed dimension tables in the Databricks Metastore:
# MAGIC - `dim_customer` (`customer_key` [SK], `customer_id` [BK], customer_name, country_code, industry_segment, is_active)
# MAGIC - `dim_part` (`part_key` [SK], `part_number` [BK], product_name, product_family, unit_of_measure, is_active)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog_name", "adb_ratecard_dev_001", "Unity Catalog Name")
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "adb_ratecard_dev_001"

print(f"Seeding Conformed Dimensions into Catalog: {catalog_name}.gold")

# Ensure gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Register & Seed `dim_customer` (Catalog Table)

# COMMAND ----------

customers_data = [
    (1001, "CUST_1001", "Acme Global Financials", "Banking & Capital Markets", "USD"),
    (1002, "CUST_1002", "Nova Health Therapeutics", "Healthcare & Life Sciences", "USD"),
    (1003, "CUST_1003", "Apex Logistics Worldwide", "Transportation & Logistics", "EUR"),
    (1004, "CUST_1004", "Kyoto Telecom Network", "Telecommunications", "JPY"),
    (1005, "CUST_1005", "Nordic Retail Group", "Retail & Consumer Goods", "EUR"),
    (1006, "CUST_1006", "Horizon Autonomous Systems", "Automotive & AI", "GBP"),
    (1007, "CUST_1007", "Solaria Energy Grid", "Energy & Utilities", "EUR"),
    (1008, "CUST_1008", "Vanguard Media Streaming", "Media & Entertainment", "USD"),
    (-1, "UNKNOWN", "Unknown / Late Arriving Customer", "Unassigned", "USD"),
]

cust_columns = ["customer_key", "customer_id", "customer_name", "customer_tier", "billing_currency"]
df_cust = spark.createDataFrame(customers_data, cust_columns)

target_dim_cust = f"{catalog_name}.gold.dim_customer"
df_cust.write.format("delta").mode("overwrite").saveAsTable(target_dim_cust)
print(f"Registered and populated catalog table: {target_dim_cust}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Register & Seed `dim_part` (Catalog Table)

# COMMAND ----------

parts_data = [
    (2001, "B91128", "OCI Compute Standard E4", "OCPU-Hour", "Cloud Compute"),
    (2002, "B91129", "OCI Compute DenseIO E4", "OCPU-Hour", "Cloud Compute"),
    (2003, "B88317", "OCI Autonomous Data Warehouse", "ECPU-Hour", "Cloud Database"),
    (2004, "B88318", "OCI Autonomous Transaction Processing", "ECPU-Hour", "Cloud Database"),
    (2005, "B91961", "OCI Block Volume Storage", "GB-Month", "Cloud Storage"),
    (2006, "B91962", "OCI Object Storage Standard", "GB-Month", "Cloud Storage"),
    (2007, "B93120", "OCI Streaming Service", "Gigabytes-In-Out", "Cloud Integration"),
    (2008, "B95011", "OCI Web Application Firewall", "Requests-Million", "Cloud Security"),
    (-1, "UNKNOWN", "Unknown / Late Arriving Part", "Units", "Unassigned"),
]

parts_columns = ["part_key", "part_number", "product_name", "uom", "service_category"]
df_parts = spark.createDataFrame(parts_data, parts_columns)

target_dim_part = f"{catalog_name}.gold.dim_part"
df_parts.write.format("delta").mode("overwrite").saveAsTable(target_dim_part)
print(f"Registered and populated catalog table: {target_dim_part}")

# COMMAND ----------

# Verification
display(spark.sql(f"SELECT * FROM {target_dim_cust} ORDER BY customer_key"))
display(spark.sql(f"SELECT * FROM {target_dim_part} ORDER BY part_key"))
