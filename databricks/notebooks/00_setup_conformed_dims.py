# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup_conformed_dims: Seed Conformed Dimensions in Databricks Catalog
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Gold (Dimensional Serving / Catalog Tables)  
# MAGIC
# MAGIC Creates and seeds the pre-existing enterprise conformed dimension tables in the Databricks Metastore:
# MAGIC - `dim_customer` (`customer_key` [SK], `customer_id` [BK], customer_name, country_code, industry_segment, is_active)
# MAGIC - `dim_part` (`part_key` [SK], `part_number` [BK], product_name, product_family, unit_of_measure, is_active)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Register & Seed `dim_customer` (Catalog Table)

# COMMAND ----------

customers_data = [
    (1001, "CUST_1001", "Acme Global Financials", "USA", "Banking & Capital Markets", True),
    (1002, "CUST_1002", "Nova Health Therapeutics", "USA", "Healthcare & Life Sciences", True),
    (1003, "CUST_1003", "Apex Logistics Worldwide", "DEU", "Transportation & Logistics", True),
    (1004, "CUST_1004", "Kyoto Telecom Network", "JPN", "Telecommunications", True),
    (1005, "CUST_1005", "Nordic Retail Group", "SWE", "Retail & Consumer Goods", True),
    (1006, "CUST_1006", "Horizon Autonomous Systems", "GBR", "Automotive & AI", True),
    (1007, "CUST_1007", "Solaria Energy Grid", "ESP", "Energy & Utilities", True),
    (1008, "CUST_1008", "Vanguard Media Streaming", "USA", "Media & Entertainment", True),
    (-1, "UNKNOWN", "Unknown / Late Arriving Customer", "NA", "Unassigned", False),
]

cust_columns = ["customer_key", "customer_id", "customer_name", "country_code", "industry_segment", "is_active"]
df_cust = spark.createDataFrame(customers_data, cust_columns)

# Save as a real Databricks Catalog Delta Table
df_cust.write.format("delta").mode("overwrite").saveAsTable("dim_customer")
print("Registered and populated catalog table: default.dim_customer")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify dim_customer via standard SQL
# MAGIC SELECT * FROM dim_customer ORDER BY customer_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Register & Seed `dim_part` (Catalog Table)

# COMMAND ----------

parts_data = [
    (2001, "B91128", "OCI Compute Standard E4", "Compute", "OCPU-Hour", True),
    (2002, "B91129", "OCI Compute DenseIO E4", "Compute", "OCPU-Hour", True),
    (2003, "B88317", "OCI Autonomous Data Warehouse", "Database", "ECPU-Hour", True),
    (2004, "B88318", "OCI Autonomous Transaction Processing", "Database", "ECPU-Hour", True),
    (2005, "B91961", "OCI Block Volume Storage", "Storage", "GB-Month", True),
    (2006, "B91962", "OCI Object Storage Standard", "Storage", "GB-Month", True),
    (2007, "B93120", "OCI Streaming Service", "Integration", "Gigabytes-In-Out", True),
    (2008, "B95011", "OCI Web Application Firewall", "Security", "Requests-Million", True),
    (-1, "UNKNOWN", "Unknown / Late Arriving Part", "Unassigned", "Units", False),
]

parts_columns = ["part_key", "part_number", "product_name", "product_family", "unit_of_measure", "is_active"]
df_parts = spark.createDataFrame(parts_data, parts_columns)

# Save as a real Databricks Catalog Delta Table
df_parts.write.format("delta").mode("overwrite").saveAsTable("dim_part")
print("Registered and populated catalog table: default.dim_part")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify dim_part via standard SQL
# MAGIC SELECT * FROM dim_part ORDER BY part_key;
