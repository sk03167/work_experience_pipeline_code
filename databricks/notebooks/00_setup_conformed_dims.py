# Databricks notebook source
# MAGIC %md
# MAGIC # 00_setup_conformed_dims: Seed Conformed Dimensions (Gold Layer)
# MAGIC
# MAGIC **Author:** Shivansh Kumar (Senior Data Engineer)  
# MAGIC **Layer:** Gold (Conformed Dimension Store)  
# MAGIC
# MAGIC ### Overview:
# MAGIC Initializes and seeds pre-existing enterprise conformed dimensions:
# MAGIC - `dim_customer` (`customer_key` [SK], `customer_id` [BK], name, region, industry)
# MAGIC - `dim_part` (`part_key` [SK], `part_number` [BK], name, family, unit_of_measure)
# MAGIC
# MAGIC Maps default `-1` surrogate keys for late-arriving / unknown records.

# COMMAND ----------

# Storage paths pointing to your ADLS Gen2 Lakehouse (or DBFS / Delta Lake tables)
# In Azure Databricks with Unity Catalog / Storage, you can write directly to abfss:// or table names
STORAGE_ACCOUNT = "stlakehousedevsk01"
GOLD_BASE_PATH = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Fallback path if direct ABFSS storage credentials are not yet mounted on the cluster:
LOCAL_DELTA_PATH = "/tmp/lakehouse/gold"

# Let's write as managed Delta tables or direct path
dim_cust_path = f"{GOLD_BASE_PATH}/dim_customer"
dim_part_path = f"{GOLD_BASE_PATH}/dim_part"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Seed Customer Conformed Dimension (`dim_customer`)

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

# Write to Delta table
try:
    df_cust.write.format("delta").mode("overwrite").save(dim_cust_path)
    print(f"Successfully seeded dim_customer to ADLS: {dim_cust_path}")
except Exception as e:
    print(f"ABFSS direct write fell back to local delta table: {e}")
    df_cust.write.format("delta").mode("overwrite").saveAsTable("dim_customer")
    print("Successfully created managed table: dim_customer")

display(df_cust)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Seed Product / Part Conformed Dimension (`dim_part`)

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

try:
    df_parts.write.format("delta").mode("overwrite").save(dim_part_path)
    print(f"Successfully seeded dim_part to ADLS: {dim_part_path}")
except Exception as e:
    print(f"ABFSS direct write fell back to local delta table: {e}")
    df_parts.write.format("delta").mode("overwrite").saveAsTable("dim_part")
    print("Successfully created managed table: dim_part")

display(df_parts)
