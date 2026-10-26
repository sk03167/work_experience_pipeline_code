"""
Setup Conformed Dimensions Notebook (Gold Layer)
Initializes and seeds pre-existing conformed dimensions:
- dim_customer (customer_key [SK], customer_id [BK], customer_name, country_code, industry_segment)
- dim_part (part_key [SK], part_number [BK], product_name, product_family, unit_of_measure)
"""

from pyspark.sql import functions as F
from ratecard_pipeline.databricks.utils.spark_utils import get_spark_session
from ratecard_pipeline.databricks.config.pipeline_config import PipelineConfig

def seed_conformed_dimensions(spark):
    print("Initializing Conformed Dimensions...")

    # 1. Seed Customer Conformed Dimension
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
    df_cust.write.format("delta").mode("overwrite").save(PipelineConfig.GOLD_DIM_CUSTOMER_PATH)
    print(f"dim_customer seeded at: {PipelineConfig.GOLD_DIM_CUSTOMER_PATH}")

    # 2. Seed Part / Product Conformed Dimension
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
    df_parts.write.format("delta").mode("overwrite").save(PipelineConfig.GOLD_DIM_PART_PATH)
    print(f"dim_part seeded at: {PipelineConfig.GOLD_DIM_PART_PATH}")

if __name__ == "__main__":
    spark = get_spark_session()
    seed_conformed_dimensions(spark)

