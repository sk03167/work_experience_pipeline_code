from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DecimalType,
    IntegerType,
    BooleanType,
    ArrayType,
    TimestampType
)

# 1. Tier Pricing Struct Definition (mirrors the Oracle/Postgres struct)
TierPricingStructSchema = StructType([
    StructField("tier_number", IntegerType(), False),
    StructField("min_usage", DecimalType(18, 4), False),
    StructField("max_usage", DecimalType(18, 4), False),
    StructField("unit_price", DecimalType(18, 6), False),
    StructField("overage_price", DecimalType(18, 6), True),
    StructField("is_overage_applicable", BooleanType(), True)
])

# 2. OLTP Ingestion Schema (Bronze)
OltpRatecardIngestSchema = StructType([
    StructField("pricing_rule_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("part_number", StringType(), False),
    StructField("subscription_id", StringType(), False),
    StructField("subscription_line_id", StringType(), False),
    StructField("date_start", DateType(), False),
    StructField("date_end", DateType(), False),
    StructField("tier_pricing_rules", ArrayType(TierPricingStructSchema), False),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True)
])

# 3. Conformed Customer Dimension Schema (Gold)
DimCustomerSchema = StructType([
    StructField("customer_key", IntegerType(), False),    # Surrogate Key
    StructField("customer_id", StringType(), False),     # Business Key
    StructField("customer_name", StringType(), False),
    StructField("country_code", StringType(), True),
    StructField("industry_segment", StringType(), True),
    StructField("is_active", BooleanType(), False)
])

# 4. Conformed Part / Product Dimension Schema (Gold)
DimPartSchema = StructType([
    StructField("part_key", IntegerType(), False),        # Surrogate Key
    StructField("part_number", StringType(), False),     # Business Key
    StructField("product_name", StringType(), False),
    StructField("product_family", StringType(), True),
    StructField("unit_of_measure", StringType(), False),
    StructField("is_active", BooleanType(), False)
])

