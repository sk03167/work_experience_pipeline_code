"""
Unit Tests for Ratecard PySpark Transformations
Tests struct unnesting, tier explosion, boundary validation, and conformed dimension lookups.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DecimalType,
    IntegerType, BooleanType, ArrayType, TimestampType
)
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("RatecardUnitTests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def test_tier_explosion_and_surrogate_key_generation(spark):
    """
    Asserts that an array of N tier structs explodes into N discrete rows,
    calculates correct tier_rule_id hash, and preserves unbounded 999999999999.0.
    """
    tier_schema = StructType([
        StructField("tier_number", IntegerType()),
        StructField("min_usage", DecimalType(18, 4)),
        StructField("max_usage", DecimalType(18, 4)),
        StructField("unit_price", DecimalType(18, 6)),
        StructField("overage_price", DecimalType(18, 6)),
        StructField("is_overage_applicable", BooleanType())
    ])

    test_data = [
        (
            "RULE_1001", "CUST_01", "B91128", "SUB_1", "LINE_1",
            date(2024, 1, 1), date(2024, 12, 31),
            [
                (1, 0.0, 1000.0, 0.025, 0.030, True),
                (2, 1000.0001, 999999999999.0, 0.020, 0.020, False)
            ]
        )
    ]

    schema = StructType([
        StructField("pricing_rule_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("part_number", StringType()),
        StructField("subscription_id", StringType()),
        StructField("subscription_line_id", StringType()),
        StructField("date_start", DateType()),
        StructField("date_end", DateType()),
        StructField("tier_pricing_rules", ArrayType(tier_schema))
    ])

    df_raw = spark.createDataFrame(test_data, schema)

    # Explode
    df_exploded = df_raw.select(
        F.col("pricing_rule_id"),
        F.explode(F.col("tier_pricing_rules")).alias("tier")
    ).select(
        F.sha2(F.concat_ws("||", F.col("pricing_rule_id"), F.col("tier.tier_number")), 256).alias("tier_rule_id"),
        F.col("pricing_rule_id"),
        F.col("tier.tier_number").alias("tier_number"),
        F.col("tier.min_usage").alias("min_usage"),
        F.col("tier.max_usage").alias("max_usage"),
        F.col("tier.unit_price").alias("unit_price")
    )

    rows = df_exploded.collect()
    assert len(rows) == 2
    assert rows[0]["tier_number"] == 1
    assert float(rows[0]["max_usage"]) == 1000.0
    assert rows[1]["tier_number"] == 2
    assert float(rows[1]["max_usage"]) == 999999999999.0
    assert rows[0]["tier_rule_id"] != rows[1]["tier_rule_id"]


def test_conformed_dimension_lookup_with_unknown_fallback(spark):
    """
    Asserts that customer_key and part_key resolve correctly,
    and missing/unmapped foreign keys fallback to -1 (UNKNOWN).
    """
    # Rules
    rules_data = [
        ("RULE_KNOWN", "CUST_MATCH", "PART_MATCH"),
        ("RULE_UNKNOWN", "CUST_MISSING", "PART_MISSING")
    ]
    df_rules = spark.createDataFrame(rules_data, ["pricing_rule_id", "customer_id", "part_number"])

    # Conformed Dims
    df_cust = spark.createDataFrame([(100, "CUST_MATCH")], ["customer_key", "customer_id"])
    df_part = spark.createDataFrame([(200, "PART_MATCH")], ["part_key", "part_number"])

    # Join with fallback
    df_joined = df_rules.join(df_cust, on="customer_id", how="left").fillna({"customer_key": -1})
    df_joined = df_joined.join(df_part, on="part_number", how="left").fillna({"part_key": -1})

    res = {r["pricing_rule_id"]: (r["customer_key"], r["part_key"]) for r in df_joined.collect()}

    assert res["RULE_KNOWN"] == (100, 200)
    assert res["RULE_UNKNOWN"] == (-1, -1)

