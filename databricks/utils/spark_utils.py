import sys
from pyspark.sql import SparkSession

def get_spark_session(app_name: str = "RatecardLakehousePipeline") -> SparkSession:
    """
    Constructs a production-tuned SparkSession configured with Delta Lake support.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
    )
    return builder.getOrCreate()

