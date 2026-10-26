# Databricks / Spark Pipeline Configuration
import os

class PipelineConfig:
    # Storage Paths (Local Delta lakehouse or ADLS Gen2 paths)
    BASE_DIR = os.getenv("STORAGE_BASE_PATH", "./data/lakehouse")
    
    # Delta Layer Paths
    BRONZE_RATECARDS_PATH = f"{BASE_DIR}/bronze/subscription_ratecard_rules"
    SILVER_RATECARDS_PATH = f"{BASE_DIR}/silver/ratecard_rules"
    SILVER_TIERS_PATH = f"{BASE_DIR}/silver/ratecard_tier_rules"
    
    # Gold Star Schema Paths
    GOLD_DIM_CUSTOMER_PATH = f"{BASE_DIR}/gold/dim_customer"
    GOLD_DIM_PART_PATH = f"{BASE_DIR}/gold/dim_part"
    GOLD_DIM_SUBSCRIPTION_PATH = f"{BASE_DIR}/gold/dim_subscription"
    GOLD_DIM_PRICING_RULE_PATH = f"{BASE_DIR}/gold/dim_pricing_rule"
    GOLD_DIM_RATECARD_TIERS_PATH = f"{BASE_DIR}/gold/dim_ratecard_tiers"
    
    # Checkpoint Paths for Streaming / Micro-batches
    CHECKPOINT_BRONZE = f"{BASE_DIR}/checkpoints/bronze_ratecard"
    CHECKPOINT_SILVER = f"{BASE_DIR}/checkpoints/silver_ratecard"
    
    # Sentinel constant for unbounded upper tier
    UNBOUNDED_TIER_MAX = 999999999999.0

