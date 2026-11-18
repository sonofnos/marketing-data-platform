import os
from pathlib import Path
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD = DATA_DIR / "gold"

RAW_CRM = DATA_DIR / "raw" / "crm_customers.csv"
RAW_CAMPAIGNS = DATA_DIR / "raw" / "campaigns.json"
RAW_EVENTS = DATA_DIR / "raw" / "campaign_events.json"
RAW_WEB = DATA_DIR / "raw" / "web_analytics.parquet"


def get_spark(app_name: str = "MarketingDataPlatform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
