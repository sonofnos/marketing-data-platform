"""Bronze layer: multi-source ingestion into partitioned parquet."""
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.config import BRONZE, RAW_CAMPAIGNS, RAW_CRM, RAW_EVENTS, RAW_WEB


def _write_bronze(df: DataFrame, name: str) -> None:
    out = str(BRONZE / name)
    (
        df.withColumn("ingested_date", F.current_date())
        .write.mode("overwrite")
        .partitionBy("ingested_date")
        .parquet(out)
    )


def ingest_crm(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read CRM customer CSV into bronze layer."""
    src = path or str(RAW_CRM)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(src)
        .withColumn("_source", F.lit("crm"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    _write_bronze(df, "crm_customers")
    return df


def ingest_campaigns(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read campaign metadata JSON into bronze layer."""
    src = path or str(RAW_CAMPAIGNS)
    df = (
        spark.read.option("multiline", "true")
        .json(src)
        .withColumn("_source", F.lit("campaigns"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    _write_bronze(df, "campaigns")
    return df


def ingest_events(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read JSONL campaign events into bronze layer."""
    src = path or str(RAW_EVENTS)
    df = (
        spark.read.json(src)
        .withColumn("_source", F.lit("events"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp")),
        )
    )
    _write_bronze(df, "campaign_events")
    return df


def ingest_web_analytics(spark: SparkSession, path: str | None = None) -> DataFrame:
    """Read web analytics parquet into bronze layer."""
    src = path or str(RAW_WEB)
    df = (
        spark.read.parquet(src)
        .withColumn("_source", F.lit("web_analytics"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    _write_bronze(df, "web_analytics")
    return df


def run_all(spark: SparkSession) -> dict[str, int]:
    counts = {}
    for name, fn in [
        ("crm_customers", ingest_crm),
        ("campaigns", ingest_campaigns),
        ("campaign_events", ingest_events),
        ("web_analytics", ingest_web_analytics),
    ]:
        df = fn(spark)
        counts[name] = df.count()
        print(f"  [bronze] {name}: {counts[name]:,} rows ingested")
    return counts
