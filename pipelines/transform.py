"""Silver + Gold transformations: clean, enrich, aggregate."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import window as W

from pipelines.config import BRONZE, GOLD, SILVER


def _read_bronze(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.parquet(str(BRONZE / name))


def _write_silver(df: DataFrame, name: str) -> None:
    (
        df.write.mode("overwrite")
        .parquet(str(SILVER / name))
    )


def _write_gold(df: DataFrame, name: str) -> None:
    (
        df.write.mode("overwrite")
        .parquet(str(GOLD / name))
    )


# ── Silver ────────────────────────────────────────────────────────────────────

def bronze_to_silver_customers(spark: SparkSession) -> DataFrame:
    """Dedup, cast types, mask PII, drop nulls on key fields."""
    df = _read_bronze(spark, "crm_customers")

    # Deduplicate: keep the row with highest purchase_count per customer_id
    window = W.Window.partitionBy("customer_id").orderBy(F.col("purchase_count").desc())
    df = (
        df.filter(F.col("customer_id").isNotNull())
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    df = (
        df.withColumn("total_spend", F.col("total_spend").cast("double"))
        .withColumn("purchase_count", F.col("purchase_count").cast("int"))
        .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))
        .withColumn("last_purchase_date", F.to_date("last_purchase_date", "yyyy-MM-dd"))
        # Mask PII: keep domain, hide local part
        .withColumn(
            "email_masked",
            F.concat(F.lit("****@"), F.regexp_extract("email", r"@(.+)$", 1)),
        )
        .drop("email", "phone")
        .withColumn("processed_at", F.current_timestamp())
    )

    _write_silver(df, "customers")
    return df


def bronze_to_silver_events(spark: SparkSession) -> DataFrame:
    """Parse timestamps, standardise event types, drop malformed rows."""
    df = _read_bronze(spark, "campaign_events")

    valid_types = ["impression", "click", "open", "conversion", "unsubscribe"]

    df = (
        df.filter(F.col("customer_id").isNotNull())
        .filter(F.col("campaign_id").isNotNull())
        .filter(F.col("event_type").isin(valid_types))
        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp"),
        )
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("revenue", F.col("revenue").cast("double"))
        .withColumn(
            "is_conversion",
            F.when(F.col("event_type") == "conversion", True).otherwise(False),
        )
        .drop("metadata")
        .withColumn("processed_at", F.current_timestamp())
    )

    _write_silver(df, "campaign_events")
    return df


def bronze_to_silver_campaigns(spark: SparkSession) -> DataFrame:
    df = _read_bronze(spark, "campaigns")
    df = (
        df.withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
        .withColumn("budget", F.col("budget").cast("double"))
        .withColumn("duration_days", F.datediff("end_date", "start_date"))
        .withColumn("processed_at", F.current_timestamp())
    )
    _write_silver(df, "campaigns")
    return df


# ── Gold ──────────────────────────────────────────────────────────────────────

def silver_to_gold_campaign_metrics(spark: SparkSession) -> DataFrame:
    """Per-campaign: impressions, clicks, conversions, CTR, CVR, revenue."""
    events = spark.read.parquet(str(SILVER / "campaign_events"))
    campaigns = spark.read.parquet(str(SILVER / "campaigns"))

    metrics = (
        events.groupBy("campaign_id")
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "impression", 1).otherwise(0)).alias("impressions"),
            F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("clicks"),
            F.sum(F.when(F.col("event_type") == "open", 1).otherwise(0)).alias("opens"),
            F.sum(F.when(F.col("event_type") == "conversion", 1).otherwise(0)).alias("conversions"),
            F.sum(F.when(F.col("event_type") == "unsubscribe", 1).otherwise(0)).alias("unsubscribes"),
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn(
            "ctr",
            F.round(F.col("clicks") / F.when(F.col("impressions") > 0, F.col("impressions")) * 100, 2),
        )
        .withColumn(
            "cvr",
            F.round(F.col("conversions") / F.when(F.col("clicks") > 0, F.col("clicks")) * 100, 2),
        )
    )

    result = metrics.join(
        campaigns.select("campaign_id", "name", "channel", "product", "budget", "target_segment"),
        on="campaign_id",
        how="left",
    ).withColumn(
        "cost_per_conversion",
        F.round(F.col("budget") / F.when(F.col("conversions") > 0, F.col("conversions")), 2),
    ).withColumn("computed_at", F.current_timestamp())

    _write_gold(result, "campaign_metrics")
    return result


def silver_to_gold_customer_ltv(spark: SparkSession) -> DataFrame:
    """30/60/90-day revenue windows per customer."""
    events = spark.read.parquet(str(SILVER / "campaign_events"))
    customers = spark.read.parquet(str(SILVER / "customers"))

    today = F.current_date()

    revenue = (
        events.filter(F.col("revenue") > 0)
        .groupBy("customer_id")
        .agg(
            F.sum(F.when(F.datediff(today, F.col("event_date")) <= 30, F.col("revenue"))).alias("ltv_30d"),
            F.sum(F.when(F.datediff(today, F.col("event_date")) <= 60, F.col("revenue"))).alias("ltv_60d"),
            F.sum(F.when(F.datediff(today, F.col("event_date")) <= 90, F.col("revenue"))).alias("ltv_90d"),
            F.sum("revenue").alias("total_revenue"),
            F.count(F.when(F.col("is_conversion"), True)).alias("conversion_count"),
        )
    )

    result = (
        customers.select("customer_id", "country", "preferred_channel", "signup_date", "is_active")
        .join(revenue, on="customer_id", how="left")
        .fillna({"ltv_30d": 0.0, "ltv_60d": 0.0, "ltv_90d": 0.0, "total_revenue": 0.0, "conversion_count": 0})
        .withColumn("computed_at", F.current_timestamp())
    )

    _write_gold(result, "customer_ltv")
    return result


def run_all(spark: SparkSession) -> None:
    for name, fn in [
        ("silver.customers", bronze_to_silver_customers),
        ("silver.events", bronze_to_silver_events),
        ("silver.campaigns", bronze_to_silver_campaigns),
        ("gold.campaign_metrics", silver_to_gold_campaign_metrics),
        ("gold.customer_ltv", silver_to_gold_customer_ltv),
    ]:
        df = fn(spark)
        print(f"  [{name}] {df.count():,} rows")
