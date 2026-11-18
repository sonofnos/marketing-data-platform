"""Gold → Targeting: RFM segmentation and audience builder."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import window as W

from pipelines.config import GOLD, SILVER


def rfm_segment(spark: SparkSession) -> DataFrame:
    """
    Score each customer on Recency, Frequency, Monetary (1–5 each).
    Assign a named segment based on combined score.
    """
    customers = spark.read.parquet(str(SILVER / "customers"))
    events = spark.read.parquet(str(SILVER / "campaign_events"))

    today = F.current_date()

    rfm_raw = (
        events.filter(F.col("event_type") == "conversion")
        .groupBy("customer_id")
        .agg(
            F.datediff(today, F.max("event_date")).alias("recency_days"),
            F.count("*").alias("frequency"),
            F.sum("revenue").alias("monetary"),
        )
    )

    # Quintile scoring (1 = worst, 5 = best)
    q_window = W.Window.orderBy("recency_days")
    q_window_desc = W.Window.orderBy(F.col("frequency").desc())
    q_window_m = W.Window.orderBy(F.col("monetary").desc())

    scored = (
        rfm_raw.withColumn(
            "r_score",
            F.ntile(5).over(q_window),  # lower recency_days = more recent = better
        )
        .withColumn("f_score", F.ntile(5).over(q_window_desc))
        .withColumn("m_score", F.ntile(5).over(q_window_m))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )

    labeled = scored.withColumn(
        "segment",
        F.when(F.col("rfm_score") >= 13, "Champions")
        .when((F.col("rfm_score") >= 10) & (F.col("f_score") >= 4), "Loyal")
        .when((F.col("rfm_score") >= 9) & (F.col("r_score") >= 4), "Potential Loyalist")
        .when((F.col("r_score") <= 2) & (F.col("rfm_score") >= 8), "At Risk")
        .when(F.col("r_score") <= 2, "Hibernating")
        .when(F.col("rfm_score") >= 7, "Promising")
        .otherwise("Needs Attention"),
    )

    result = labeled.join(
        customers.select("customer_id", "country", "preferred_channel", "email_masked", "is_active"),
        on="customer_id",
        how="left",
    ).withColumn("segmented_at", F.current_timestamp())

    out = str(GOLD / "rfm_segments")
    result.write.mode("overwrite").parquet(out)
    return result


def build_audience(spark: SparkSession, segment_label: str) -> DataFrame:
    """Return customer contact data for a given segment — ready for campaign targeting."""
    segments = spark.read.parquet(str(GOLD / "rfm_segments"))
    audience = (
        segments.filter(F.col("segment") == segment_label)
        .filter(F.col("is_active") == True)
        .select(
            "customer_id",
            "email_masked",
            "preferred_channel",
            "country",
            "rfm_score",
            "monetary",
            "recency_days",
        )
        .orderBy(F.col("rfm_score").desc())
    )
    return audience


def segment_summary(spark: SparkSession) -> DataFrame:
    """Aggregate stats per segment for reporting."""
    segments = spark.read.parquet(str(GOLD / "rfm_segments"))
    return (
        segments.groupBy("segment")
        .agg(
            F.count("*").alias("customer_count"),
            F.round(F.avg("rfm_score"), 2).alias("avg_rfm_score"),
            F.round(F.avg("monetary"), 2).alias("avg_monetary"),
            F.round(F.avg("recency_days"), 1).alias("avg_recency_days"),
        )
        .orderBy(F.col("avg_rfm_score").desc())
    )


def run_all(spark: SparkSession) -> None:
    df = rfm_segment(spark)
    print(f"  [gold.rfm_segments] {df.count():,} customers segmented")
    summary = segment_summary(spark)
    summary.show(truncate=False)
