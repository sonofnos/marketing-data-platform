"""Tests for RFM segmentation logic."""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType,
                                StructField, StructType)


def _write_silver_customers(spark, silver_dir):
    rows = [
        ("C001", "****@x.com", "NG", "Lagos", "email", "2023-01-01", "2024-01-01", 100.0, 3, True, "2024-01-01"),
        ("C002", "****@y.com", "GH", "Accra", "sms", "2024-01-01", "2024-10-01", 500.0, 10, True, "2024-01-01"),
        ("C003", "****@z.com", "KE", "Nairobi", "push", "2022-01-01", "2023-01-01", 50.0, 1, True, "2024-01-01"),
    ]
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("email_masked", StringType()),
        StructField("country", StringType()),
        StructField("city", StringType()),
        StructField("preferred_channel", StringType()),
        StructField("signup_date", StringType()),
        StructField("last_purchase_date", StringType()),
        StructField("total_spend", DoubleType()),
        StructField("purchase_count", IntegerType()),
        StructField("is_active", BooleanType()),
        StructField("processed_at", StringType()),
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(silver_dir / "customers"))


def _write_silver_events(spark, silver_dir):
    rows = [
        # C001: recent conversions, moderate frequency
        ("e1", "C001", "CAMP01", "conversion", "email", "mobile", "2024-10-20T10:00:00", "2024-10-20", 80.0, True),
        ("e2", "C001", "CAMP01", "conversion", "email", "mobile", "2024-10-21T10:00:00", "2024-10-21", 90.0, True),
        ("e3", "C001", "CAMP01", "click", "email", "mobile", "2024-10-22T10:00:00", "2024-10-22", 0.0, False),
        # C002: highest frequency + spend
        *[
            (f"e{10+i}", "C002", "CAMP01", "conversion", "sms", "mobile",
             f"2024-10-{15+i:02d}T10:00:00", f"2024-10-{15+i:02d}", 100.0, True)
            for i in range(10)
        ],
        # C003: old conversions, low value
        ("e99", "C003", "CAMP01", "conversion", "push", "tablet", "2024-01-01T10:00:00", "2024-01-01", 10.0, True),
    ]
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("campaign_id", StringType()),
        StructField("event_type", StringType()),
        StructField("channel", StringType()),
        StructField("device", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("event_date", StringType()),
        StructField("revenue", DoubleType()),
        StructField("is_conversion", BooleanType()),
    ])
    spark.createDataFrame(rows, schema).write.mode("overwrite").parquet(str(silver_dir / "campaign_events"))


class TestSegmentation:
    def _setup(self, spark, tmp_data_dir):
        _write_silver_customers(spark, tmp_data_dir / "silver")
        _write_silver_events(spark, tmp_data_dir / "silver")

    def test_all_customers_segmented(self, spark, tmp_data_dir):
        from pipelines import segment
        self._setup(spark, tmp_data_dir)
        df = segment.rfm_segment(spark)
        assert df.count() == 3

    def test_segment_labels_valid(self, spark, tmp_data_dir):
        from pipelines import segment
        self._setup(spark, tmp_data_dir)
        df = segment.rfm_segment(spark)
        valid = {"Champions", "Loyal", "Potential Loyalist", "At Risk", "Hibernating", "Promising", "Needs Attention"}
        labels = {r["segment"] for r in df.collect()}
        assert labels.issubset(valid)

    def test_rfm_scores_in_range(self, spark, tmp_data_dir):
        from pipelines import segment
        self._setup(spark, tmp_data_dir)
        df = segment.rfm_segment(spark)
        for row in df.collect():
            assert 1 <= row["r_score"] <= 5
            assert 1 <= row["f_score"] <= 5
            assert 1 <= row["m_score"] <= 5

    def test_build_audience_filters_inactive(self, spark, tmp_data_dir):
        from pipelines import segment
        self._setup(spark, tmp_data_dir)
        segment.rfm_segment(spark)
        for label in ["Champions", "Loyal", "Promising", "Needs Attention"]:
            audience = segment.build_audience(spark, label)
            inactive = audience.filter(F.col("is_active") == False).count()
            assert inactive == 0

    def test_segment_summary_sums_to_total(self, spark, tmp_data_dir):
        from pipelines import segment
        self._setup(spark, tmp_data_dir)
        segment.rfm_segment(spark)
        summary = segment.segment_summary(spark)
        total = summary.agg(F.sum("customer_count")).first()[0]
        assert total == 3
