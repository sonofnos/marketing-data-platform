"""Tests for silver and gold transformation logic."""
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def _write_bronze_customers(spark, bronze_dir: Path):
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("email", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("country", StringType()),
        StructField("total_spend", DoubleType()),
        StructField("purchase_count", IntegerType()),
        StructField("is_active", StringType()),
        StructField("preferred_channel", StringType()),
        StructField("signup_date", StringType()),
        StructField("last_purchase_date", StringType()),
        StructField("city", StringType()),
        StructField("phone", StringType()),
        StructField("_source", StringType()),
        StructField("_ingested_at", StringType()),
    ])
    rows = [
        ("C001", "ada@x.com", "Ada", "L", "NG", 100.0, 3, "true", "email", "2023-01-01", "2024-01-01", "Lagos", "+234", "crm", "2024-01-01"),
        ("C001", "ada@x.com", "Ada", "L", "NG", 200.0, 7, "true", "email", "2023-01-01", "2024-05-01", "Lagos", "+234", "crm", "2024-01-01"),
        ("C002", "grace@y.com", "Grace", "H", "GH", 0.0, 0, "false", "sms", "2024-01-01", "2024-01-01", "Accra", "+233", "crm", "2024-01-01"),
        ("C003", None, "Bad", "Row", None, 50.0, 1, "true", "email", "2024-01-01", "2024-01-01", "Lagos", "+234", "crm", "2024-01-01"),
    ]
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(str(bronze_dir / "crm_customers"))


def _write_bronze_events(spark, bronze_dir: Path):
    rows = [
        ("e1", "C001", "CAMP01", "click", "email", "mobile", "2024-10-15T12:00:00", 0.0, False, "2024-10-15"),
        ("e2", "C001", "CAMP01", "conversion", "email", "mobile", "2024-10-15T13:00:00", 99.0, True, "2024-10-15"),
        ("e3", "C002", "CAMP01", "impression", "sms", "tablet", "2024-10-14T10:00:00", 0.0, False, "2024-10-14"),
        ("e4", "C001", "CAMP01", "INVALID_TYPE", "email", "mobile", "2024-10-15T14:00:00", 0.0, False, "2024-10-15"),
        ("e5", None, "CAMP01", "click", "sms", "mobile", "2024-10-13T10:00:00", 0.0, False, "2024-10-13"),  # null customer
    ]
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("campaign_id", StringType()),
        StructField("event_type", StringType()),
        StructField("channel", StringType()),
        StructField("device", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("revenue", DoubleType()),
        StructField("is_conversion", StringType()),
        StructField("event_date", StringType()),
    ])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(str(bronze_dir / "campaign_events"))


def _write_bronze_campaigns(spark, bronze_dir: Path):
    rows = [
        ("CAMP01", "Test Campaign", "email", "Pro Plan", 1000.0, "2024-01-01", "2024-01-31", "Champions", "active"),
    ]
    schema = StructType([
        StructField("campaign_id", StringType()),
        StructField("name", StringType()),
        StructField("channel", StringType()),
        StructField("product", StringType()),
        StructField("budget", DoubleType()),
        StructField("start_date", StringType()),
        StructField("end_date", StringType()),
        StructField("target_segment", StringType()),
        StructField("status", StringType()),
    ])
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(str(bronze_dir / "campaigns"))


class TestSilverTransforms:
    def test_dedup_keeps_highest_purchase_count(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_customers(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_customers(spark)
        c001 = df.filter(F.col("customer_id") == "C001")
        assert c001.count() == 1
        assert c001.first()["purchase_count"] == 7

    def test_pii_masking_hides_local_part(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_customers(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_customers(spark)
        emails = [r["email_masked"] for r in df.collect() if r["email_masked"]]
        assert all(e.startswith("****@") for e in emails)
        assert "email" not in df.columns  # original dropped

    def test_null_customer_id_dropped(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_customers(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_customers(spark)
        nulls = df.filter(F.col("customer_id").isNull()).count()
        assert nulls == 0

    def test_invalid_event_types_filtered(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_events(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_events(spark)
        types = {r["event_type"] for r in df.collect()}
        assert "INVALID_TYPE" not in types

    def test_null_customer_events_dropped(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_events(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_events(spark)
        nulls = df.filter(F.col("customer_id").isNull()).count()
        assert nulls == 0

    def test_is_conversion_flag(self, spark, tmp_data_dir):
        from pipelines import transform
        _write_bronze_events(spark, tmp_data_dir / "bronze")
        df = transform.bronze_to_silver_events(spark)
        conversions = df.filter(F.col("is_conversion") == True)
        assert conversions.count() >= 1


class TestGoldTransforms:
    def _setup(self, spark, tmp_data_dir):
        _write_bronze_campaigns(spark, tmp_data_dir / "bronze")
        _write_bronze_events(spark, tmp_data_dir / "bronze")
        _write_bronze_customers(spark, tmp_data_dir / "bronze")
        from pipelines import transform
        transform.bronze_to_silver_customers(spark)
        transform.bronze_to_silver_events(spark)
        transform.bronze_to_silver_campaigns(spark)

    def test_campaign_metrics_ctr_range(self, spark, tmp_data_dir):
        from pipelines import transform
        self._setup(spark, tmp_data_dir)
        df = transform.silver_to_gold_campaign_metrics(spark)
        ctr_vals = [r["ctr"] for r in df.collect() if r["ctr"] is not None]
        assert all(0 <= v <= 100 for v in ctr_vals)

    def test_campaign_metrics_has_campaign_name(self, spark, tmp_data_dir):
        from pipelines import transform
        self._setup(spark, tmp_data_dir)
        df = transform.silver_to_gold_campaign_metrics(spark)
        assert df.count() >= 1
        assert "name" in df.columns

    def test_customer_ltv_non_negative(self, spark, tmp_data_dir):
        from pipelines import transform
        self._setup(spark, tmp_data_dir)
        df = transform.silver_to_gold_customer_ltv(spark)
        neg = df.filter(F.col("total_revenue") < 0).count()
        assert neg == 0
