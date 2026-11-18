"""Tests for the data quality check framework."""
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from pipelines.quality import (QualityError, check_no_duplicates, check_null_rates,
                                check_referential_integrity, check_value_range)


@pytest.fixture()
def clean_df(spark):
    schema = StructType([
        StructField("id", StringType()),
        StructField("country", StringType()),
        StructField("spend", DoubleType()),
    ])
    return spark.createDataFrame(
        [("A", "NG", 100.0), ("B", "GH", 200.0), ("C", "KE", 50.0)],
        schema,
    )


@pytest.fixture()
def dirty_df(spark):
    schema = StructType([
        StructField("id", StringType()),
        StructField("country", StringType()),
        StructField("spend", DoubleType()),
    ])
    return spark.createDataFrame(
        [("A", "NG", 100.0), ("A", "GH", 200.0), ("C", None, -10.0)],
        schema,
    )


class TestNullRates:
    def test_passes_when_no_nulls(self, spark, clean_df):
        results = check_null_rates(clean_df, ["id", "country"], 0.0, "silver", "test")
        assert all(r.passed for r in results)

    def test_fails_when_nulls_exceed_threshold(self, spark, dirty_df):
        results = check_null_rates(dirty_df, ["country"], 0.0, "silver", "test")
        assert not results[0].passed

    def test_passes_with_permissive_threshold(self, spark, dirty_df):
        results = check_null_rates(dirty_df, ["country"], 0.5, "silver", "test")
        assert results[0].passed


class TestNoDuplicates:
    def test_passes_on_unique_keys(self, spark, clean_df):
        result = check_no_duplicates(clean_df, "id", "silver", "test")
        assert result.passed

    def test_fails_on_duplicate_keys(self, spark, dirty_df):
        result = check_no_duplicates(dirty_df, "id", "silver", "test")
        assert not result.passed


class TestValueRange:
    def test_passes_within_range(self, spark, clean_df):
        result = check_value_range(clean_df, "spend", 0, 1000, "gold", "test")
        assert result.passed

    def test_fails_outside_range(self, spark, dirty_df):
        result = check_value_range(dirty_df, "spend", 0, 1000, "gold", "test")
        assert not result.passed  # -10.0 is out of range


class TestReferentialIntegrity:
    def test_passes_when_all_keys_match(self, spark):
        parent = spark.createDataFrame([("C001",), ("C002",)], ["customer_id"])
        child = spark.createDataFrame([("C001",), ("C002",)], ["cust_ref"])
        result = check_referential_integrity(spark, child, "cust_ref", parent, "customer_id", "silver", "events")
        assert result.passed

    def test_fails_with_orphaned_keys(self, spark):
        parent = spark.createDataFrame([("C001",)], ["customer_id"])
        child = spark.createDataFrame([("C001",), ("C999",)], ["cust_ref"])  # C999 is orphan
        result = check_referential_integrity(spark, child, "cust_ref", parent, "customer_id", "silver", "events")
        assert not result.passed
        assert "1 orphaned" in result.detail
