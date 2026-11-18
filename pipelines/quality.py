"""Data quality checks across all layers."""
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.config import BRONZE, GOLD, SILVER


@dataclass
class QualityResult:
    layer: str
    table: str
    check: str
    passed: bool
    detail: str = ""
    failures: list[str] = field(default_factory=list)


class QualityError(Exception):
    pass


def _null_rate(df: DataFrame, col: str) -> float:
    total = df.count()
    if total == 0:
        return 0.0
    nulls = df.filter(F.col(col).isNull()).count()
    return nulls / total


def check_null_rates(df: DataFrame, columns: list[str], threshold: float, layer: str, table: str) -> list[QualityResult]:
    results = []
    for col in columns:
        rate = _null_rate(df, col)
        passed = rate <= threshold
        results.append(QualityResult(
            layer=layer, table=table,
            check=f"null_rate.{col}",
            passed=passed,
            detail=f"{rate:.2%} null (threshold {threshold:.0%})",
        ))
    return results


def check_no_duplicates(df: DataFrame, key_col: str, layer: str, table: str) -> QualityResult:
    total = df.count()
    distinct = df.select(key_col).distinct().count()
    passed = total == distinct
    return QualityResult(
        layer=layer, table=table,
        check=f"no_duplicates.{key_col}",
        passed=passed,
        detail=f"{total} rows, {distinct} distinct keys",
    )


def check_referential_integrity(
    spark: SparkSession,
    child_df: DataFrame, child_col: str,
    parent_df: DataFrame, parent_col: str,
    layer: str, table: str,
) -> QualityResult:
    orphans = (
        child_df.select(child_col)
        .distinct()
        .join(parent_df.select(parent_col), child_df[child_col] == parent_df[parent_col], "left_anti")
    )
    count = orphans.count()
    passed = count == 0
    return QualityResult(
        layer=layer, table=table,
        check=f"referential_integrity.{child_col}->{parent_col}",
        passed=passed,
        detail=f"{count} orphaned keys",
    )


def check_value_range(df: DataFrame, col: str, min_val, max_val, layer: str, table: str) -> QualityResult:
    out_of_range = df.filter(
        (F.col(col) < min_val) | (F.col(col) > max_val)
    ).count()
    passed = out_of_range == 0
    return QualityResult(
        layer=layer, table=table,
        check=f"value_range.{col}[{min_val},{max_val}]",
        passed=passed,
        detail=f"{out_of_range} rows outside range",
    )


def run_all(spark: SparkSession, raise_on_failure: bool = False) -> list[QualityResult]:
    results: list[QualityResult] = []

    # Silver customers
    customers = spark.read.parquet(str(SILVER / "customers"))
    results += check_null_rates(customers, ["customer_id", "country"], 0.0, "silver", "customers")
    results.append(check_no_duplicates(customers, "customer_id", "silver", "customers"))
    results.append(check_value_range(customers, "total_spend", 0, 1_000_000, "silver", "customers"))

    # Silver events
    events = spark.read.parquet(str(SILVER / "campaign_events"))
    results += check_null_rates(events, ["event_id", "customer_id", "campaign_id", "event_type"], 0.0, "silver", "events")
    results.append(check_no_duplicates(events, "event_id", "silver", "events"))

    # Referential integrity: events → customers
    results.append(check_referential_integrity(
        spark, events, "customer_id", customers, "customer_id", "silver", "events"
    ))

    # Gold campaign metrics
    metrics = spark.read.parquet(str(GOLD / "campaign_metrics"))
    results.append(check_value_range(metrics, "ctr", 0, 100, "gold", "campaign_metrics"))
    results.append(check_value_range(metrics, "cvr", 0, 100, "gold", "campaign_metrics"))

    # Print summary
    passed = sum(1 for r in results if r.passed)
    failed = [r for r in results if not r.passed]
    print(f"\n  Quality: {passed}/{len(results)} checks passed")
    for f in failed:
        print(f"  ✗ FAIL [{f.layer}.{f.table}] {f.check}: {f.detail}")

    if raise_on_failure and failed:
        raise QualityError(f"{len(failed)} quality check(s) failed: {[r.check for r in failed]}")

    return results
