"""Day 15: data quality checks for silver and gold Delta tables.

Custom PySpark checks (Great Expectations is the scale alternative — same ideas,
heavier ops). Each check returns a CheckResult; failures are logged as alerts.
Row-level violations can be quarantined instead of silently dropped.

Quality dimensions covered here (of the classic six):
  - Completeness: null/key checks
  - Uniqueness: grain / dedup key
  - Validity: value ranges (price > 0, volume >= 0, side enum, OHLC sanity)
  - Consistency: schema columns present (implicit via typed reads)
  - Timeliness: freshness (max event_time vs now)
  - Accuracy: row-count drift vs prior run (sanity, not truth)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

METRICS_PATH = "s3a://gold/_dq/metrics"
SILVER_QUARANTINE_PATH = "s3a://silver/quarantine/trades"
GOLD_QUARANTINE_PATH = "s3a://gold/quarantine/ohlc"

# How stale silver may be before the freshness check fails. Default 7 days so a
# stopped local producer doesn't fail DQ during dev; override for prod.
FRESHNESS_HOURS = float(os.getenv("DQ_FRESHNESS_HOURS", "168"))
# Row count may not drop more than this fraction vs the last recorded run.
MIN_ROW_COUNT_RATIO = float(os.getenv("DQ_MIN_ROW_COUNT_RATIO", "0.95"))


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def alert(results: list[CheckResult], layer: str) -> bool:
    """Log check outcomes. Returns True if every check passed."""
    print(f"\n---- DQ report: {layer} ----")
    failed = 0
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"  [{status}] {r.name}: {r.detail}")
        if not r.passed:
            failed += 1
    verdict = "OK" if failed == 0 else f"{failed} FAILED"
    print(f"  verdict: {verdict}")
    return failed == 0


def _count_violations(df: DataFrame, condition: F.Column) -> int:
    return df.where(condition).count()


def check_silver(df: DataFrame, row_count: int, prior_row_count: int | None) -> list[CheckResult]:
    """Run silver-layer DQ checks on a DataFrame."""
    results: list[CheckResult] = []

    null_keys = _count_violations(
        df,
        F.col("product_id").isNull() | F.col("trade_id").isNull(),
    )
    results.append(
        CheckResult(
            "null_keys",
            null_keys == 0,
            f"{null_keys} rows with null product_id or trade_id",
        )
    )

    null_money = _count_violations(
        df,
        F.col("price").isNull() | F.col("size").isNull() | F.col("event_time").isNull(),
    )
    results.append(
        CheckResult(
            "null_money_or_time",
            null_money == 0,
            f"{null_money} rows with null price, size, or event_time",
        )
    )

    bad_price = _count_violations(df, F.col("price") <= 0)
    results.append(
        CheckResult(
            "price_positive",
            bad_price == 0,
            f"{bad_price} rows with price <= 0",
        )
    )

    bad_size = _count_violations(df, F.col("size") <= 0)
    results.append(
        CheckResult(
            "size_positive",
            bad_size == 0,
            f"{bad_size} rows with size <= 0",
        )
    )

    bad_side = _count_violations(df, ~F.col("side").isin("buy", "sell"))
    results.append(
        CheckResult(
            "side_valid",
            bad_side == 0,
            f"{bad_side} rows with side not in (buy, sell)",
        )
    )

    distinct = df.select(F.countDistinct("product_id", "trade_id")).collect()[0][0]
    dupes = row_count - distinct
    results.append(
        CheckResult(
            "unique_dedup_key",
            dupes == 0,
            (
                f"{dupes} duplicate (product_id, trade_id) pairs "
                f"({row_count} rows, {distinct} distinct)"
            ),
        )
    )

    max_event = df.agg(F.max("event_time").alias("m")).collect()[0]["m"]
    if max_event is None:
        results.append(CheckResult("freshness", False, "no event_time values"))
    else:
        # Spark may return naive UTC timestamps.
        if max_event.tzinfo is None:
            max_event = max_event.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(tz=timezone.utc) - max_event).total_seconds() / 3600
        ok = age_hours <= FRESHNESS_HOURS
        results.append(
            CheckResult(
                "freshness",
                ok,
                f"latest event_time {age_hours:.1f}h ago (limit {FRESHNESS_HOURS}h)",
            )
        )

    if prior_row_count is None:
        results.append(CheckResult("row_count_drift", True, "no prior baseline"))
    else:
        ratio = row_count / prior_row_count if prior_row_count else 1.0
        ok = ratio >= MIN_ROW_COUNT_RATIO
        results.append(
            CheckResult(
                "row_count_drift",
                ok,
                (
                    f"{row_count} rows vs prior {prior_row_count} "
                    f"(ratio {ratio:.3f}, min {MIN_ROW_COUNT_RATIO})"
                ),
            )
        )

    return results


def check_gold(df: DataFrame, row_count: int, prior_row_count: int | None) -> list[CheckResult]:
    """Run gold-layer DQ checks on OHLC candles."""
    results: list[CheckResult] = []

    null_grain = _count_violations(
        df,
        F.col("product_id").isNull() | F.col("interval_start").isNull(),
    )
    results.append(
        CheckResult(
            "null_grain",
            null_grain == 0,
            f"{null_grain} rows with null product_id or interval_start",
        )
    )

    distinct = df.select(F.countDistinct("product_id", "interval_start")).collect()[0][0]
    dupes = row_count - distinct
    results.append(
        CheckResult(
            "grain_unique",
            dupes == 0,
            (
                f"{dupes} duplicate (product_id, interval_start) "
                f"({row_count} rows, {distinct} distinct)"
            ),
        )
    )

    null_ohlc = _count_violations(
        df,
        F.col("open").isNull()
        | F.col("high").isNull()
        | F.col("low").isNull()
        | F.col("close").isNull(),
    )
    results.append(
        CheckResult(
            "null_ohlc",
            null_ohlc == 0,
            f"{null_ohlc} rows with null OHLC",
        )
    )

    bad_volume = _count_violations(df, F.col("volume") < 0)
    results.append(
        CheckResult(
            "volume_non_negative",
            bad_volume == 0,
            f"{bad_volume} rows with volume < 0",
        )
    )

    bad_ohlc = _count_violations(
        df,
        (F.col("high") < F.col("low"))
        | (F.col("open") > F.col("high"))
        | (F.col("open") < F.col("low"))
        | (F.col("close") > F.col("high"))
        | (F.col("close") < F.col("low")),
    )
    results.append(
        CheckResult(
            "ohlc_sane",
            bad_ohlc == 0,
            f"{bad_ohlc} rows where OHLC violates high/low bounds",
        )
    )

    if prior_row_count is None:
        results.append(CheckResult("row_count_drift", True, "no prior baseline"))
    else:
        ratio = row_count / prior_row_count if prior_row_count else 1.0
        ok = ratio >= MIN_ROW_COUNT_RATIO
        results.append(
            CheckResult(
                "row_count_drift",
                ok,
                f"{row_count} rows vs prior {prior_row_count} (ratio {ratio:.3f})",
            )
        )

    return results


def silver_violation_filter() -> F.Column:
    """Rows in silver that fail row-level validity checks."""
    return (
        F.col("product_id").isNull()
        | F.col("trade_id").isNull()
        | F.col("price").isNull()
        | F.col("size").isNull()
        | F.col("event_time").isNull()
        | (F.col("price") <= 0)
        | (F.col("size") <= 0)
        | ~F.col("side").isin("buy", "sell")
    )


def quarantine_silver_violations(spark: SparkSession, silver_path: str) -> int:
    """Move silver rows failing row-level checks to the quarantine table."""
    df = spark.read.format("delta").load(silver_path)
    bad = (
        df.where(silver_violation_filter())
        .withColumn("reason", F.lit("silver_dq_failure"))
        .withColumn("quarantined_at", F.current_timestamp())
    )
    n = bad.count()
    if n > 0:
        bad.write.format("delta").mode("append").save(SILVER_QUARANTINE_PATH)
        print(f"  quarantined {n} silver rows -> {SILVER_QUARANTINE_PATH}")
    return n


def quarantine_gold_violations(spark: SparkSession, gold_path: str) -> int:
    """Move gold rows failing OHLC/grain checks to quarantine."""
    df = spark.read.format("delta").load(gold_path)
    bad = (
        df.where(
            F.col("product_id").isNull()
            | F.col("interval_start").isNull()
            | F.col("open").isNull()
            | F.col("high").isNull()
            | F.col("low").isNull()
            | F.col("close").isNull()
            | (F.col("volume") < 0)
            | (F.col("high") < F.col("low"))
            | (F.col("open") > F.col("high"))
            | (F.col("open") < F.col("low"))
            | (F.col("close") > F.col("high"))
            | (F.col("close") < F.col("low"))
        )
        .withColumn("reason", F.lit("gold_dq_failure"))
        .withColumn("quarantined_at", F.current_timestamp())
    )
    n = bad.count()
    if n > 0:
        bad.write.format("delta").mode("append").save(GOLD_QUARANTINE_PATH)
        print(f"  quarantined {n} gold rows -> {GOLD_QUARANTINE_PATH}")
    return n


def load_prior_row_count(spark: SparkSession, table_name: str) -> int | None:
    """Read the last recorded row count for a table, if metrics exist."""
    try:
        hist = (
            spark.read.format("delta")
            .load(METRICS_PATH)
            .where(F.col("table_name") == table_name)
            .orderBy(F.col("checked_at").desc())
            .limit(1)
            .collect()
        )
    except Exception:
        return None
    if not hist:
        return None
    return int(hist[0]["row_count"])


def save_metrics(spark: SparkSession, table_name: str, row_count: int) -> None:
    """Append a row-count snapshot for drift checks on the next run."""
    row = spark.createDataFrame(
        [(table_name, row_count, datetime.now(tz=timezone.utc))],
        "table_name STRING, row_count LONG, checked_at TIMESTAMP",
    )
    try:
        row.write.format("delta").mode("append").save(METRICS_PATH)
    except Exception:
        row.write.format("delta").mode("overwrite").save(METRICS_PATH)
