"""Real Spark tests for the data-quality checks (dq.py).

Builds small silver/gold DataFrames with deliberately planted defects and asserts
that the corresponding checks fail (and that clean data passes). This is the
un-fakeable core of "is the data trustworthy?".
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("pyspark")

from dq import CheckResult, check_gold, check_silver

_SILVER_SCHEMA = (
    "product_id STRING, trade_id LONG, side STRING, price DOUBLE, size DOUBLE, event_time TIMESTAMP"
)
_GOLD_SCHEMA = (
    "product_id STRING, interval_start TIMESTAMP, "
    "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE"
)


def _by_name(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {r.name: r for r in results}


def _recent() -> datetime:
    return datetime.now(tz=UTC) - timedelta(minutes=5)


# --- silver ----------------------------------------------------------------


def _silver_row(**overrides: object) -> dict[str, object]:
    row = {
        "product_id": "BTC-USD",
        "trade_id": 1,
        "side": "buy",
        "price": 100.0,
        "size": 0.5,
        "event_time": _recent(),
    }
    row.update(overrides)
    return row


def test_clean_silver_passes_every_check(spark) -> None:  # type: ignore[no-untyped-def]
    rows = [_silver_row(trade_id=1), _silver_row(trade_id=2, side="sell")]
    df = spark.createDataFrame(rows, _SILVER_SCHEMA)
    results = _by_name(check_silver(df, row_count=2, prior_row_count=2))
    assert all(r.passed for r in results.values()), {
        n: r.detail for n, r in results.items() if not r.passed
    }


def test_silver_flags_duplicate_dedup_keys(spark) -> None:  # type: ignore[no-untyped-def]
    # Two rows sharing (product_id, trade_id) — the exact duplicate that the
    # watermark dedup is meant to remove upstream.
    rows = [_silver_row(trade_id=7), _silver_row(trade_id=7)]
    df = spark.createDataFrame(rows, _SILVER_SCHEMA)
    results = _by_name(check_silver(df, row_count=2, prior_row_count=None))
    assert results["unique_dedup_key"].passed is False


def test_silver_flags_non_positive_price_and_size(spark) -> None:  # type: ignore[no-untyped-def]
    rows = [_silver_row(trade_id=1, price=0.0), _silver_row(trade_id=2, size=-1.0)]
    df = spark.createDataFrame(rows, _SILVER_SCHEMA)
    results = _by_name(check_silver(df, row_count=2, prior_row_count=None))
    assert results["price_positive"].passed is False
    assert results["size_positive"].passed is False


def test_silver_flags_invalid_side(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([_silver_row(side="long")], _SILVER_SCHEMA)
    results = _by_name(check_silver(df, row_count=1, prior_row_count=None))
    assert results["side_valid"].passed is False


def test_silver_freshness_fails_on_stale_data(spark) -> None:  # type: ignore[no-untyped-def]
    stale = datetime(2000, 1, 1, tzinfo=UTC)
    df = spark.createDataFrame([_silver_row(event_time=stale)], _SILVER_SCHEMA)
    results = _by_name(check_silver(df, row_count=1, prior_row_count=None))
    assert results["freshness"].passed is False


def test_silver_row_count_drift_fails_on_a_big_drop(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([_silver_row()], _SILVER_SCHEMA)
    # 1 row now vs 100 before is a >5% drop -> drift check fails.
    results = _by_name(check_silver(df, row_count=1, prior_row_count=100))
    assert results["row_count_drift"].passed is False


# --- gold ------------------------------------------------------------------


def _gold_row(**overrides: object) -> dict[str, object]:
    row = {
        "product_id": "BTC-USD",
        "interval_start": _recent(),
        "open": 100.0,
        "high": 110.0,
        "low": 95.0,
        "close": 105.0,
        "volume": 12.0,
    }
    row.update(overrides)
    return row


def test_clean_gold_passes_every_check(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([_gold_row()], _GOLD_SCHEMA)
    results = _by_name(check_gold(df, row_count=1, prior_row_count=1))
    assert all(r.passed for r in results.values()), {
        n: r.detail for n, r in results.items() if not r.passed
    }


def test_gold_flags_impossible_ohlc(spark) -> None:  # type: ignore[no-untyped-def]
    # high < low is physically impossible for a candle.
    df = spark.createDataFrame([_gold_row(high=90.0, low=95.0)], _GOLD_SCHEMA)
    results = _by_name(check_gold(df, row_count=1, prior_row_count=None))
    assert results["ohlc_sane"].passed is False


def test_gold_flags_negative_volume(spark) -> None:  # type: ignore[no-untyped-def]
    df = spark.createDataFrame([_gold_row(volume=-1.0)], _GOLD_SCHEMA)
    results = _by_name(check_gold(df, row_count=1, prior_row_count=None))
    assert results["volume_non_negative"].passed is False
