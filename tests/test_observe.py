"""Tests for the observability helpers (spark_jobs/observe.py).

These cover the pure streaming-lag / latency math extracted from a
``StreamingQuery.lastProgress`` dict — the numbers that answer "is the consumer
keeping up?" — with no SparkSession required. (Metric *writes* live in the Spark
tier; see tests/spark/.)
"""

from __future__ import annotations

from observe import _sum_offsets, kafka_lag, stream_progress_fields

# --- offset summation ------------------------------------------------------


def test_sum_offsets_adds_across_topics_and_partitions() -> None:
    offsets = {
        "crypto.trades.raw": {"0": 100, "1": 250},
        "other.topic": {"0": 50},
    }
    assert _sum_offsets(offsets) == 400


def test_sum_offsets_coerces_string_offsets() -> None:
    assert _sum_offsets({"t": {"0": "10", "1": "5"}}) == 15


def test_sum_offsets_returns_none_for_non_mapping() -> None:
    assert _sum_offsets(None) is None
    assert _sum_offsets("nope") is None


# --- consumer lag ----------------------------------------------------------


def test_kafka_lag_is_latest_minus_processed() -> None:
    progress = {
        "sources": [
            {
                "latestOffset": {"crypto.trades.raw": {"0": 1000}},
                "endOffset": {"crypto.trades.raw": {"0": 940}},
            }
        ]
    }
    assert kafka_lag(progress) == 60


def test_kafka_lag_never_negative() -> None:
    # A rebalance can momentarily make endOffset look ahead of latestOffset.
    progress = {
        "sources": [
            {
                "latestOffset": {"t": {"0": 100}},
                "endOffset": {"t": {"0": 120}},
            }
        ]
    }
    assert kafka_lag(progress) == 0


def test_kafka_lag_is_none_when_offsets_absent() -> None:
    assert kafka_lag({"sources": [{}]}) is None
    assert kafka_lag({}) is None


# --- progress field extraction --------------------------------------------


def test_stream_progress_fields_extracts_throughput_and_latency() -> None:
    progress = {
        "batchId": 7,
        "numInputRows": 5000,
        "inputRowsPerSecond": 1200.5,
        "processedRowsPerSecond": 1180.0,
        "batchDuration": 4200,
        "sources": [
            {
                "latestOffset": {"t": {"0": 1000}},
                "endOffset": {"t": {"0": 990}},
            }
        ],
    }
    fields = stream_progress_fields(progress)
    assert fields["batch_id"] == 7
    assert fields["input_rows"] == 5000
    assert fields["input_rows_per_sec"] == 1200.5
    assert fields["processed_rows_per_sec"] == 1180.0
    assert fields["batch_duration_ms"] == 4200
    assert fields["kafka_lag"] == 10


def test_stream_progress_fields_drops_missing_values() -> None:
    # batchId 0 is falsy but valid and must survive; None values are dropped.
    fields = stream_progress_fields({"batchId": 0, "numInputRows": None})
    assert fields["batch_id"] == 0
    assert "input_rows" not in fields
    assert "kafka_lag" not in fields
