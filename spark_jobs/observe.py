"""Day 20: observability — structured JSON logging + a run-metrics store.

Two primitives every job shares:

  1. ``JobLogger`` — emits one **JSON object per log line** to stdout (job, event,
     level, ts, + arbitrary fields). Machine-parseable logs are the difference
     between "grep the console" and "query your operations". Spark's own log4j
     output stays at WARN; these are the *application* events.
  2. ``record_run`` — appends a row to the **observability table**
     (``s3a://gold/_observability/runs``) so volume, freshness, and quality are
     queryable after the fact, not just visible in a scrollback buffer.

Maps to the five observability pillars (freshness, volume, schema, lineage,
quality) — see ``docs/observability.md``.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUN_METRICS_PATH = os.getenv("OBS_METRICS_PATH", "s3a://gold/_observability/runs")

RUN_METRICS_SCHEMA = (
    "job STRING, layer STRING, event STRING, rows LONG, "
    "dq_passed INT, dq_failed INT, duration_seconds DOUBLE, "
    "freshness_seconds DOUBLE, ts TIMESTAMP"
)


class _JsonFormatter(logging.Formatter):
    """Render each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "job": getattr(record, "job", record.name),
            "event": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class JobLogger:
    """Structured logger scoped to one job. ``log.event("started", rows=10)``."""

    def __init__(self, job: str) -> None:
        self.job = job
        self._logger = logging.getLogger(f"lakehouse.{job}")
        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(_JsonFormatter())
            self._logger.addHandler(handler)
            self._logger.setLevel(logging.INFO)
            self._logger.propagate = False

    def event(self, event: str, level: int = logging.INFO, **fields: Any) -> None:
        self._logger.log(level, event, extra={"job": self.job, "fields": fields})

    def warning(self, event: str, **fields: Any) -> None:
        self.event(event, level=logging.WARNING, **fields)

    def error(self, event: str, exc: bool = False, **fields: Any) -> None:
        self._logger.error(event, extra={"job": self.job, "fields": fields}, exc_info=exc)


def _sum_offsets(offset_map: Any) -> int | None:
    """Sum per-partition offsets across topics; None if not available."""
    if not isinstance(offset_map, dict):
        return None
    total = 0
    for parts in offset_map.values():
        if isinstance(parts, dict):
            for off in parts.values():
                try:
                    total += int(off)
                except (TypeError, ValueError):
                    continue
    return total


def kafka_lag(progress: dict[str, Any]) -> int | None:
    """Consumer lag = latest available offsets - offsets processed this batch."""
    for src in progress.get("sources") or []:
        latest = _sum_offsets(src.get("latestOffset"))
        end = _sum_offsets(src.get("endOffset"))
        if latest is not None and end is not None:
            return max(latest - end, 0)
    return None


def stream_progress_fields(progress: dict[str, Any]) -> dict[str, Any]:
    """Extract lag/latency metrics from a StreamingQuery.lastProgress dict."""
    fields: dict[str, Any] = {
        "batch_id": progress.get("batchId"),
        "input_rows": progress.get("numInputRows"),
        "input_rows_per_sec": progress.get("inputRowsPerSecond"),
        "processed_rows_per_sec": progress.get("processedRowsPerSecond"),
        "batch_duration_ms": progress.get("batchDuration"),
    }
    lag = kafka_lag(progress)
    if lag is not None:
        fields["kafka_lag"] = lag
    return {k: v for k, v in fields.items() if v is not None}


def load_prior_rows(spark: SparkSession, layer: str) -> int | None:
    """Most recent recorded row count for a layer — the drift baseline.

    Reads the observability table so it is the single metrics store (no separate
    ``_dq/metrics`` write in the memory-tight Airflow path).
    """
    try:
        hist = (
            spark.read.format("delta")
            .load(RUN_METRICS_PATH)
            .where((F.col("layer") == layer) & F.col("rows").isNotNull())
            .orderBy(F.col("ts").desc())
            .limit(1)
            .collect()
        )
    except Exception:
        return None
    if not hist:
        return None
    return int(hist[0]["rows"])


def record_run(
    spark: SparkSession,
    *,
    job: str,
    layer: str,
    event: str,
    rows: int | None = None,
    dq_passed: int | None = None,
    dq_failed: int | None = None,
    duration_seconds: float | None = None,
    freshness_seconds: float | None = None,
) -> None:
    """Append one observability row. Never raises — monitoring must not break jobs."""
    try:
        row = spark.createDataFrame(
            [
                (
                    job,
                    layer,
                    event,
                    rows,
                    dq_passed,
                    dq_failed,
                    duration_seconds,
                    freshness_seconds,
                    datetime.now(tz=timezone.utc),
                )
            ],
            RUN_METRICS_SCHEMA,
        )
        try:
            row.write.format("delta").mode("append").save(RUN_METRICS_PATH)
        except Exception:
            row.write.format("delta").mode("overwrite").save(RUN_METRICS_PATH)
    except Exception as err:  # pragma: no cover - best-effort telemetry
        JobLogger(job).warning("record_run_failed", error=str(err))
