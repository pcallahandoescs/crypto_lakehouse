"""Data access — read the gold Delta tables with delta-rs.

Everything the API serves flows through this one class, which keeps two nice
properties:

  * **No JVM.** ``deltalake`` (delta-rs) reads the ``_delta_log`` and Parquet
    directly from MinIO and hands back Apache Arrow — opening a table is cheap
    enough to do per request.
  * **Testable.** The endpoints depend on this class, so the fast test gate can
    swap in a fake and exercise all the HTTP behaviour without MinIO or Spark.

Filtering strategy: push the cheap, high-selectivity ``product_id`` equality down
to Arrow (predicate pushdown), then do the time-range clip, ordering, and limit
in Python — gold candle counts per product are small, so this stays simple and
correct rather than fighting Arrow's timezone handling in a pushed-down filter.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pyarrow.dataset as ds
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from serving.config import Settings

_CANDLE_COLS = [
    "product_id",
    "interval_start",
    "interval_end",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "trade_count",
]
_REALTIME_COLS = [
    "product_id",
    "window_start",
    "window_end",
    "trade_count",
    "volume",
    "vwap",
    "price_volatility",
]


def _as_utc(value: datetime) -> datetime:
    """Coerce a possibly-naive datetime to timezone-aware UTC for comparison."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


class DeltaStore:
    """Read-only accessor over the gold Delta tables."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    # -- internal ------------------------------------------------------------

    def _read(
        self,
        uri: str,
        *,
        columns: list[str] | None = None,
        product: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return table rows as dicts, or ``[]`` if the table isn't created yet.

        A missing table is a normal early-lifecycle state (a job hasn't run),
        not an error — so we return empty rather than 500.
        """
        try:
            table = DeltaTable(uri, storage_options=self._settings.storage_options)
        except TableNotFoundError:
            return []
        dataset = table.to_pyarrow_dataset()
        expr = ds.field("product_id") == product if product is not None else None
        arrow = dataset.to_table(columns=columns, filter=expr)
        rows: list[dict[str, Any]] = arrow.to_pylist()
        return rows

    # -- queries -------------------------------------------------------------

    def candles(
        self,
        product: str,
        start: datetime | None,
        end: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Historical OHLC candles for a product, newest first, time-clipped."""
        rows = self._read(self._settings.ohlc_uri, columns=_CANDLE_COLS, product=product)
        if start is not None:
            lo = _as_utc(start)
            rows = [r for r in rows if _as_utc(r["interval_start"]) >= lo]
        if end is not None:
            hi = _as_utc(end)
            rows = [r for r in rows if _as_utc(r["interval_start"]) <= hi]
        rows.sort(key=lambda r: r["interval_start"], reverse=True)
        return rows[:limit]

    def realtime(self, product: str | None, limit: int) -> list[dict[str, Any]]:
        """Latest speed-layer windows, newest first (optionally one product)."""
        rows = self._read(self._settings.realtime_uri, columns=_REALTIME_COLS, product=product)
        rows.sort(key=lambda r: r["window_start"], reverse=True)
        return rows[:limit]

    def runs(self, limit: int) -> list[dict[str, Any]]:
        """Most recent job-run observability records, newest first."""
        rows = self._read(self._settings.runs_uri)
        rows.sort(key=lambda r: r["ts"], reverse=True)
        return rows[:limit]

    def table_status(self) -> dict[str, bool]:
        """Per-table reachability, for the health probe."""
        return {
            "ohlc": self._exists(self._settings.ohlc_uri),
            "realtime_metrics": self._exists(self._settings.realtime_uri),
            "observability_runs": self._exists(self._settings.runs_uri),
        }

    def _exists(self, uri: str) -> bool:
        exists: bool = DeltaTable.is_deltatable(uri, storage_options=self._settings.storage_options)
        return exists
