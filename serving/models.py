"""Response schemas — the API's public data contract.

Prices and volume stay :class:`~decimal.Decimal` end to end: they're stored as
exact ``DECIMAL(38,18)`` in Delta and money must never round-trip through binary
float. VWAP and volatility are derived analytical means, so float is fine there
(and matches how the Spark jobs compute them).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class Candle(BaseModel):
    """One OHLC candle from ``gold/ohlc`` — one row per product per interval."""

    product_id: str
    interval_start: datetime
    interval_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: float | None = None
    trade_count: int


class RealtimeMetric(BaseModel):
    """A speed-layer window from ``gold/realtime_metrics`` (near-real-time)."""

    product_id: str
    window_start: datetime
    window_end: datetime
    trade_count: int
    volume: Decimal
    vwap: float | None = None
    price_volatility: float | None = None


class RunMetric(BaseModel):
    """One job-run record from the observability metrics table."""

    job: str
    layer: str
    event: str
    rows: int | None = None
    dq_passed: int | None = None
    dq_failed: int | None = None
    duration_seconds: float | None = None
    freshness_seconds: float | None = None
    ts: datetime


class Health(BaseModel):
    """Liveness + per-table reachability (a lightweight readiness probe)."""

    status: str
    tables: dict[str, bool]
