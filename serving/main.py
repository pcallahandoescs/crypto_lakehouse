"""FastAPI app — the lakehouse's read API over gold.

Endpoints (all read-only):

  * ``GET /health``            liveness + per-table reachability
  * ``GET /products``          the products this deployment serves
  * ``GET /candles``           historical OHLC candles (gold/ohlc)
  * ``GET /metrics/realtime``  latest speed-layer windows (gold/realtime_metrics)
  * ``GET /metrics/runs``      recent job-run observability records

The store and settings are provided via FastAPI dependencies (and cached), so
tests can override them with a fake — no MinIO or Spark needed to exercise the
HTTP surface. Interactive docs are served at ``/docs``.
"""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache

from fastapi import Depends, FastAPI, Query

from serving.config import Settings, load_settings
from serving.models import Candle, Health, RealtimeMetric, RunMetric
from serving.store import DeltaStore


@lru_cache
def get_settings() -> Settings:
    """Resolve config once per process."""
    return load_settings()


@lru_cache
def get_store() -> DeltaStore:
    """Single shared store (dependency-overridable in tests)."""
    return DeltaStore(get_settings())


app = FastAPI(
    title="Crypto Lakehouse API",
    version="0.1.0",
    summary="Read-only access to the gold layer: OHLC candles and real-time metrics.",
)


@app.get("/health", response_model=Health, tags=["ops"])
def health(store: DeltaStore = Depends(get_store)) -> Health:
    """Liveness plus which gold tables are currently reachable."""
    return Health(status="ok", tables=store.table_status())


@app.get("/products", tags=["reference"])
def products(settings: Settings = Depends(get_settings)) -> dict[str, list[str]]:
    """The product ids this deployment is configured to serve."""
    return {"products": list(settings.products)}


@app.get("/candles", response_model=list[Candle], tags=["market-data"])
def candles(
    product: str = Query(..., description="Product id, e.g. BTC-USD", examples=["BTC-USD"]),
    start: datetime | None = Query(None, description="Inclusive lower bound on interval_start"),
    end: datetime | None = Query(None, description="Inclusive upper bound on interval_start"),
    limit: int = Query(200, ge=1, le=5000, description="Max candles to return (newest first)"),
    store: DeltaStore = Depends(get_store),
) -> list[Candle]:
    """Historical OHLC/VWAP candles for a product, newest first."""
    return [Candle(**row) for row in store.candles(product, start, end, limit)]


@app.get("/metrics/realtime", response_model=list[RealtimeMetric], tags=["market-data"])
def realtime(
    product: str | None = Query(None, description="Optional product filter, e.g. BTC-USD"),
    limit: int = Query(100, ge=1, le=5000, description="Max windows to return (newest first)"),
    store: DeltaStore = Depends(get_store),
) -> list[RealtimeMetric]:
    """Latest speed-layer windowed metrics (near-real-time)."""
    return [RealtimeMetric(**row) for row in store.realtime(product, limit)]


@app.get("/metrics/runs", response_model=list[RunMetric], tags=["ops"])
def runs(
    limit: int = Query(50, ge=1, le=1000, description="Max run records (newest first)"),
    store: DeltaStore = Depends(get_store),
) -> list[RunMetric]:
    """Recent pipeline job-run records from the observability metrics table."""
    return [RunMetric(**row) for row in store.runs(limit)]
