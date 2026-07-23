"""Serving-API tests — exercise the full HTTP surface with a fake store.

These are fast-gate tests: no MinIO, no Spark, no JVM. We override the store and
settings dependencies with in-memory fakes so we can assert on routing, query
validation, field mapping/serialization, and that query params reach the store.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from serving.config import Settings
from serving.main import app, get_settings, get_store

TS = datetime(2026, 7, 18, 12, 0, tzinfo=UTC)

CANDLE: dict[str, Any] = {
    "product_id": "BTC-USD",
    "interval_start": TS,
    "interval_end": datetime(2026, 7, 18, 12, 1, tzinfo=UTC),
    "open": Decimal("60000.00"),
    "high": Decimal("60100.00"),
    "low": Decimal("59900.00"),
    "close": Decimal("60050.00"),
    "volume": Decimal("1.5"),
    "vwap": 60010.5,
    "trade_count": 42,
}
REALTIME: dict[str, Any] = {
    "product_id": "BTC-USD",
    "window_start": TS,
    "window_end": datetime(2026, 7, 18, 12, 1, tzinfo=UTC),
    "trade_count": 10,
    "volume": Decimal("0.75"),
    "vwap": 60005.0,
    "price_volatility": 3.2,
}
RUN: dict[str, Any] = {
    "job": "gold-aggregate",
    "layer": "gold",
    "event": "candles_written",
    "rows": 1000,
    "dq_passed": 5,
    "dq_failed": 0,
    "duration_seconds": 12.3,
    "freshness_seconds": 45.0,
    "ts": TS,
}


class FakeStore:
    """In-memory stand-in for DeltaStore that records the args it's called with."""

    def __init__(self) -> None:
        self.calls: dict[str, tuple[Any, ...]] = {}

    def candles(
        self, product: str, start: datetime | None, end: datetime | None, limit: int
    ) -> list[dict[str, Any]]:
        self.calls["candles"] = (product, start, end, limit)
        return [CANDLE]

    def realtime(self, product: str | None, limit: int) -> list[dict[str, Any]]:
        self.calls["realtime"] = (product, limit)
        return [REALTIME]

    def runs(self, limit: int) -> list[dict[str, Any]]:
        self.calls["runs"] = (limit,)
        return [RUN]

    def table_status(self) -> dict[str, bool]:
        return {"ohlc": True, "realtime_metrics": True, "observability_runs": False}


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def client(store: FakeStore) -> Iterator[TestClient]:
    settings = Settings(
        ohlc_uri="s3://gold/ohlc",
        realtime_uri="s3://gold/realtime_metrics",
        runs_uri="s3://gold/_observability/runs",
        products=("BTC-USD", "ETH-USD"),
        storage_options={},
    )
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_settings] = lambda: settings
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_health_reports_table_status(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["tables"] == {
        "ohlc": True,
        "realtime_metrics": True,
        "observability_runs": False,
    }


def test_products_lists_configured(client: TestClient) -> None:
    resp = client.get("/products")
    assert resp.status_code == 200
    assert resp.json() == {"products": ["BTC-USD", "ETH-USD"]}


def test_candles_maps_fields_and_preserves_decimals(client: TestClient) -> None:
    resp = client.get("/candles", params={"product": "BTC-USD"})
    assert resp.status_code == 200
    [candle] = resp.json()
    assert candle["product_id"] == "BTC-USD"
    assert candle["trade_count"] == 42
    # Money stays exact: Decimal serializes as a string, not a lossy float.
    assert candle["open"] == "60000.00"
    assert candle["vwap"] == 60010.5


def test_candles_forwards_query_params_to_store(client: TestClient, store: FakeStore) -> None:
    resp = client.get(
        "/candles",
        params={
            "product": "ETH-USD",
            "start": "2026-07-18T00:00:00Z",
            "end": "2026-07-18T23:59:59Z",
            "limit": 25,
        },
    )
    assert resp.status_code == 200
    product, start, end, limit = store.calls["candles"]
    assert product == "ETH-USD"
    assert limit == 25
    assert start == datetime(2026, 7, 18, 0, 0, tzinfo=UTC)
    assert end == datetime(2026, 7, 18, 23, 59, 59, tzinfo=UTC)


def test_candles_requires_product(client: TestClient) -> None:
    assert client.get("/candles").status_code == 422


@pytest.mark.parametrize("limit", [0, 6000])
def test_candles_rejects_out_of_range_limit(client: TestClient, limit: int) -> None:
    resp = client.get("/candles", params={"product": "BTC-USD", "limit": limit})
    assert resp.status_code == 422


def test_realtime_optional_product(client: TestClient, store: FakeStore) -> None:
    resp = client.get("/metrics/realtime", params={"limit": 5})
    assert resp.status_code == 200
    assert store.calls["realtime"] == (None, 5)
    assert resp.json()[0]["price_volatility"] == 3.2


def test_runs_returns_records(client: TestClient, store: FakeStore) -> None:
    resp = client.get("/metrics/runs", params={"limit": 10})
    assert resp.status_code == 200
    assert store.calls["runs"] == (10,)
    assert resp.json()[0]["job"] == "gold-aggregate"
