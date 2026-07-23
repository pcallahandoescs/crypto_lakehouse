"""Real Spark tests for the bronze -> silver transformation (silver_transform.py).

Runs the actual ``parse_bronze`` / ``conform_trades`` / ``to_quarantine``
functions on small in-memory DataFrames, so the parsing, typing, conforming, and
malformed-row routing are verified against a genuine Spark engine.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

from silver_transform import (
    conform_trades,
    parse_bronze,
    to_quarantine,
)

_INGEST = datetime(2026, 7, 5, 0, 0, 0, tzinfo=UTC)


def _trade_json(**overrides: object) -> str:
    base = {
        "type": "match",
        "trade_id": 1,
        "maker_order_id": "a",
        "taker_order_id": "b",
        "side": "buy",
        "size": "0.5",
        "price": "100.0",
        "product_id": "BTC-USD",
        "sequence": 10,
        "time": "2026-07-04T23:34:58.071539Z",
    }
    base.update(overrides)
    return json.dumps(base)


def _bronze(spark, values: list[str]):  # type: ignore[no-untyped-def]
    rows = [(v, _INGEST) for v in values]
    return spark.createDataFrame(rows, "value STRING, ingest_timestamp TIMESTAMP")


def test_conform_parses_types_and_preserves_exact_decimals(spark) -> None:  # type: ignore[no-untyped-def]
    bronze = _bronze(spark, [_trade_json(trade_id=42, price="63109.18", size="0.00634069")])
    conformed = conform_trades(parse_bronze(bronze)).collect()

    assert len(conformed) == 1
    row = conformed[0]
    assert row["product_id"] == "BTC-USD"
    assert row["trade_id"] == 42
    assert row["side"] == "buy"
    # Money is exact Decimal, never a lossy float.
    assert row["price"] == Decimal("63109.18")
    assert row["size"] == Decimal("0.00634069")
    assert row["event_time"] is not None
    assert row["silver_timestamp"] is not None


def test_conform_drops_rows_missing_required_fields(spark) -> None:  # type: ignore[no-untyped-def]
    # A row whose price is absent parses to a null price -> not a valid trade.
    good = _trade_json(trade_id=1)
    bad = _trade_json(trade_id=2)
    bad_dict = json.loads(bad)
    del bad_dict["price"]
    bronze = _bronze(spark, [good, json.dumps(bad_dict)])

    conformed = conform_trades(parse_bronze(bronze)).collect()
    ids = {r["trade_id"] for r in conformed}
    assert ids == {1}


def test_unparseable_json_is_routed_to_quarantine_not_silver(spark) -> None:  # type: ignore[no-untyped-def]
    bronze = _bronze(spark, [_trade_json(trade_id=1), "{not valid json"])

    parsed = parse_bronze(bronze)
    conformed = conform_trades(parsed).collect()
    quarantined = to_quarantine(parsed).collect()

    assert {r["trade_id"] for r in conformed} == {1}
    assert len(quarantined) == 1
    assert quarantined[0]["reason"] == "parse_or_contract_failure"
    assert quarantined[0]["quarantined_at"] is not None


def test_out_of_order_events_keep_their_own_event_times(spark) -> None:  # type: ignore[no-untyped-def]
    # Late / out-of-order arrival: a trade that happened earlier arrives second.
    early = _trade_json(trade_id=1, time="2026-07-04T23:00:00.000000Z")
    late = _trade_json(trade_id=2, time="2026-07-04T22:59:00.000000Z")
    bronze = _bronze(spark, [early, late])

    rows = conform_trades(parse_bronze(bronze)).collect()
    conformed = {r["trade_id"]: r["event_time"] for r in rows}
    # Each row keeps its true event time regardless of arrival order — the basis
    # for correct windowing + watermarks downstream.
    assert conformed[1] > conformed[2]
