"""Tests for the Trade data contract (consumer/schema.py).

These lock the ingestion-boundary invariants: a well-formed Coinbase trade
parses with exact types, and every violation we care about is rejected.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from consumer.schema import Trade

# A real message shape, straight from the Coinbase feed study.
VALID_RAW = (
    b'{"type":"match","trade_id":1050206160,'
    b'"maker_order_id":"52fc8a90-23bc-4ca1-a593-4e7f250fa2e0",'
    b'"taker_order_id":"63ad3cd1-637f-4601-a7d4-30b600e51263",'
    b'"side":"buy","size":"0.00634069","price":"63109.18",'
    b'"product_id":"BTC-USD","sequence":132031021885,'
    b'"time":"2026-07-04T23:34:58.071539Z"}'
)


def test_valid_trade_parses_with_exact_types() -> None:
    trade = Trade.model_validate_json(VALID_RAW)
    assert trade.product_id == "BTC-USD"
    assert trade.side == "buy"
    # Money must be exact Decimal, never a lossy float.
    assert trade.price == Decimal("63109.18")
    assert isinstance(trade.price, Decimal)
    assert trade.dedup_key == ("BTC-USD", 1050206160)


def _base_trade() -> dict[str, object]:
    return {
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


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("price", "0"),  # price must be > 0
        ("price", "-5"),
        ("size", "0"),  # size must be > 0
        ("side", "long"),  # not a valid side
        ("type", "heartbeat"),  # not a trade type
        ("trade_id", 0),  # must be > 0
        ("product_id", ""),  # must be non-empty
    ],
)
def test_invalid_values_are_rejected(field: str, bad_value: object) -> None:
    data = _base_trade()
    data[field] = bad_value
    with pytest.raises(ValidationError):
        Trade.model_validate(data)


def test_missing_field_is_rejected() -> None:
    data = _base_trade()
    del data["price"]
    with pytest.raises(ValidationError):
        Trade.model_validate(data)


def test_unexpected_field_is_rejected() -> None:
    # extra="forbid": upstream schema drift must surface loudly, not silently.
    data = _base_trade()
    data["new_exchange_field"] = "surprise"
    with pytest.raises(ValidationError):
        Trade.model_validate(data)


# --- schema drift: enforcement vs. evolution -------------------------------
#
# The contract at the ingestion boundary is deliberately *strict* (extra="forbid",
# required fields, typed values): upstream drift must fail fast and loud here, at
# the edge, rather than corrupt bronze/silver silently. Additive *evolution* is a
# separate, controlled decision made downstream at the Delta layer (mergeSchema),
# not something the boundary contract should wave through. These tests pin that
# policy so a future loosening of the contract is a conscious change, not an
# accident.


def test_drift_new_upstream_field_fails_fast_at_the_boundary() -> None:
    # Simulate Coinbase adding a field to the matches payload mid-stream.
    drifted = _base_trade()
    drifted["taker_fee_rate"] = "0.0005"
    with pytest.raises(ValidationError) as excinfo:
        Trade.model_validate(drifted)
    assert "taker_fee_rate" in str(excinfo.value)


def test_drift_renamed_field_is_rejected_as_missing() -> None:
    # A rename looks like: the old required field vanishes + a new one appears.
    drifted = _base_trade()
    drifted["ticker"] = drifted.pop("product_id")
    with pytest.raises(ValidationError):
        Trade.model_validate(drifted)


def test_drift_retyped_field_is_rejected() -> None:
    # Upstream starts sending price as a JSON number instead of a string, or a
    # non-numeric string — either way the exact-Decimal contract must reject it.
    for bad_price in ("not-a-number", "12,345.6"):
        drifted = _base_trade()
        drifted["price"] = bad_price
        with pytest.raises(ValidationError):
            Trade.model_validate(drifted)


def test_non_drift_valid_payload_still_parses() -> None:
    # Guardrail: the strict contract must not reject a well-formed trade.
    assert Trade.model_validate(_base_trade()).product_id == "BTC-USD"
