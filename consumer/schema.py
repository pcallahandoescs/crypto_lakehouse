"""The Trade data contract — the formal schema at the ingestion boundary.

This Pydantic model is the single, authoritative definition of what a valid
trade *is* as it enters our system from the Coinbase `matches` feed. It encodes
the things a plain JSON blob can't: types, nullability, and invariants
(price > 0, side is exactly buy/sell, timestamps are real datetimes, prices are
exact Decimals rather than lossy floats).

Why a contract here?
- **Fail at the boundary, not deep in the pipeline.** Catching a malformed or
  drifted message the moment it arrives is far cheaper than discovering corrupt
  gold-layer aggregates days later.
- **Executable documentation.** The model *is* the schema doc — it can't go
  stale, because the code validates against it.

Note on strictness: `extra="forbid"` means an unexpected new field is treated as
a contract violation. That's deliberate — it turns silent upstream schema drift
into a loud, catchable signal (see the schema-registry note in the docs for how
this generalizes to Avro/Protobuf compatibility rules at scale).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Trade(BaseModel):
    """A single executed trade from the Coinbase `matches` channel.

    Field semantics (see docs/coinbase_websocket_schema.md for the full study):
      - ``side`` is the *maker's* side, not the taker's — the classic gotcha.
      - ``price`` / ``size`` arrive as decimal strings; we parse to ``Decimal``
        to preserve exact precision (never float for money).
      - ``(product_id, trade_id)`` is the natural dedup key.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["match", "last_match"]
    trade_id: int = Field(gt=0)
    maker_order_id: str
    taker_order_id: str
    side: Literal["buy", "sell"]
    size: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    product_id: str = Field(min_length=1)
    sequence: int = Field(gt=0)
    time: datetime

    @property
    def dedup_key(self) -> tuple[str, int]:
        """Stable identity of a trade: same key => same trade (a duplicate)."""
        return (self.product_id, self.trade_id)
