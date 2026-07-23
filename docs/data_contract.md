# Data Contract — the ingestion boundary

A **data contract** is an explicit, enforced agreement about the shape and
meaning of data crossing a boundary. Ours sits where trades enter the system
from the Coinbase `matches` feed. It is defined once, in code, as a Pydantic
model: [`consumer/schema.py`](../consumer/schema.py).

## Why have one at all?

Kafka itself is schema-agnostic — a topic is just bytes. That's a feature
(flexible, fast) and a risk (nothing stops a malformed or drifted message from
flowing downstream and quietly corrupting silver/gold aggregates days later).
The contract closes that gap:

- **Fail fast, fail cheap.** Reject bad data at the door, not deep in a Spark
  job or, worse, in a chart a stakeholder is looking at.
- **Executable documentation.** The model can't go stale — it's the same thing
  we validate against.
- **A shared definition.** Producer, consumer, and (later) the bronze/silver
  parsing all point at one authoritative shape.

## The contract (fields, types, invariants)

| Field | Type | Invariant / notes |
|---|---|---|
| `type` | `"match" \| "last_match"` | only trade events; anything else is rejected |
| `trade_id` | `int` | `> 0`; half of the dedup key |
| `maker_order_id` | `str` | resting order that was hit |
| `taker_order_id` | `str` | incoming order that crossed the book |
| `side` | `"buy" \| "sell"` | **maker's** side (the classic gotcha) |
| `size` | `Decimal` | `> 0`; parsed from a decimal *string* (never float) |
| `price` | `Decimal` | `> 0`; exact precision for money |
| `product_id` | `str` | non-empty; other half of the dedup key |
| `sequence` | `int` | `> 0`; per-product ordering counter |
| `time` | `datetime` | event time (exchange-side) |

Two deliberate choices worth calling out:

- **`Decimal`, not `float`.** Prices/sizes arrive as strings like `"63109.18"`.
  Parsing to `Decimal` preserves exact value; `float` would introduce binary
  rounding error — unacceptable for money. (See the feed study in
  [`coinbase_websocket_schema.md`](./coinbase_websocket_schema.md).)
- **`extra="forbid"`.** An unexpected new field is a *violation*, not a shrug.
  This converts silent upstream schema drift into a loud, catchable signal.
  Tradeoff: it's strict — if Coinbase adds a field we must consciously update
  the contract. That's the point (see compatibility below).

The natural **dedup key** is `(product_id, trade_id)`: same key ⇒ same trade.
This is what the verification consumer counts duplicates on, and what silver
will deduplicate on in the silver layer.

## How we verify it

[`consumer/verify.py`](../consumer/verify.py) reads the whole topic and
validates every message against the contract, reporting counts, per-product
totals, contract violations (with samples), and duplicates. It reads from the
beginning every run and never commits offsets, so verification is deterministic
and side-effect-free. Contract invariants are also unit-tested in
[`tests/test_schema.py`](../tests/test_schema.py).

```bash
# with the stack running and the producer streaming:
uv run python -m consumer.verify   # Ctrl+C to print the final report
```

## Scaling this up: schema registry

Our Pydantic contract is a lightweight, single-repo solution — perfect here, but
in a larger org producers and consumers are different teams/services and can't
share a Python class. The industry answer is a **schema registry** (e.g.
Confluent Schema Registry) with a binary format like **Avro** or **Protobuf**:

- The schema is registered centrally and each message carries a small **schema
  ID**; consumers fetch the matching schema to deserialize. Payloads are compact
  (no repeated field names) and strongly typed.
- The registry enforces **compatibility rules** on new schema versions:
  - **BACKWARD** — new consumers can read old data (e.g. adding an optional
    field). Most common default.
  - **FORWARD** — old consumers can read new data.
  - **FULL** — both.
  This is the disciplined, org-scale version of what our `extra="forbid"` does
  by hand: it makes schema *evolution* safe and intentional instead of a
  surprise. (We revisit enforcement-vs-evolution for the Delta tables on
  Days 11 and 21.)

For this project the Pydantic contract is the right call — we control both ends,
and it gives us validation, typing, and docs in one file with zero extra
infrastructure. The registry is the natural next step and is captured here as a
reasoned "when you'd reach for it" decision.
