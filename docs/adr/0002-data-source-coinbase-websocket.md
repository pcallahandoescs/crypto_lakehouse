# 0002. Data source: Coinbase WebSocket

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

The project needs a **real, live, high-velocity** data source that is legal to
use and free of licensing friction. Equity market data is encumbered by exchange
licensing and redistribution rules — a poor fit for a public portfolio repo.
Crypto market data is publicly streamable without those constraints, and the
domain (real-time crypto/finance) is directly relevant to the target employers.

## Decision

We will ingest live trades from the **Coinbase Exchange WebSocket** `matches`
channel for a small set of products (BTC-USD, ETH-USD), documented in
[`docs/coinbase_websocket_schema.md`](../coinbase_websocket_schema.md).

## Consequences

- Free, real, continuous data with genuine market-data quirks to handle
  (decimals-as-strings, maker-side semantics, bursts, event-vs-processing time)
  — which makes the downstream engineering *authentic*, not toy.
- No historical replay from the source itself (WebSocket is live-only); this is
  precisely why Kafka retention + immutable bronze provide our replay story.
- Dependence on a third-party feed's uptime/format; mitigated by the producer's
  reconnect logic and the Day 6 data contract catching drift.

## Alternatives considered

- **Binance WebSocket** — comparable; Coinbase chosen for familiarity and clean
  schema. Easy to add later as a second source.
- **A simulated/synthetic feed** — fully controllable but defeats the purpose:
  no real-world messiness, weaker portfolio signal. Rejected.
- **Paid equity data** — licensing/redistribution problems for a public repo.
  Rejected.
