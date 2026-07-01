# Data Source: Coinbase Exchange WebSocket (`matches` channel)

Documentation of the source feed, captured on **2026-07-01** by
[`producer/explore_feed.py`](../producer/explore_feed.py). This grounds the formal
schema / data contract (Day 6) and downstream design in the *real* feed, not
assumptions.

## Connection

| Property | Value |
|---|---|
| Endpoint | `wss://ws-feed.exchange.coinbase.com` |
| Auth | **None** — public, read-only market data |
| Channel | `matches` (every individual trade) |
| Products observed | `BTC-USD`, `ETH-USD` |

**Subscribe handshake** (client → server):

```json
{ "type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD"], "channels": ["matches"] }
```

The server replies once with a `subscriptions` confirmation, then emits one
`last_match` per product (the most recent trade), then `match` messages in real
time.

## Message schema (`match` / `last_match`)

Example (verbatim from the live feed):

```json
{
  "type": "last_match",
  "trade_id": 1048487461,
  "maker_order_id": "5be21e86-6e63-486f-8113-07f5155f305c",
  "taker_order_id": "d552cc91-eb56-4514-880d-0cbcd0f9d0f0",
  "side": "buy",
  "size": "0.00000001",
  "price": "60383.65",
  "product_id": "BTC-USD",
  "sequence": 131842478451,
  "time": "2026-07-01T22:56:22.239078Z"
}
```

| Field | JSON type | Semantic type | Meaning / notes |
|---|---|---|---|
| `type` | string | enum | `match` (live) or `last_match` (initial snapshot per product). Treat identically for data; useful to distinguish the first message. |
| `trade_id` | number | int64 | Unique trade id **per product** (not globally). Part of our dedup key. |
| `maker_order_id` | string | UUID | Resting (maker) order. |
| `taker_order_id` | string | UUID | Incoming (taker) order that crossed the book. |
| `side` | string | enum | **Side of the MAKER order** — see gotcha below. Values: `buy` / `sell`. |
| `size` | string | decimal | Base-asset quantity traded (e.g. BTC amount). **Decimal-as-string.** |
| `price` | string | decimal | Trade price in quote currency (USD). **Decimal-as-string.** |
| `product_id` | string | enum | Market, e.g. `BTC-USD`. Our natural partition/key column. |
| `sequence` | number | int64 | Per-product monotonically increasing sequence. Ordering + gap detection. |
| `time` | string | timestamp | Exchange **event time**, ISO-8601 UTC, microsecond precision, `Z` suffix. This is our event-time for windowing/watermarks. |

## Gotchas (design-relevant)

1. **`price` and `size` are decimals encoded as strings.** Cast to `DecimalType`
   (or Python `Decimal`) in silver — **never `float`** (money precision).
2. **`side` is the MAKER's side, not the aggressor's.** Per Coinbase, a `buy`
   side means the maker was a buy order that got hit (a down-tick / seller-
   initiated trade); `sell` means an up-tick / buyer-initiated trade. So for
   buy/sell *pressure* metrics, the taker/aggressor side is the **opposite** of
   this field. Document the convention explicitly wherever we compute it.
3. **No ingestion timestamp in the payload.** `time` is exchange event-time
   only. We add our own ingestion timestamp at the bronze layer (Day 10) — the
   two enable measuring end-to-end latency and late-data handling.
4. **One taker order can produce many `match` messages.** A large order sweeping
   the book emits a burst of trades at the same/adjacent prices and the same
   millisecond (seen clearly in ETH bursts). Expect duplicate-looking rows that
   are legitimately distinct trades (distinct `trade_id`/`sequence`).
5. **Dedup key:** `(product_id, trade_id)` — unique per product. `sequence` also
   works and additionally reveals dropped messages (gaps).

## Volume & velocity (observed)

| Metric | Observation |
|---|---|
| Average rate | ~**9 msg/s** across both products |
| Range | ~5.9 – 11.8 msg/s (**bursty**) |
| Product mix | **BTC-USD dominates**; ETH-USD is sparse |
| Rough daily volume | ~9 × 86,400 ≈ **~780k trades/day** |
| Rough daily raw size | ~300 B/msg → **~230 MB/day** uncompressed |

### Implications for downstream design

- **Throughput is modest** — a single Kafka broker/partition handles this
  trivially. Partitioning is about *ordering + parallelism*, not raw volume.
- **Keying by `product_id`** preserves per-market trade ordering, but because
  BTC dominates, that partition is **hot** — a real-world **data-skew** example
  to call out (Day 3/4 partition-count decision, Day 14 layout discussion).
- **Bursts** mean the pipeline must tolerate short spikes (~2× average); size
  Spark trigger intervals / Kafka consumer settings accordingly.
- **Volume is small enough** that the local MinIO lakehouse is comfortable even
  over weeks of data.
