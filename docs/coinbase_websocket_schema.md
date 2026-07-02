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

## Order book 101 (background for the gotchas)

An exchange matches two order types:

- **Limit order** — executes only at a set price or better; if it can't fill
  now, it *rests on the book* waiting.
- **Market order** — executes immediately against orders already on the book.

The book is the resting limit orders, best prices in the middle:

```
        SELLERS resting (asks)
   1.0 BTC @ $60,002
   0.3 BTC @ $60,001
   0.5 BTC @ $60,000   <- best ask
   ------------------------- spread
   0.4 BTC @ $59,999   <- best bid
   0.8 BTC @ $59,998
        BUYERS resting (bids)
```

Every trade has two parties:

- **Maker** — the order that was *already resting* (provides liquidity).
- **Taker** — the *incoming* order that crosses the spread and executes against
  a maker (removes liquidity; the aggressor).

**A trade = one taker hitting one maker.** That's why each message carries both
`maker_order_id` and `taker_order_id`. Crucially, **either side can be the
maker** — a buyer can rest a bid and later a seller hits it, or vice versa. "Who
was resting" is a separate question from "who was buying."

## Gotchas (design-relevant)

### 1. `price` and `size` are decimals encoded as strings
JSON has no decimal type — only `number`, which is a binary float where
`0.1 + 0.2 == 0.30000000000000004`. Over millions of trades those errors
accumulate and can even make the batch and speed layers disagree. Coinbase sends
`"60383.65"` as a **string** to preserve exact digits. Parse to an exact type —
Python `Decimal` or Spark `DecimalType(precision, scale)` — **never `float`/
`double`** (Day 11).

### 2. `side` is the MAKER's side, not the aggressor's
`side` reports the side of the *resting* (maker) order, so the **aggressor
(taker) is always the opposite**:

- `"side": "buy"`  -> a resting **buy** was hit -> the taker was a **seller**
  (seller-initiated, typically a down-tick).
- `"side": "sell"` -> a resting **sell** was taken -> the taker was a **buyer**
  (buyer-initiated, up-tick).

**Why it matters:** order-flow / "buy vs sell pressure" metrics (order-flow
imbalance, taker-buy ratio, signed VWAP) depend on the *aggressor*. Naively
doing `if side == "buy": buy_pressure += size` labels every trade **backwards** —
a silent correctness bug (the dashboard shows bullish when it's bearish). When we
compute these (Days 12-13), derive an explicit `aggressor_side = opposite(side)`
column so nobody downstream misreads it.

### 3. No ingestion timestamp in the payload
`time` is the exchange **event time** (when the trade happened), *not* when we
received/stored it (**processing time**). They differ due to network lag,
reconnects, and replays. Event time drives correctness (windowing + watermarks,
Day 13 — a late trade must still land in the correct candle); we stamp our own
**ingestion time** at bronze (Day 10) to measure latency and lag. Conflating the
two is a classic streaming bug.

### 4. One taker order can produce many `match` messages ("sweeping")
A single large taker sweeps *multiple* resting makers. Example: a market
**BUY 1.5 BTC** against the asks above fills 0.5 @ 60000, 0.3 @ 60001, 0.7 @
60002 — **three trades from one order**, same millisecond, each with a distinct
`trade_id`/`maker_order_id` but the **same `taker_order_id`**. It is *not*
multiple buyers — it's one buyer hitting several sellers.

We saw this live in ETH — five trades, same ms, same price, consecutive ids:

```
22:56:25.653  ETH-USD  ^ buy  price=1619.67  size=0.06174099  id=823270348
22:56:25.653  ETH-USD  ^ buy  price=1619.67  size=0.06174099  id=823270349
22:56:25.653  ETH-USD  ^ buy  price=1619.67  size=0.06174099  id=823270350
22:56:25.653  ETH-USD  ^ buy  price=1619.67  size=0.06174099  id=823270351
22:56:25.653  ETH-USD  ^ buy  price=1619.67  size=0.00212637  id=823270352
```

These are **legitimately distinct trades** — do not collapse them. They also
drive the burst peaks in the velocity numbers below.

### 5. Dedup key: `(product_id, trade_id)`
`trade_id` is unique *per product* (BTC and ETH have independent id sequences),
so the product must be part of the key. We dedupe because the pipeline targets
**exactly-once**: on a producer reconnect or Kafka replay the same trade can
arrive twice. Dedupe on the **id**, never on `(price, time)` — that would wrongly
merge distinct burst trades (gotcha #4). Bonus: `sequence` is monotonic per
product, so gaps reveal dropped messages (a DQ check for Week 3).

## Volume & velocity (observed)

Measured on 2026-07-01 (BTC-USD + ETH-USD, `matches` channel only), using the
rolling-window stats in `explore_feed.py`. Representative run: 645 trades / 53s.

| Metric | Observation |
|---|---|
| Average rate | ~**12 msg/s** (645 / 53s); a shorter earlier sample was ~9/s |
| Recent-interval rate | up to ~**28 msg/s** over a 5s window |
| **Peak (rolling 1s)** | **~59 msg/s** — roughly **5× the average** |
| Product mix | **BTC-USD leads but the ratio varies** (~2:1 this run — 427 vs 218; an earlier short sample was almost all BTC) |
| Rough daily volume | ~12 × 86,400 ≈ **~1.05M trades/day** |
| Rough daily raw size | ~300 B/msg → **~300 MB/day** uncompressed |

### Implications for downstream design

- **Size for the peak, not the average.** Average is ~12/s but bursts hit
  ~59/s (a ~5× burst factor). A pipeline tuned only for the average would choke
  on a sweep. Both numbers matter; the peak drives Kafka/Spark sizing.
- **Throughput is still modest even at peak** — ~60 msg/s is trivial for a
  single Kafka broker. So partitioning here is about *ordering + parallelism*,
  not raw volume.
- **Keying by `product_id`** preserves per-market trade ordering, but BTC is
  usually the busiest partition — a real (if moderate, and time-varying)
  **data-skew** example to call out (Day 3/4 partition-count decision, Day 14
  layout discussion).
- **Volume (~1M trades/day)** is small enough that the local MinIO lakehouse is
  comfortable even over weeks of data.
