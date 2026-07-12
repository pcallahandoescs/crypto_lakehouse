# Gold aggregation (Day 12): OHLC candles + VWAP + volume

Third pipeline stage and the **analytical product** — the table consumers (a
dashboard, an analyst, a model) actually query.
[`spark_jobs/gold_aggregate.py`](../spark_jobs/gold_aggregate.py) reads silver and
rolls individual trades up into **candles** at `s3a://gold/ohlc`.

## Grain

The single most important property of a serving table: **one row per
`(product_id, interval_start)`**. The interval is a **tumbling window**
(non-overlapping, fixed width — 1 minute by default). Every trade falls into
exactly one candle. State the grain clearly and everything else (joins,
aggregations, dashboard queries) follows.

## The metrics

For each product+interval:

| Column | Meaning | How |
|---|---|---|
| `open` | first trade price in the interval | `min_by(price, (event_time, sequence))` |
| `high` | max price | `max(price)` |
| `low` | min price | `min(price)` |
| `close` | last trade price in the interval | `max_by(price, (event_time, sequence))` |
| `volume` | total base-asset traded | `sum(size)` (exact decimal) |
| `vwap` | volume-weighted avg price | `sum(price*size) / sum(size)` |
| `trade_count` | trades in the interval | `count(*)` |

### Why `min_by`/`max_by` for open/close

`open` and `close` are **order-dependent** — they're the *first* and *last* trade
by time, not the smallest/largest price. A plain `first()`/`last()` in a grouped
aggregation has **no guaranteed order** (rows arrive in whatever order the shuffle
produced). `min_by(price, struct(event_time, sequence))` deterministically returns
the price at the earliest `(event_time, sequence)` — using `sequence` as the
tie-breaker when two trades share a timestamp (which happens — see the Day-2 feed
study). This is the correct, deterministic way to get open/close.

### Why VWAP is a double when everything else is decimal

Money stays **exact decimal** through OHLC and volume. VWAP is the exception, on
purpose:

- Multiplying two `DECIMAL(38,18)` values (`price * size`) blows the 38-digit
  precision budget — Spark would silently reduce scale (or null on overflow).
- VWAP is a *derived analytical ratio* (a weighted mean), where sub-cent floating
  error is irrelevant.

So `price*size` and the `sum(size)` used for the ratio are cast to `double`; the
reported `volume` and OHLC prices remain exact decimals. This is a deliberate,
documented trade-off — exactness where it's money, pragmatism where it's a mean.

## Batch + idempotent-by-overwrite

This is a **batch** job (not streaming): read all of silver, recompute every
candle, `overwrite` gold. Overwrite makes it **idempotent** — running it twice
yields identical output, no duplicates. That's fine while the dataset is small;
recomputing all history every run is wasteful at scale, so **Day 16** upgrades
this to an incremental **`MERGE`/upsert** keyed on the grain.

## Partitioning (Day 14)

Gold is **partitioned by `date`** (`to_date(interval_start)`). Typical queries
filter a calendar day, so Delta can **prune** whole directories. We deliberately
do **not** partition by minute (`interval_start`) — that would create one tiny
file per candle. `product_id` is handled by **Z-ordering** instead (see
[`data_layout.md`](./data_layout.md)).

## Run it

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g gold_aggregate.py
```

It prints the candle count and the 10 most recent candles. Verify with
[`counts.py`](../spark_jobs/counts.py):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py
```

Expect **`gold candles == gold distinct grain`** (one row per product+interval —
proof the grain holds). Browsable in the MinIO console (http://localhost:9001) →
`gold/ohlc`.

For 5-minute candles, set the interval (writes the same table — change the path if
you want both side by side):

```bash
docker compose run --rm -e GOLD_INTERVAL="5 minutes" spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g gold_aggregate.py
```
