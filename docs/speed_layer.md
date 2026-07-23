# Speed layer: real-time windowed metrics

The streaming half of the **Lambda architecture**.
[`spark_jobs/speed_metrics.py`](../spark_jobs/speed_metrics.py) reads Kafka
directly and computes rolling metrics live, writing to `s3a://gold/realtime_metrics`.
It's the low-latency complement to the batch medallion path.

## Speed vs. batch — why have both

| | Speed layer | Batch layer (bronze→silver→gold) |
|---|---|---|
| Source | Kafka (direct) | Kafka → bronze → silver → gold |
| Latency | seconds | minutes+ |
| Correctness | approximate / provisional | exact, fully reprocessable |
| Reads | `startingOffsets=latest` (now) | full history |
| Optimized for | freshness | correctness |

This is the Lambda bet: a fast, approximate view **and** a slow, exact view of the
same data. A dashboard shows the speed layer for "right now"; analysis and
backfills trust the batch gold tables. The cost is maintaining two code paths —
which is exactly the trade-off documented in
[ADR 0006](adr/0006-lambda-architecture.md) (and the note on when you'd collapse
to Kappa).

The speed layer reads Kafka **directly** (not from bronze) on purpose: it's an
independent path that doesn't wait on the batch layer, so a stall in bronze/silver
never delays real-time metrics.

## Metrics & the sliding window

Per product, over a **sliding** 1-minute window recomputed every 15 seconds:

- `trade_count`, `volume` (exact decimal)
- `vwap` = `sum(price*size)/sum(size)` (double — same decimal-overflow rationale
  as gold)
- `price_volatility` = `stddev(price)` in the window (null for a single-trade
  window, which is correct)

**Sliding vs. tumbling:** gold used *tumbling* windows (adjacent, non-overlapping
— one candle per minute). The speed layer uses a *sliding* window (`window("1
minute", "15 seconds")`): a 1-minute window that advances every 15s, so windows
**overlap** and each trade lands in up to 4 of them. That gives a smooth "rolling
last-60-seconds" metric that updates 4× a minute — the right shape for a live
gauge, versus discrete candles.

## Watermarks & late data

A streaming aggregation over event time must decide: how long do we wait for
late-arriving trades before finalizing a window? That's the **watermark**:

```python
trades.withWatermark("event_time", "1 minute")
```

- The watermark tracks `max(event_time) − 1 minute`. A trade whose `event_time` is
  older than the current watermark is **too late** and dropped from its window.
- It also bounds **state**: once the watermark passes a window's end, that window
  is finalized and its state evicted — so memory stays bounded even though the
  stream is infinite.

**Output mode = append** (with the watermark): a window row is written **once it's
final** — i.e. after the watermark passes its end and no more data can change it.
This is the durable, exactly-once-friendly choice for a Delta sink. The cost:
a window appears ~1 watermark after it closes. A lower-latency design would use
`foreachBatch` + Delta `MERGE` to upsert *in-progress* windows (update semantics),
which overlaps the batch idempotency work.

## Run it

The speed layer needs **live** trades (it starts at `latest`), so start the
producer first:

```bash
docker compose start producer

docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g speed_metrics.py
```

Let it run a few minutes (windows only finalize after the watermark, so give it
2–3 min before expecting rows). `[progress]` lines show input trades per batch.
Ctrl+C to stop. Verify:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py   # gold_realtime rows > 0
```

Browsable in the MinIO console (http://localhost:9001) → `gold/realtime_metrics`.
Its own checkpoint (`s3a://gold/_checkpoints/realtime_metrics`) makes it resumable.
