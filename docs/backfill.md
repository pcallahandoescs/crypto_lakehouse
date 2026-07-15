# Backfill & reprocessing (Day 17)

Batch backfills recompute **silver** and **gold** for a chosen **event-time**
date range. They rely on two foundations built earlier in the project:

1. **Immutable bronze** — the append-only log of every raw message (Day 10).
2. **Idempotent MERGE writes** — upsert on grain keys, safe to re-run (Day 16).

## Why bronze, not Coinbase?

Coinbase WebSocket is **live-only** — you cannot ask it to replay last Tuesday.
Kafka *could* be replayed (retained log), but in this architecture **bronze is
the durable replay source**: once a message is landed, reprocessing never needs
the live feed again.

That is the concrete version of the **Kappa** idea: treat the log as truth and
**reprocess it** when logic changes, data was missed, or you need to rebuild a
date slice. Lambda keeps a separate speed layer for freshness; backfills target
the **batch** path (silver + gold), which is the correctness layer.

```
Coinbase (live, no replay)
    -> Kafka (retained log, replay possible)
        -> bronze Delta (immutable, our long-term replay log)
            -> silver MERGE  <- backfill reads from here
                -> gold MERGE  <- backfill recomputes candles
```

## The job

[`backfill.py`](../spark_jobs/backfill.py) runs:

| Step | Input | Output | Write mode |
|---|---|---|---|
| Silver | bronze (filtered by trade `time`) | silver | MERGE on `(product_id, trade_id)` |
| Gold | silver (filtered by `event_time`) | gold OHLC | MERGE on `(product_id, interval_start)` |

**Date bounds:** `[--start, --end)` in UTC — start **inclusive**, end **exclusive**.
Example: `--start 2026-07-01 --end 2026-07-02` covers all of July 1 UTC.

Invalid rows during silver backfill append to the **quarantine** table (same as
the streaming path).

Gold reads silver with a **one-interval pad** on each side so edge candles
(e.g. the first minute of the range) include trades from the prior minute.

## Run it

Pick a range that overlaps your data:

```bash
# See what event-time span bronze covers
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" backfill.py --show-range
```

On Docker Desktop (~2 GB per container), run **silver and gold as two commands**
(same pattern as `prove_idempotency.py` — avoids exit 137):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    backfill.py --start 2026-07-05 --end 2026-07-06 --skip-gold

docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    backfill.py --start 2026-07-05 --end 2026-07-06 --skip-silver
```

Single command (works when Docker has enough RAM):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    backfill.py --start 2026-07-05 --end 2026-07-06
```

Partial runs:

```bash
# Re-conform bronze -> silver only (e.g. after a silver transform fix)
backfill.py --start 2026-07-01 --end 2026-07-02 --skip-gold

# Re-aggregate candles from existing silver only
backfill.py --start 2026-07-01 --end 2026-07-02 --skip-silver
```

Verify totals and grain:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py
```

Re-run the **same** backfill command — row counts for the full tables should
not double (MERGE upsert). See [`idempotency.md`](idempotency.md).

## What backfill does *not* touch

| Layer | Why |
|---|---|
| **Kafka** | Source log; bronze already captured the history |
| **Bronze** | Immutable — backfill reads, never rewrites |
| **Speed / realtime gold** | Separate Kafka-direct path; approximate by design |
| **Streaming checkpoints** | Batch backfill bypasses streaming entirely |

## When you'd run this in production

- **Logic fix** — bug in `conform_trades` or `to_gold`; backfill affected dates.
- **Missed window** — batch job failed overnight; reprocess that date range.
- **New downstream grain** — add a 5-minute gold table; backfill history from silver.
- **Audit / recovery** — rebuild analytical tables from bronze after an incident.

Day 19 wires this into an Airflow **backfill DAG** with parameterized dates.
