# Silver transform (Day 11): clean, type & conform

Second pipeline stage. [`spark_jobs/silver_transform.py`](../spark_jobs/silver_transform.py)
reads the **bronze** table (raw JSON strings) and produces **silver** at
`s3a://silver/trades`: every trade parsed into typed columns, malformed rows
dropped, and duplicates removed on the natural key. Silver is the first table
other jobs and people are meant to actually query.

## What "conformed" means

Bronze faithfully preserves whatever the source sent — including its quirks
(prices as strings, a JSON blob, junk messages). Silver imposes **one canonical
shape and set of semantics** so nothing downstream has to re-handle source
messiness:

| Concern | Bronze | Silver |
|---|---|---|
| Structure | one `value` JSON string | typed columns |
| Money (`price`/`size`) | string inside JSON | `DECIMAL(38,18)` — exact, never float |
| Time | string inside JSON | real `TIMESTAMP` (`event_time`) |
| Validity | everything, incl. junk | only rows that satisfy the contract |
| Uniqueness | may contain duplicates | one row per `(product_id, trade_id)` |

The Spark `TRADE_SCHEMA` is the Spark-side mirror of the Pydantic `Trade`
contract in [`consumer/schema.py`](../consumer/schema.py) — same fields, same
"decimal for money" rule. Two encodings of the same contract, at two boundaries.

## Parse & drop-malformed

`from_json(value, TRADE_SCHEMA)` casts the JSON string into a struct. Anything it
can't parse becomes **null**:

- `"hello kafka"` → not valid JSON → the whole struct is null.
- `{"test":"trade"}` → valid JSON but none of the real fields present → struct
  with all-null fields.

We then keep only rows where the identity + money + time fields are non-null
(`trade_id`, `product_id`, `price`, `size`, `time`). That single filter rejects
both junk messages we planted on Day 3, which is the "drop malformed rows" step.
(A stricter design quarantines rejects into a side table — implemented on
**Day 15**; see [`data_quality.md`](./data_quality.md).)

## Deduplication + watermark (the idempotency seed)

The natural key is `(product_id, trade_id)` — same key means the same trade.
De-duping a **stream** naively would force Spark to remember *every key forever*
(unbounded state). A **watermark** fixes that:

```python
conformed.withWatermark("event_time", "1 hour") \
         .dropDuplicatesWithinWatermark(["product_id", "trade_id"])
```

- The **watermark** is a moving "we no longer expect data older than this"
  threshold: `max(event_time seen) − 1 hour`. It tells Spark how long to hold
  dedup state; keys for trades older than the watermark are **evicted**, so state
  stays bounded.
- `dropDuplicatesWithinWatermark` (Spark 3.5+) drops repeats of a key seen within
  that window, **without** needing `event_time` in the key itself (the older
  `dropDuplicates` required exact event-time match too, which is fragile).

This is the project's first taste of **idempotency**: re-delivered or replayed
trades collapse to one row. Day 16 generalizes it to batch `MERGE`/upsert.

The 1-hour watermark is a deliberate trade-off: it tolerates duplicates arriving
up to an hour apart, at the cost of an hour of retained state. A duplicate that
somehow arrived >1 hour after the original would slip through — acceptable here,
and tunable.

## Schema enforcement vs. evolution (the policy)

The silver write **does not** set `mergeSchema`, so Delta **enforces** the
schema: a write whose columns don't match silver's schema **fails loudly**
instead of silently corrupting the table. That's the default and the safe choice
for a trusted layer.

The opposite mode, **schema evolution**, is opt-in via `.option("mergeSchema",
"true")` — it *allows* an additive change (e.g. a new nullable column) to extend
the table schema on write. Policy for this project:

- **Enforce by default** everywhere. Drift should break the build, not the data.
- **Evolve deliberately** only for known, additive, backward-compatible changes,
  as an explicit code change — never as an accident.

We prove the evolution path with a deliberate schema-drift test on **Day 21**.

## Run it

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g silver_transform.py
```

The `--driver-memory 2g` matters: the dedup is a **stateful shuffle**, which is
far heavier than bronze's stateless append (see the memory note below). Early
`[progress]` lines chew through the bronze backlog in `maxFilesPerTrigger` chunks;
later ones tail new bronze commits. Ctrl+C to stop. Because silver has its own
checkpoint (`s3a://silver/_checkpoints/trades`), re-running resumes rather than
reprocessing.

### Memory: why silver is heavier than bronze

Silver's dedup forces a shuffle, and Spark's default `spark.sql.shuffle.partitions
= 200` is a *cluster* default — for a **stateful** streaming query that means 200
state-store instances in a single laptop JVM, which OOM-kills a 1 GB driver.
Mitigations, all applied:

- `spark.sql.shuffle.partitions = 8` in [`common.py`](../spark_jobs/common.py)
  (right-sized for single-node; inherited by every job).
- `maxFilesPerTrigger` on the bronze read, so the backlog is chunked, not one
  giant batch.
- `--driver-memory 2g` at submit time for headroom.

Note: Structured Streaming **locks the shuffle-partition count into the checkpoint
at query start**, so changing it requires starting from a fresh checkpoint.

## Verify

Use [`counts.py`](../spark_jobs/counts.py) — it's the source of truth (the
`[progress]` lines only sample every 10s and miss back-to-back catch-up batches):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py
```

Expect **`silver rows == silver distinct keys`** (dedup is clean) and silver ==
bronze minus any junk/duplicates. Also browsable in the MinIO console
(http://localhost:9001) → `silver/trades` (Parquet + `_delta_log/`).
