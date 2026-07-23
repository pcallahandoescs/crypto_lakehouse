# Bronze ingestion: Kafka → Delta, exactly-once

The first pipeline stage. A Spark Structured Streaming job
([`spark_jobs/bronze_ingest.py`](../spark_jobs/bronze_ingest.py)) reads
`crypto.trades.raw` and lands every message into the **bronze** Delta table at
`s3a://bronze/trades`, append-only.

## What bronze stores (and what it deliberately doesn't)

Bronze is a **faithful, immutable copy of the source**. Each row is:

| Column | Source | Why |
|---|---|---|
| `key` | Kafka message key | the `product_id` the producer keyed on |
| `value` | Kafka message value | the **exact JSON string** Coinbase sent |
| `topic`, `partition`, `offset` | Kafka metadata | provenance + exact position in the log |
| `kafka_timestamp`, `kafka_timestamp_type` | Kafka metadata | broker-side event time |
| `ingest_timestamp` | `current_timestamp()` | when *we* landed it |

Crucially, there is **no JSON parsing, casting, or cleaning** here. That's
silver's job. Keeping bronze raw is a deliberate principle:

- **Replayability** — if a bug is found in the silver/gold logic later, we can
  reprocess from bronze without re-fetching from Coinbase (which is live-only and
  can't be replayed).
- **Audit** — bronze is the ground truth of "what the source actually sent,"
  including malformed messages. We never edit or delete it (append-only).

## Immutability

Bronze is **append-only**: the stream only ever adds rows; nothing updates or
deletes. Combined with Kafka retention, this is what makes backfills and
the replay/Kappa story real. Corrections happen *downstream* (silver/gold), never
by mutating bronze.

## Checkpointing + exactly-once

The write configures a **checkpoint** at `s3a://bronze/_checkpoints/trades`. This
is what upgrades the pipeline from "at-least-once" to **exactly-once**:

1. For each micro-batch, Structured Streaming writes the **Kafka offsets** that
   batch will cover into the checkpoint's `offsets/` log *before* processing.
2. The **Delta sink** commits that batch atomically (one `_delta_log` commit) and
   records the streaming **batch id** in the commit (`txnAppId`/`txnVersion`).
3. On a crash and restart, the stream reads the checkpoint to resume at the right
   offsets, and Delta **rejects a re-commit of a batch id it already committed**.

So the two failure modes are both closed:
- **No data loss** (the risk of at-most-once): offsets aren't advanced until the
  data is durably committed.
- **No duplicates** (the risk of naive at-least-once): a replayed batch is
  deduplicated by Delta's idempotent, batch-id-keyed commit.

`startingOffsets=earliest` only applies on the *first* run (empty checkpoint);
after that the checkpoint is the source of truth for where to resume. Delete the
checkpoint and you'd re-ingest from the beginning (creating duplicates in bronze),
which is why the checkpoint is part of the table's contract, not a throwaway.

## Run it

The Kafka connector JARs are baked into the Spark image (see
[`spark_jobs/Dockerfile`](../spark_jobs/Dockerfile)), so rebuild once if the image
changed:

```bash
docker compose build spark

# stream Kafka -> bronze (Ctrl+C to stop after a few batches)
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master local[*] bronze_ingest.py
```

You'll see `[progress] batch=N inputRows=... rows/s=...` lines as micro-batches
land. Then inspect the result:

- MinIO console (http://localhost:9001) → `bronze` → `trades` (Parquet files +
  `_delta_log/`) and `_checkpoints/trades/` (the `offsets/` and `commits/` logs).
- Or a quick count via a Spark shell/job reading `s3a://bronze/trades`.

Because the producer is still streaming live trades into Kafka, re-running this
job resumes from the checkpoint and appends only the *new* trades since last run.
