# 0010. Z-ordering vs. liquid clustering

- **Status:** Accepted
- **Date:** 2026-07-12

## Context

Beyond partitioning, Delta offers ways to **co-locate related data** so the
engine reads fewer files (data skipping):

- **`OPTIMIZE ... ZORDER BY (col)`** — rewrites files so rows with similar values
  in the Z-order columns sit together, improving multi-dimensional skipping. It's
  a periodic, manual maintenance operation and composes with directory
  partitioning.
- **Liquid clustering (`CLUSTER BY`)** — clustering keys are a table property Delta
  maintains incrementally, targeting high-cardinality keys and shifting query
  patterns. **Mutually exclusive** with partitioning.

Stack: **Delta 3.2.0** (see [`spark_jobs/Dockerfile`](../spark_jobs/Dockerfile)).

## Decision

1. **Use `OPTIMIZE` + `ZORDER BY (product_id)` on `gold/ohlc`** after batch writes,
   measured with [`optimize.py`](../spark_jobs/optimize.py). Composes with date
   partitioning from [ADR 0009](./0009-partitioning-strategy.md).
2. **Use `OPTIMIZE` without Z-order on `bronze/trades`** to compact streaming
   small files (file-count reduction is the primary win).
3. **Do not adopt liquid clustering on `gold/ohlc` yet** — it is **mutually
   exclusive** with `partitionBy("date")`, and date partitioning already matches
   our query pattern at this scale. We **ran**
   [`liquid_cluster_probe.py`](../spark_jobs/liquid_cluster_probe.py) and
   confirmed **`CLUSTER BY` works on Delta 3.2.0** in this stack (probe table
   shows `clusteringColumns=[product_id, event_time]`). Liquid clustering is the
   documented upgrade path for a **new** high-cardinality table (e.g. per-symbol
   facts at scale), not a drop-in replacement for the current partitioned gold
   table without a rewrite.

## Consequences

- Gold layout = **date partitions** + **product_id Z-order** inside partitions.
- Periodic maintenance job(s) needed (`optimize.py` or scheduled Airflow task in
  Week 3) — Z-order is not automatic on every write.
- `VACUUM` must respect the 7-day default retention so time travel keeps working
  after compactions rewrite files.

## Alternatives considered

| Alternative | Outcome |
|---|---|
| Compaction only (no Z-order) | Good for bronze; gold still benefits from product_id skipping as data grows |
| Z-order only (no partitioning) | Loses cheap date directory pruning |
| Liquid clustering | **Supported** on Delta 3.2.0 (probe passed), but **mutually exclusive** with partitioning — keep partition+Z-order on gold; use CLUSTER BY for a future high-cardinality table |
| Do nothing | Small-files problem worsens as streaming runs; missed teaching moment for layout toolkit |

## Measurement

Run and record the before/after results:

```bash
optimize.py s3a://bronze/trades
optimize.py s3a://gold/ohlc product_id
liquid_cluster_probe.py
```

Compare **`numFiles`** before/after; on Z-order runs, inspect **`zOrderStats`**
in the optimize metrics output.
