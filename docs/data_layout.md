# Data layout & optimization

Once tables hold real data, **physical layout** determines query cost on a
lakehouse. This doc explains the toolkit we applied and how to measure it with
[`optimize.py`](../spark_jobs/optimize.py).

## The evolution

1. **Partitioning** — prune whole directories by a low-cardinality filter column
   (usually date).
2. **Compaction (`OPTIMIZE`)** — bin-pack many small files into fewer, right-sized
   ones (fixes the small-files problem streaming creates).
3. **Z-ordering (`OPTIMIZE ... ZORDER BY`)** — co-locate related rows inside files
   so Delta's per-file **min/max statistics** skip more data on multi-dimensional
   filters.
4. **Liquid clustering (`CLUSTER BY`)** — the modern successor that maintains
   clustering incrementally without manual partition tuning; **mutually exclusive**
   with partitioning. We **evaluated** it on our Delta version (see below).

These compose: partition for coarse pruning, compact for file count, Z-order for
fine-grained skipping *within* partitions.

## Partitioning — directory pruning

Gold is written with:

```python
.withColumn("date", F.to_date("interval_start"))
.write.partitionBy("date")
```

Physical layout becomes `gold/ohlc/date=2026-07-07/...`. A query filtering
`date = '2026-07-07'` can **skip** other date directories entirely — the engine
never lists or reads them.

### Why we did NOT partition by `product_id` or per-minute time

| Choice | Problem at our scale |
|---|---|
| `product_id` (2 values) | Almost no pruning benefit — you'd scan ~half the table anyway |
| `interval_start` (minute grain) | **Over-partitioning** — ~one tiny file per candle → metadata bloat, slow listing, worse than no partitions |
| `date` | Matches typical “show me this day” queries; ~one directory per calendar day |

**Rule of thumb:** partition on columns you *always* filter, with **low-to-moderate**
cardinality. High-cardinality or too-fine grain → thousands of tiny files.

Silver and bronze stay **unpartitioned** for now — streaming append creates enough
small files that compaction matters more than directory layout at this volume.

## Small files & compaction

Structured Streaming commits a new Parquet file (often small) every micro-batch.
Over days, bronze accumulates **many small files**. Each file carries metadata
overhead; readers open more files; listing gets slower.

`OPTIMIZE` **rewrites** small files into fewer, larger ones (bin-packing) without
changing logical data. Run it on bronze after heavy ingestion:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" optimize.py s3a://bronze/trades
```

Watch **`numFiles`** in the before/after output — that's the concrete signal.

## Z-ordering & data skipping

Partition pruning works on **directory** boundaries. **Z-order** works **inside**
files: rows with similar `product_id` values are co-located, so each file's
stored min/max stats let Delta skip files that can't contain `product_id = 'BTC-USD'`.

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" optimize.py s3a://gold/ohlc product_id
```

On a tiny local table, query time may barely move — the teaching value is **`numFiles`**
and the **`zOrderStats`** line in the optimize metrics, not sub-second latency
deltas. At production scale (TB, millions of files), skipping dominates.

## VACUUM & time travel

`OPTIMIZE` rewrites data files; the old files remain on disk until **`VACUUM`**
removes them. Delta keeps unreferenced files for a **retention window** (default
**7 days**) so **time travel** (`VERSION AS OF`) still works.

Our script runs **`VACUUM ... DRY RUN`** — it lists what *would* be deleted without
deleting. Never run a real `VACUUM` with a short retention on production without
understanding you may break old time-travel queries.

## Liquid clustering — evaluated on Delta 3.2.0

[`liquid_cluster_probe.py`](../spark_jobs/liquid_cluster_probe.py) creates a
throwaway table with:

```sql
CREATE TABLE ... USING DELTA CLUSTER BY (product_id, event_time)
```

**Result on our stack:** supported. `DESCRIBE DETAIL` reports
`clusteringColumns=[product_id, event_time]` after `OPTIMIZE`.

We still use **date partitioning + Z-order on `gold/ohlc`** because liquid
clustering is **mutually exclusive** with `partitionBy` — adopting it would
require rewriting gold without date partitions. At Week-2 scale (572 candles, 2
products), partition + Z-order is the simpler, measurable choice. Liquid
clustering is the documented path for a **new** table with high-cardinality keys
and shifting query patterns.

## Related ADRs

- [0009 — Gold-table partitioning strategy](./adr/0009-partitioning-strategy.md)
- [0010 — Z-ordering vs. liquid clustering](./adr/0010-zorder-vs-liquid-clustering.md)
