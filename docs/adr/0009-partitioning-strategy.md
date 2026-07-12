# 0009. Gold-table partitioning strategy

- **Status:** Accepted
- **Date:** 2026-07-12

## Context

Physical data layout determines query cost on a lakehouse. Partitioning splits a
table into subdirectories by column value so the engine can **prune** irrelevant
data. But the choice is a real tradeoff: partitioning on a high-cardinality
column (or too fine a time grain) creates the **small-files problem** — thousands
of tiny files that *slow* reads and bloat metadata.

Gold OHLC holds ~572 candles across two products and two calendar days at Week-2
scale — enough to decide layout principles, not enough for dramatic latency wins.

## Decision

Partition **`gold/ohlc` by `date`** (`to_date(interval_start)`), **not** by
`product_id` or minute-level `interval_start`.

- **`date`** matches the typical analytical filter (“trades/candles for July 7”)
  with low cardinality (one directory per calendar day).
- **`product_id`** is deferred to **Z-ordering** ([ADR 0010](./0010-zorder-vs-liquid-clustering.md))
  — only two products, so a partition dimension would barely prune.
- **Minute grain** would be textbook **over-partitioning** (~one file per row at
  our candle grain).

Silver and bronze remain unpartitioned; streaming append + periodic `OPTIMIZE`
is the right maintenance model there first.

## Consequences

- Queries filtering by `date` skip other date directories (`SHOW PARTITIONS` in
  [`optimize.py`](../spark_jobs/optimize.py) confirms layout).
- Re-running `gold_aggregate.py` with `overwriteSchema=true` rebuilds the
  partitioned table from silver.
- Adding many more products or years increases partition count linearly by **days**,
  not by rows — the intended scaling shape.

## Alternatives considered

| Alternative | Why not (here) |
|---|---|
| No partitioning | Simpler, but no directory pruning as gold grows |
| Partition by `product_id` | Only 2 values → ~50% scan anyway; Z-order handles product filters inside files |
| Partition by `date` + `product_id` | Doubles directory count for negligible gain at 2 products |
| Liquid clustering instead | Evaluated separately — see ADR 0010; not supported cleanly on Delta 3.2.0 |
