# 0010. Z-ordering vs. liquid clustering

- **Status:** Proposed (to be decided Day 14)
- **Date:** _TBD_

## Context

Beyond partitioning, Delta offers ways to **co-locate related data** so the
engine reads fewer files (data skipping):

- **`OPTIMIZE ... ZORDER BY (col)`** — rewrites files so rows with similar values
  in the Z-order columns sit together, improving multi-dimensional skipping. It's
  a periodic, manual maintenance operation and composes with directory
  partitioning.
- **Liquid clustering (`CLUSTER BY`)** — the modern successor: clustering keys are
  a table property Delta maintains incrementally, with no manual partition tuning.
  It targets high-cardinality keys and shifting query patterns, and is **mutually
  exclusive** with partitioning.

Which to use (and whether OSS Delta supports liquid clustering cleanly in our
version) can only be judged against real tables and query patterns on Day 14.

## Decision

_Deferred to Day 14._ Plan: apply `OPTIMIZE`/`ZORDER` to gold and **measure**
data-skipping impact; **evaluate** liquid clustering and implement it on one
table if the OSS Delta version supports it cleanly, otherwise record it as a
reasoned decision (honesty rule — only claim what we actually run).

## Consequences

_To be filled on Day 14, including data-skipping stats (files read before vs.
after) and the liquid-clustering support finding._

## Alternatives considered

_To be filled on Day 14 (compaction only; Z-order; liquid clustering; do
nothing)._
