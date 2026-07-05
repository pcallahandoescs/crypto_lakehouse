# 0009. Gold-table partitioning strategy

- **Status:** Proposed (to be decided Day 14, once gold tables hold real data)
- **Date:** _TBD_

## Context

Physical data layout determines query cost on a lakehouse. Partitioning splits a
table into subdirectories by column value so the engine can **prune** irrelevant
data. But the choice is a real tradeoff: partitioning on a high-cardinality
column (or too fine a time grain) creates the **small-files problem** — thousands
of tiny files that *slow* reads and bloat metadata. This can only be decided
meaningfully against populated tables and measured query patterns, which don't
exist until Week 2.

## Decision

_Deferred._ Candidate: partition gold by **event date** (and possibly
`product_id`), sized to the observed volume. To be finalized on Day 14 with
before/after measurements. This ADR is reserved now so the decision is explicit
and not forgotten.

## Consequences

_To be filled on Day 14, including measured file counts and query times before
vs. after, and the over-partitioning pitfalls actually observed._

## Alternatives considered

_To be filled on Day 14 (no partitioning; partition by date; date + product;
superseded entirely by liquid clustering — see
[ADR 0010](./0010-zorder-vs-liquid-clustering.md))._
