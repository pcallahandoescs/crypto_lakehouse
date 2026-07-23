# 0006. Lambda architecture (speed + batch)

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

The project has two distinct consumer needs: **low-latency** real-time metrics
(a live dashboard) and **correct, complete** historical aggregates that can be
recomputed and backfilled. These optimize for different things — freshness vs.
correctness — and are hard to serve well with a single naive path.

## Decision

We will build a **Lambda architecture**: a streaming **speed layer** (Spark
Structured Streaming → `gold_realtime`) for fresh windowed metrics, and a
**batch layer** (Spark batch → bronze → silver → gold) for correct historical
aggregates and reprocessing, both sharing the Delta lakehouse.

## Consequences

- We can *demonstrate* the speed/batch distinction and reconcile the two paths —
  strong senior signal — rather than merely naming it.
- Cost: two code paths to build and maintain (the well-known Lambda downside).
- Kafka retention + immutable bronze give the batch path replay, and also set up
  the counter-argument below.

## Alternatives considered

- **Kappa (single streaming path + replay)** — simpler operationally: one code
  path, reprocess by replaying the log. We would choose Kappa if a single
  streaming job with replay could meet both latency *and* correctness needs
  (and the team wanted to avoid dual maintenance). Here we intentionally build
  Lambda to exercise both paths, and document Kappa as the path we'd consolidate
  toward in production. (Revisit as a possible superseding ADR.)
- **Batch-only** — no real-time story; fails the live-dashboard requirement.
- **Streaming-only, no history** — no backfill/correctness guarantees. Rejected.
