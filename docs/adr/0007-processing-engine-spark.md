# 0007. Processing engine: Spark / PySpark

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

We need an engine for both the batch medallion transforms and the streaming
speed layer. Using one engine for both reduces cognitive load, shares code and
skills, and integrates cleanly with Delta and S3A (MinIO).

## Decision

We will use **Spark (PySpark)** for both **Structured Streaming** (bronze
ingestion, speed layer) and **batch** (silver/gold, backfills). Spark reads
Kafka, writes Delta on MinIO via S3A, and provides the `OPTIMIZE`/`ZORDER`/
`VACUUM` layout tools.

## Consequences

- One engine, two paradigms (stream + batch) with a largely shared API —
  smaller surface area to learn and operate; directly relevant day-to-day skill.
- First-class Delta + S3A integration; the trickiest wiring (Spark ↔ S3A ↔
  Delta) is validated in isolation before real jobs run on it.
- Heavier runtime (JVM, cluster semantics) than lightweight alternatives; fine
  for a realistic, scalable design and a single-node local cluster.

## Alternatives considered

- **Flink** — stream-native, excellent for low-latency/complex event processing.
  Spark chosen because one engine covers both batch and streaming here and is the
  more common lakehouse skill. Would consider Flink for a streaming-first system.
- **dbt** — great for SQL transforms on a warehouse, but not for streaming or
  raw Kafka ingestion. Complementary, not a replacement here.
- **Pandas/Polars** — single-node only; wrong tool for a scalable pipeline.
  Rejected.
