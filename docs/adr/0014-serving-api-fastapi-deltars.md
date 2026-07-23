# ADR 0014: Serving API — FastAPI reading Delta with delta-rs (no Spark)

**Status:** Accepted
**Date:** 2026-07-18

## Context

The gold layer holds the analytical products — OHLC candles, speed-layer metrics,
run observability — but nothing exposes them to a consumer (a dashboard, an
analyst, a model). We need a read API that returns gold rows over HTTP with
low, request-scoped latency.

The obvious reflex is "reuse Spark," but a `SparkSession` costs seconds to start
and a JVM's worth of memory — fine for a batch job, wrong for a request/response
service that must answer in milliseconds and scale horizontally.

## Decision

A **FastAPI** service (`serving/`) that reads the gold Delta tables directly with
**delta-rs** (`deltalake`, a Rust implementation) — **no JVM in the request path**.

- delta-rs reads the `_delta_log` + Parquet from MinIO via its S3 object-store
  backend, configured with the same `AWS_*` credentials Spark uses, and returns
  Apache Arrow. Opening a table is cheap enough to do per request.
- Data access is isolated in one class, `serving/store.DeltaStore`. The
  high-selectivity `product_id` filter is pushed down to Arrow; the time-range
  clip, ordering, and limit run in Python (gold candle counts per product are
  small). A missing table returns `[]`, not a 500 — an unrun job is a normal
  early-lifecycle state.
- Endpoints: `/health` (liveness + per-table reachability), `/products`,
  `/candles` (historical OHLC), `/metrics/realtime` (speed layer),
  `/metrics/runs` (the observability table from ADR 0013). OpenAPI docs at `/docs`.
- Prices/volume stay `Decimal` end to end (exact money, per ADR 0008); VWAP and
  volatility are float, matching how the Spark jobs derive them.

## Consequences

**Positive**

- Fast, lightweight, horizontally scalable: a slim image with no JVM or Spark.
- The store is dependency-injected, so the full HTTP surface is unit-tested with
  an in-memory fake — the fast gate needs no MinIO, Spark, or network.
- Cloud-portable: point `MINIO_ENDPOINT` at real S3/GCS/ADLS, no code change.

**Negative**

- Two Delta readers now exist (Spark for writes/heavy jobs, delta-rs for serving);
  they must stay protocol-compatible (both track the Delta spec, so low risk).
- Reads open the table per request and clip in Python — fine at this scale; a
  higher-traffic deployment would add table-handle caching and push the
  time-range predicate down to Arrow.

## Alternatives considered

- **PySpark in the API** — rejected: multi-second session startup and JVM memory
  make it unsuitable for request/response serving.
- **DuckDB `delta_scan`** — attractive (SQL over Delta), but adds an extension
  that auto-downloads at runtime; delta-rs + Arrow is a smaller, self-contained
  dependency for what is currently simple, single-table reads.
- **Precompute into Postgres/Redis** — a real option at scale, but adds a second
  store to keep in sync; serving Delta directly avoids that duplication here.
