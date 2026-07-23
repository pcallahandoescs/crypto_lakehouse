# 0005. Table format: Delta Lake

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

Raw files on object storage (Parquet in a bucket) give you no transactions, no
schema safety, and no way to safely concurrent-write or time-travel. A
lakehouse needs a **table format** layered over the files that adds ACID
guarantees, schema enforcement/evolution, and metadata for fast reads —
turning "files in a bucket" into real tables.

## Decision

We will use **Delta Lake** as the table format for all medallion tables. Delta
maintains a transaction log (`_delta_log`) that is the source of truth for a
table's state, delivering ACID commits, schema enforcement + optional evolution
(`mergeSchema`), time travel, and — with Spark — the `OPTIMIZE`/`ZORDER`/`VACUUM`
layout toolkit and (version-permitting) liquid clustering.

## Consequences

- ACID on object storage: safe concurrent/idempotent writes (MERGE), atomic
  commits enabling **exactly-once** streaming into bronze, and time travel for
  audit/debugging.
- Schema enforcement stops bad writes at the table boundary; evolution provides a
  controlled path for additive change (proven with a schema-drift test).
- Ties us to the Delta ecosystem (best-in-class with Spark, which we already
  use); interop is improving but historically Delta-centric.

## Alternatives considered

- **Apache Iceberg** — open, engine-neutral, excellent; the main rival. Delta
  chosen for tightest Spark integration and because it's a format widely used in
  the target finance/data environments. Would revisit Iceberg for multi-engine
  or vendor-neutral requirements.
- **Apache Hudi** — strong for upserts/CDC; smaller mindshare for this use.
- **Plain Parquet** — no ACID, no schema safety, no time travel. Rejected.
