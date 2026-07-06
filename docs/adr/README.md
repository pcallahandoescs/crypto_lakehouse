# Architecture Decision Records (ADRs)

An **ADR** captures one significant architectural decision: the context that
forced a choice, the decision itself, the alternatives considered, and the
consequences (good and bad). They are short, immutable, and numbered — a
decision is never edited away; if it changes, a new ADR supersedes it.

Why keep them: the *why* behind a system is the first thing lost to time and the
most valuable thing to a new engineer (or an interviewer). ADRs make the
reasoning durable. See [ADR 0001](./0001-record-architecture-decisions.md).

## Status legend

- **Accepted** — decided and in effect.
- **Proposed** — placeholder for a decision we will make later (with the date/day
  it's due). We reserve these so the choice is never forgotten.
- **Superseded by NNNN** — replaced by a later ADR.

## Index

| # | Title | Status |
|---|---|---|
| [0001](./0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](./0002-data-source-coinbase-websocket.md) | Data source: Coinbase WebSocket | Accepted |
| [0003](./0003-ingestion-kafka-kraft.md) | Ingestion: Apache Kafka in KRaft mode | Accepted |
| [0004](./0004-object-storage-minio.md) | Object storage: MinIO (S3-compatible) | Accepted |
| [0005](./0005-table-format-delta-lake.md) | Table format: Delta Lake | Accepted |
| [0006](./0006-lambda-architecture.md) | Lambda architecture (speed + batch) | Accepted |
| [0007](./0007-processing-engine-spark.md) | Processing engine: Spark / PySpark | Accepted |
| [0008](./0008-data-contract-pydantic.md) | Data contract: Pydantic at the boundary | Accepted |
| [0009](./0009-partitioning-strategy.md) | Gold-table partitioning strategy | Proposed (Day 14) |
| [0010](./0010-zorder-vs-liquid-clustering.md) | Z-ordering vs. liquid clustering | Proposed (Day 14) |
| [0011](./0011-minio-image-chainguard.md) | MinIO container image: Chainguard | Accepted |

## Template

New ADRs follow [`template.md`](./template.md) (Nygard-style: Context /
Decision / Consequences / Alternatives).
