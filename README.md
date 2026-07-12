# Real-Time Crypto Market Data Lakehouse

> A production-grade, end-to-end data platform: live crypto trades streamed from the
> Coinbase WebSocket, buffered through Kafka, processed by Spark into a Delta Lake
> medallion architecture on MinIO, orchestrated with Airflow, served via FastAPI +
> a live dashboard, and deployed on Kubernetes.

**Status:** Building in public — **Week 2 complete (Day 14 — full Lambda pipeline
end-to-end in Compose)**. Live trades flow Kafka → bronze → silver → gold, plus a
real-time speed layer. See the [runbook](./docs/runbook.md) to reproduce the stack.

Full design and the reasoning behind every choice:
[`ARCHITECTURE.md`](./ARCHITECTURE.md) · decisions log [`docs/adr/`](./docs/adr/) ·
[runbook](./docs/runbook.md).

---

## Why this project

It exercises all five data-lifecycle stages (generation → ingestion → storage →
processing → serving) and the engineering undercurrents that separate a real data
platform from a demo: **data quality, idempotency, replay/backfill, orchestration,
observability, testing, and containerized deployment**. It is built as a **Lambda
architecture** (a real-time speed layer *and* a correct batch layer) on purpose, so
the design tradeoffs — including *when you'd instead choose Kappa* — are demonstrable,
not just nameable.

## Architecture (target)

```mermaid
flowchart TD
    CB["Coinbase WebSocket<br/>(live trades)"] -->|GENERATION| K["Apache Kafka<br/>(KRaft mode)"]
    K -->|INGESTION / durable log| SPEED["Speed Layer<br/>Spark Structured Streaming<br/>(windowed real-time metrics)"]
    K --> BATCH["Batch Layer<br/>Spark batch jobs<br/>(bronze → silver → gold, backfills)"]

    subgraph LAKE["Delta Lakehouse on MinIO (S3-compatible) — STORAGE"]
        BRONZE["bronze<br/>(raw, immutable)"] --> SILVER["silver<br/>(clean, typed, deduped)"] --> GOLD["gold<br/>(OHLC candles, VWAP)"]
        GOLD_RT["gold_realtime<br/>(rolling metrics)"]
    end

    SPEED --> GOLD_RT
    BATCH --> BRONZE
    GOLD --> API["FastAPI<br/>(data API)"]
    GOLD_RT --> API
    API --> DASH["Dashboard<br/>(live candlesticks)"]

    AIRFLOW["Airflow<br/>(orchestrates batch + backfills)"] -.-> BATCH
```

> This diagram is the target design; components come online across Weeks 1–4.
> See [`ARCHITECTURE.md`](./ARCHITECTURE.md) for the full design, the Lambda
> rationale, and current build status.

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Source | Coinbase WebSocket | Free, real, legal real-time market data |
| Ingestion | Apache Kafka (KRaft) | Durable, replayable event log; decouples producers/consumers |
| Object storage | MinIO | Self-hosted, S3-compatible; portable to any cloud |
| Table format | Delta Lake | ACID + time-travel + schema enforcement on object storage |
| Processing | Spark / PySpark | One engine for both streaming and batch |
| Orchestration | Airflow | Industry-standard DAG orchestration for batch/backfills |
| Serving | FastAPI | Modern Python data API |
| Dashboard | Streamlit | Fastest path to a live, demoable chart |
| Containers | Docker + Compose | Reproducible local stack |
| Deployment | Kubernetes (kind) | Container orchestration: scaling, self-healing |

Alternatives considered for each choice are documented in the
[decisions log](./docs/adr/).

## Repository layout

```
producer/     # Coinbase WebSocket -> Kafka producer service
spark_jobs/   # Spark Structured Streaming + batch jobs (bronze/silver/gold)
airflow/      # DAGs orchestrating the batch layer + backfills
serving/      # FastAPI service querying the gold Delta tables
dashboard/    # Live candlestick dashboard
k8s/          # Kubernetes manifests / Helm chart
docs/         # Data schema, Kafka setup, data contract
docs/adr/     # Architecture Decision Records (the decisions log)
tests/        # Unit tests (transformations, DQ logic)
```

## Development

Requires [uv](https://docs.astral.sh/uv/) and Docker.

```bash
make install      # sync the virtualenv from pyproject + uv.lock
make check        # lint + format-check + typecheck + tests (the CI gate)
make hooks        # install pre-commit git hooks
```

Quality tooling: **ruff** (lint + format), **mypy** (strict typing), **pytest**.

## Roadmap

- **Week 1** — Foundations & ingestion: live data flowing into Kafka, containerized.
- **Week 2** — Lakehouse & processing: full Lambda pipeline end-to-end in Compose. **Done.**
- **Week 3** — Production rigor: data quality, idempotency, replay, orchestration, tests.
- **Week 4** — Serving & deployment: FastAPI + dashboard, Kubernetes + Helm, docs.

## License

[MIT](./LICENSE)
