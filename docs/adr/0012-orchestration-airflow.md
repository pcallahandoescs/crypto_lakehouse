# ADR 0012: Orchestration with Apache Airflow

**Status:** Accepted
**Date:** 2026-07-15

## Context

The lakehouse has batch Spark jobs (silver, gold, DQ, backfill) that must run in
order, on a schedule, with retries and an audit trail. Streaming ingestion stays
outside orchestration (long-lived Structured Streaming queries).

We need an orchestration layer before wiring DAGs (Day 19).

## Decision

Run **Apache Airflow 2.10** in Docker Compose with:

- **PostgreSQL** for metadata
- **LocalExecutor** for local development (scheduler executes tasks in-process)
- DAG code in `airflow/dags/` mounted into containers
- **`orchestration` Compose profile** so Airflow is optional on plain `docker compose up`

Default UI credentials are local-dev only (`admin` / `admin`).

## Consequences

**Positive**

- Industry-standard orchestration; interview-familiar
- Dependency graph, scheduling, retries, and UI out of the box
- Clear separation: Airflow coordinates, Spark computes

**Negative**

- Another stack to run locally (Postgres + webserver + scheduler)
- LocalExecutor does not isolate tasks (production would use Celery/Kubernetes executor)
- Day 19 must bridge Airflow → Spark (DockerOperator or similar)

## Alternatives considered

- **Cron + shell** — no DAG model or visibility; rejected.
- **Prefect / Dagster** — viable; Airflow chosen for breadth of adoption in DE roles.
- **Run everything manually** — fine for Week 2 demo, not for Week 3 production-grade goal.
