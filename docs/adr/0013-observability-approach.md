# ADR 0013: Observability via structured logs + a Delta metrics table

**Status:** Accepted
**Date:** 2026-07-18

## Context

The pipeline works and is orchestrated (Day 19), but there is no operational
answer to "is it healthy?" We need the five observability pillars — freshness,
volume, schema, lineage, quality — surfaced in a way that is queryable and
alertable, on a single-node local stack, without standing up a heavyweight
monitoring cluster.

## Decision

Three lightweight primitives instead of a metrics platform:

1. **Structured JSON logging** (`spark_jobs/observe.py:JobLogger` + a matching
   producer formatter): one JSON object per line, so logs are machine-parseable
   by any drain. Spark's log4j output stays at `WARN`.
2. **A Delta run-metrics table** (`s3a://gold/_observability/runs`) written by
   `observe.record_run` after each batch job: rows, DQ pass/fail, duration,
   freshness. It **doubles as the drift baseline** (`load_prior_rows`) so each
   memory-tight task does a single Delta write. `metrics_report.py` prints it;
   the Day 22 FastAPI `/metrics` endpoint will serve it.
3. **Airflow `on_failure_callback`** (`lakehouse/alerts.py`): a structured
   `ALERT` line on any task failure, with an optional `SLACK_WEBHOOK_URL` push.

Streaming jobs additionally log lag/latency from `StreamingQuery.lastProgress`
(input/processed rate, batch duration, Kafka offset lag).

## Consequences

**Positive**

- Queryable operations (the metrics table) + greppable/indexable logs, with zero
  new services to run.
- Single metrics write per task keeps the nightly DAG within Docker Desktop
  memory limits (a second write OOMs — see Day 19).
- The metrics store is reusable: Day 22 serves it over HTTP unchanged.

**Negative**

- No time-series UI (Prometheus/Grafana) or distributed tracing — the scale-up
  path is documented, not built.
- Metrics are recorded by batch jobs; long-running streams surface health via
  logs only (no periodic stream metric write, to avoid extra state/IO).

## Alternatives considered

- **Prometheus + Grafana + Loki** — the production answer; rejected as
  operational overkill for a single-node portfolio stack. Documented as scale-up.
- **StatsD / OpenTelemetry exporter** — viable, but adds a collector; the Delta
  table already gives durable, queryable metrics for batch jobs.
- **Great Expectations data docs** — quality-only; complements but does not
  replace freshness/volume/lag telemetry.
