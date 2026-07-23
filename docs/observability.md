# Observability

Building a pipeline is one thing; **knowing whether it's healthy** is another.
The DataOps layer adds structured logs, a queryable metrics store, streaming
lag/latency, and failure alerting — mapped to the five pillars of data
observability.

## The five pillars (and where each lives)

| Pillar | Question it answers | Where we cover it |
|---|---|---|
| **Freshness** | How recent is the data? | `dq_validate` records `freshness_seconds` (age of newest `event_time`) to the run-metrics table; `metrics_report.py` prints it |
| **Volume** | Did we get the expected number of rows? | `rows` per layer in the run-metrics table + `row_count_drift` DQ check (ratio vs. prior run) |
| **Schema** | Did the shape change? | Delta **schema enforcement** (writes rejected on drift) + the Pydantic contract at ingestion; exercised by a schema-drift test |
| **Lineage** | Where did this data come from? | Medallion layering (bronze → silver → gold) with provenance columns (Kafka topic/partition/offset, `ingest_timestamp`, `silver_timestamp`); see below |
| **Quality** | Is the data correct? | DQ checks (`dq.py`) → `dq_passed` / `dq_failed` counts in the run-metrics table; quarantine tables for bad rows |

## Structured logging

Every job emits **one JSON object per log line** via `spark_jobs/observe.py`
(`JobLogger`) and the equivalent formatter in the producer. Machine-parseable
logs are the difference between "grep the console" and "query your operations" —
a log drain (Loki/CloudWatch/Datadog) can index these directly.

```json
{"ts": "2026-07-19T03:11:21Z", "level": "INFO", "job": "dq-validate", "event": "checks_complete", "layer": "silver", "rows": 165807, "dq_passed": 8, "dq_failed": 0, "freshness_seconds": 1047581.4, "ok": true}
```

Spark's own log4j output stays at `WARN`; these are the *application* events
(started, batch, checks_complete, merge_complete, throughput, stopping).

## Metrics store: the run-metrics table

`observe.record_run` appends a row to a Delta table
(`s3a://gold/_observability/runs`) after each batch job. Columns: `job`, `layer`,
`event`, `rows`, `dq_passed`, `dq_failed`, `duration_seconds`,
`freshness_seconds`, `ts`. This table **doubles as the drift baseline**
(`observe.load_prior_rows`) so there is a single metrics write per task — a
second Delta write OOMs the memory-tight Airflow tasks.

Read it at a glance:

```bash
docker compose run --rm spark \
  /opt/spark/bin/spark-submit --master local[*] metrics_report.py
```

```
job              layer         rows        dq    fresh  last_run (UTC)
------------------------------------------------------------------------
dq-validate      gold           572       6/6        -  2026-07-19 03:11
dq-validate      silver     165,807       8/8    291.0h  2026-07-19 03:11
  all latest DQ checks passing
```

(The serving layer's `/metrics` and `/health` endpoints read this same table, so
the store is built here and serving stays a thin read.)

## Streaming lag & latency

The streaming jobs (bronze, silver, speed) log per-batch progress extracted from
`StreamingQuery.lastProgress` via `observe.stream_progress_fields`:

- `input_rows`, `input_rows_per_sec`, `processed_rows_per_sec` — throughput
- `batch_duration_ms` — processing latency
- `kafka_lag` — unconsumed offsets (`latestOffset - endOffset` summed across
  partitions); the single most important streaming health signal (is the
  consumer keeping up?)

The producer logs a `throughput` event (published count + trades/sec) every 100
messages.

## Failure alerting (Airflow)

Both DAGs set `on_failure_callback = alert_on_failure`
(`airflow/plugins/lakehouse/alerts.py`). On any task failure it emits a
structured `ALERT` log line (dag, task, try count, log URL, error) that a log
drain can match and forward. Set `SLACK_WEBHOOK_URL` to also push a message to
Slack — the hook shows exactly where a real integration slots in (email /
PagerDuty follow the same shape).

Airflow already gives retries + state history; alerting adds the **push** so a
failed nightly run reaches a human instead of waiting to be noticed.

## Lineage

Lineage here is **structural**, not a separate catalog:

```
Coinbase WS → Kafka (crypto.trades.raw)
   → bronze/trades   (+ topic, partition, offset, kafka_timestamp, ingest_timestamp)
   → silver/trades   (+ silver_timestamp; parsed/typed/deduped from bronze)
   → gold/ohlc       (aggregated from silver; grain = product × interval)
   → gold/realtime_metrics (speed layer, straight from Kafka)
```

Every row carries the provenance columns to trace it back to its Kafka offset.
At scale this graph would be captured in a catalog (OpenLineage / Marquez /
Unity Catalog) — noted as the scale-up path, not built here.

## What this is not

- **No Prometheus/Grafana stack** — for a single-node pipeline the Delta
  run-metrics table + structured logs give the same signal without another
  service to run. The scale-up path (metrics exporter → Prometheus → Grafana,
  logs → Loki) is documented, not built.
- **No distributed tracing** — one JVM per job; spans would be noise here.
