# Runbook: start the full stack & verify end-to-end

Week 2 milestone: a complete **Lambda** pipeline in Docker Compose — live
Coinbase trades → Kafka → bronze → silver → gold (batch) **and** gold_realtime
(speed layer).

## Prerequisites

- Docker Desktop running (give it **6–8 GB RAM** if you run Spark alongside Kafka)
- Repo cloned, `uv` installed for local Python tooling

## 1. Start the backbone

```bash
docker compose up -d kafka minio producer createbuckets
docker compose ps          # kafka healthy, minio + producer up
```

Create the Kafka topic once (idempotent):

```bash
./scripts/create_topics.sh
```

## 2. Batch layer (bronze → silver → gold)

Run each job in order. Use quotes around `"local[*]"` in zsh.

**Bronze** — stream Kafka → Delta (Ctrl+C after a few batches, or let it tail):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g bronze_ingest.py
```

**Silver** — parse, type, dedup bronze → silver:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g silver_transform.py
```

**Gold** — OHLC candles + VWAP (batch, partitioned by `date`):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g gold_aggregate.py
```

## 3. Speed layer (real-time metrics)

Needs **live** trades (`startingOffsets=latest`). Leave running ~3 minutes so
watermarked windows finalize:

```bash
docker compose start producer   # if stopped

docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g speed_metrics.py
```

## 4. Verify counts (the smoke check)

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py
```

Expected shape:

| Check | What “good” looks like |
|---|---|
| `silver dedup clean?` | `True` (rows == distinct `(product_id, trade_id)`) |
| `gold grain clean?` | `True` (rows == distinct `(product_id, interval_start)`) |
| `gold_realtime rows` | `> 0` after the speed job ran long enough |

Browse tables in the MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`).

## 5. Data layout

After gold is populated, compact and Z-order:

```bash
# bronze: many small streaming files -> fewer, larger files
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" optimize.py s3a://bronze/trades

# gold: compaction + Z-order by product_id for data skipping
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" optimize.py s3a://gold/ohlc product_id
```

Probe liquid clustering support (OSS Delta 3.2):

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" liquid_cluster_probe.py
```

See [`data_layout.md`](./data_layout.md) for the concepts and how to read the
before/after numbers.

## 6. Data quality

After silver and gold are populated:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" dq_validate.py
```

Expect all checks **PASS** on clean data. See [`data_quality.md`](./data_quality.md).

## 7. Idempotency proof

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g prove_idempotency.py
```

See [`idempotency.md`](./idempotency.md).

## 8. Backfill

Reprocess a date range from **immutable bronze** into silver and gold (MERGE
upserts — safe to re-run). Bounds are UTC: `[--start, --end)` (end exclusive).

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" backfill.py --show-range

# Two commands on Docker Desktop (avoids OOM / exit 137):
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    backfill.py --start 2026-07-05 --end 2026-07-06 --skip-gold

docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    backfill.py --start 2026-07-05 --end 2026-07-06 --skip-silver
```

See [`backfill.md`](./backfill.md).

## 9. Airflow

```bash
docker compose --profile orchestration up -d
# UI: http://localhost:8088  (admin / admin)
curl -sf http://localhost:8088/health && echo " ok"
```

Unpause and trigger `example_lakehouse` in the UI to smoke-test. See
[`airflow.md`](./airflow.md).

Build images before first DAG run:

```bash
docker compose --profile jobs build spark
docker compose --profile orchestration build
```

**Pipeline DAGs** (unpause → trigger manually):

| DAG | What it runs |
|---|---|
| `batch_lakehouse` | gold_aggregate_btc → gold_aggregate_eth → dq_validate_silver → dq_validate_gold (nightly 03:00 UTC) |
| `backfill_lakehouse` | Parameterized backfill — trigger with `{"start_date":"2026-07-05","end_date":"2026-07-08"}` |

## 9a. Observability

Structured JSON logs on every job + a Delta run-metrics table
(`s3a://gold/_observability/runs`). See [`observability.md`](./observability.md).

```bash
# Health at a glance: rows, DQ pass/fail, freshness per layer
docker compose run --rm spark \
  /opt/spark/bin/spark-submit --master "local[*]" metrics_report.py
```

Task failures fire an Airflow `on_failure_callback` (structured `ALERT` log; set
`SLACK_WEBHOOK_URL` to also push to Slack).

## 9b. Tests & CI

Two tiers (see the README for the split):

```bash
make check       # fast gate: ruff + mypy + pytest (pure-Python) — what CI runs
make test-spark  # JVM tier: real Spark transformation + DQ tests (needs Java 17)
```

GitHub Actions runs `make check` on every push/PR (build badge in the README);
the Spark tier runs in a separate workflow.

## 10. Useful debug commands

```bash
# Producer publishing?
docker compose logs --tail 10 producer

# Peek Kafka (host listener)
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto.trades.raw \
  --max-messages 3

# Local Python quality gate
make check
```

## 11. Stop / reset

```bash
docker compose stop                    # stop services, keep data volumes
docker compose down                  # stop + remove containers
docker compose down -v               # ⚠ also deletes kafka/minio data
```

To re-ingest from scratch: delete bronze/silver/gold paths in MinIO **and** their
`_checkpoints/` directories — checkpoints are part of each stream's contract.

## Architecture at a glance

```
Coinbase WS → producer → Kafka
                          ├→ bronze_ingest (stream) → bronze/trades
                          │                              ↓
                          │                         silver_transform (stream) → silver/trades
                          │                              ↓
                          │                         gold_aggregate (batch) → gold/ohlc
                          └→ speed_metrics (stream) → gold/realtime_metrics
```

Both paths share Kafka as the source; the speed layer reads it **directly** so
batch stalls never block real-time metrics.
