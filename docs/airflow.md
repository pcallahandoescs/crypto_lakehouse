# Orchestration with Airflow

Airflow coordinates **batch** work: scheduled silver → gold runs, DQ validation,
and parameterized backfills. Streaming jobs (bronze ingest, silver transform,
speed layer) stay long-running outside Airflow.

## Why orchestration?

| Without Airflow | With Airflow |
|---|---|
| Manual `docker compose run spark ...` in order | DAG encodes **dependencies** (silver before gold) |
| Forgotten steps after an outage | **Schedule** runs overnight |
| One failure aborts the shell script | **Retries** with backoff per task |
| No history of what ran when | **UI + metadata DB** — audit trail |

Airflow does not replace Spark — it **submits** Spark jobs via Docker Compose and
tracks their lifecycle.

## Architecture (local)

```
postgres          ← Airflow metadata (DAG runs, task state)
airflow-webserver ← UI on http://localhost:8088
airflow-scheduler ← parses DAGs, runs tasks (LocalExecutor + Docker socket)
airflow/dags/     ← batch_lakehouse, backfill_lakehouse, example_lakehouse
```

The scheduler container mounts `/var/run/docker.sock` and sets `LAKEHOUSE_HOST_DIR`
to the repo path on your Mac (`${PWD}` when you `docker compose up`). Each task runs:

```bash
docker run --rm --network crypto_pipeline_project_default \
  -v "$HOST_DIR/spark_jobs:/opt/spark/work-dir" \
  crypto-lakehouse-spark:3.5.3 \
  /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g <job>.py
```

(`docker run`, not `docker compose run` — bind mounts must use **host** paths.)

## Prerequisites

Build images once:

```bash
docker compose --profile jobs build spark
docker compose --profile orchestration build airflow-scheduler
```

Backbone must be up (MinIO + Kafka) before batch DAG tasks run:

```bash
docker compose up -d kafka minio producer createbuckets
```

## Start Airflow

```bash
docker compose --profile orchestration up -d
```

Open **http://localhost:8088** (`admin` / `admin`). Port **8088** avoids clashes
with other services on 8080.

```bash
curl -sf http://localhost:8088/health && echo " ok"
```

## DAGs

| DAG | Schedule | Tasks | Purpose |
|---|---|---|---|
| **`batch_lakehouse`** | `0 3 * * *` (03:00 UTC daily) | gold_aggregate_btc → gold_aggregate_eth → dq_validate_silver → dq_validate_gold | One product/layer per Spark container (Docker memory) |
| **`backfill_lakehouse`** | Manual only | backfill_silver → backfill_gold | Parameterized date-range replay (includes silver MERGE) |
| `example_lakehouse` | Manual | hello task | Smoke test |

All DAGs are **paused at creation** — unpause before the schedule fires or trigger
manually.

### Run the batch DAG manually

1. UI → **batch_lakehouse** → Unpause → **Trigger DAG**
2. Graph: `gold_aggregate_btc` → `gold_aggregate_eth` → `dq_validate_silver` → `dq_validate_gold` (sequential — not parallel; avoids OOM)
3. Task logs show `docker run ... spark-submit` output

On failure, each task fires `on_failure_callback` (structured `ALERT` log; set
`SLACK_WEBHOOK_URL` to push to Slack). See [`observability.md`](./observability.md).

**Note:** `silver_batch.py` is intentionally **not** in this DAG — a full bronze→silver
MERGE over ~165k rows OOMs on Docker Desktop (exit 137). Use **backfill_lakehouse**
when you need to reprocess silver from bronze.

Default retries: **2** with 5-minute delay between attempts.

### Run the backfill DAG

1. UI → **backfill_lakehouse** → **Trigger DAG w/ config**
2. JSON params (match your bronze event-time span):

```json
{
  "start_date": "2026-07-05",
  "end_date": "2026-07-08"
}
```

3. Graph: `backfill_silver` (--skip-gold) → `backfill_gold` (--skip-silver)

Two tasks mirror the OOM-safe CLI pattern (separate container runs).

## Repository layout

```
airflow/
  Dockerfile    # Airflow + Docker CLI for compose submits
  dags/         # DAG definitions
  plugins/
    lakehouse/
      spark_compose.py   # shared spark-submit bash helper
  logs/         # task logs (gitignored)
```

## Stop

```bash
docker compose --profile orchestration down
```

## Alternatives considered

| Tool | Why not (for this project) |
|---|---|
| **Cron + shell scripts** | No dependency graph, retries, or UI |
| **Prefect / Dagster** | Strong modern choices; Airflow matches README target |
| **Spark-only scheduling** | No cross-job orchestration or backfill UI |

See [`docs/adr/0012-orchestration-airflow.md`](./adr/0012-orchestration-airflow.md).
