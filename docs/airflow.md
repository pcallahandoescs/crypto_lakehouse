# Orchestration with Airflow (Day 18)

Airflow coordinates **batch** work: scheduled silver → gold runs, DQ validation,
and parameterized backfills. Streaming jobs (bronze ingest, silver transform,
speed layer) stay long-running outside Airflow — orchestration targets jobs with
a clear start and end.

## Why orchestration?

| Without Airflow | With Airflow |
|---|---|
| Manual `docker compose run spark ...` in order | DAG encodes **dependencies** (silver before gold) |
| Forgotten steps after an outage | **Schedule** runs overnight |
| One failure aborts the shell script | **Retries** with backoff per task |
| No history of what ran when | **UI + metadata DB** — audit trail |

Airflow does not replace Spark — it **submits** Spark jobs (Day 19) and tracks
their lifecycle.

## Architecture (local)

```
postgres          ← Airflow metadata (DAG runs, task state, connections)
airflow-webserver ← UI on http://localhost:8088
airflow-scheduler ← parses DAGs, queues tasks (LocalExecutor runs them in-process)
airflow/dags/     ← Python DAG files (mounted into containers)
```

**LocalExecutor** is enough for laptop dev: the scheduler process executes tasks
directly. Production would use Celery/Kubernetes executors for isolation and scale.

## Start Airflow

Requires the backbone (MinIO/Kafka) only if DAGs call Spark — the UI itself needs
just Postgres + Airflow:

```bash
docker compose --profile orchestration up -d postgres airflow-init airflow-webserver airflow-scheduler
```

Or shorthand (starts init once, then webserver + scheduler):

```bash
docker compose --profile orchestration up -d
```

Wait ~30s, then open **http://localhost:8088**

(Port **8088** on the host — 8080 is often taken by other local services.)

| Field | Value |
|---|---|
| Username | `admin` |
| Password | `admin` |

Check health:

```bash
docker compose --profile orchestration ps
curl -sf http://localhost:8088/health && echo " webserver ok"
```

## Smoke-test DAG

[`example_dag.py`](../airflow/dags/example_dag.py) defines `example_lakehouse` —
a single Python task that prints a confirmation string.

1. Open the UI → **DAGs**
2. Find `example_lakehouse` (paused by default)
3. Toggle **Unpause**
4. Click **Trigger DAG** (play button)
5. Open the run → **Graph** → task should go green

DAGs are **paused at creation** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`) so
a typo in new code does not immediately flood the scheduler.

## Repository layout

```
airflow/
  dags/       # Python DAG files (Day 19: batch + backfill DAGs)
  plugins/    # custom hooks/operators (empty for now)
  logs/       # task logs (gitignored, created at runtime)
```

## Day 19 preview

Next up:

- **Batch DAG** — silver batch → gold aggregate → `dq_validate.py`, with retries
- **Backfill DAG** — parameterized `--start` / `--end` calling `backfill.py`

Those DAGs will use `DockerOperator` (or `docker compose run`) to submit the
existing Spark job container against MinIO.

## Stop

```bash
docker compose --profile orchestration down
# add -v to wipe postgres metadata (DAG run history)
```

## Alternatives considered

| Tool | Why not (for this project) |
|---|---|
| **Cron + shell scripts** | No dependency graph, retries, or UI |
| **Prefect / Dagster** | Strong modern choices; Airflow is the industry default and matches README target |
| **Spark-only scheduling** | Spark has no first-class cross-job orchestration or backfill UI |

See [`docs/adr/0012-orchestration-airflow.md`](./adr/0012-orchestration-airflow.md).
