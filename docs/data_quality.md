# Data quality

Trust built into the pipeline — not a separate audit you run once and forget.
[`spark_jobs/dq.py`](../spark_jobs/dq.py) holds reusable PySpark checks;
[`dq_validate.py`](../spark_jobs/dq_validate.py) runs the full suite on silver
and gold after the pipeline runs.

## Why custom checks (not Great Expectations)

Great Expectations is the industry choice at org scale (expectation suites, data
docs, cloud integrations). For this project, **custom PySpark checks** keep the
DQ logic visible, testable in-repo, and free of another orchestrated service.
The *concepts* are the same — expectations map 1:1 to our `CheckResult` checks.

## Quality dimensions we cover

| Dimension | Silver | Gold |
|---|---|---|
| **Completeness** | null `product_id`, `trade_id`, price, size, time | null grain, null OHLC |
| **Uniqueness** | distinct `(product_id, trade_id)` | distinct `(product_id, interval_start)` |
| **Validity** | price > 0, size > 0, side ∈ {buy,sell} | volume ≥ 0, OHLC sane (high ≥ low, open/close in range) |
| **Timeliness** | max `event_time` within `DQ_FRESHNESS_HOURS` (default 168h) | — |
| **Consistency** | typed Delta schema (enforced on write) | typed + partitioned schema |
| **Accuracy** | row-count drift vs prior run | row-count drift vs prior run |

Row-count drift is a **sanity** check (catch accidental wipes), not a truth
test — it compares to the last recorded count in `s3a://gold/_dq/metrics`.

## Fail vs quarantine

| Layer | Strategy |
|---|---|
| **Silver stream** | Invalid parse/contract rows → **`silver/quarantine/trades`** (not silently dropped) |
| **Silver/gold batch DQ** | Row-level violations found in trusted tables → quarantine tables + **console alert** |
| **Aggregate checks** | Log **FAIL**; `dq_validate.py` exits 1 if any check fails (CI/Airflow hook point) |

Quarantine tables are append-only audit trails: `value` + `reason` + `quarantined_at`
(silver stream rejects) or full row copy + reason (batch re-scan).

## Wired into the pipeline

- **`silver_transform.py`** — second writeStream sends parse failures to quarantine.
- **`gold_aggregate.py`** — runs gold DQ checks and saves metrics after each write.
- **`dq_validate.py`** — standalone full suite (silver + gold + quarantine sweep).

## Run it

After silver and gold are populated:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" dq_validate.py
```

Expect all **PASS** on clean data. Re-running updates the metrics baseline (row
count drift passes if counts are stable).

Tune for prod-like freshness:

```bash
docker compose run --rm -e DQ_FRESHNESS_HOURS=2 spark \
    /opt/spark/bin/spark-submit --master "local[*]" dq_validate.py
```

Browse quarantine in MinIO: `silver/quarantine/trades`, `gold/quarantine/ohlc`.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `DQ_FRESHNESS_HOURS` | `168` | Max age of latest silver `event_time` |
| `DQ_MIN_ROW_COUNT_RATIO` | `0.95` | Min ratio vs prior run row count |
