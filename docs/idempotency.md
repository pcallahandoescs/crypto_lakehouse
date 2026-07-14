# Idempotency & replay safety (Day 16)

Batch jobs must be **safe to re-run**: a retry after a crash, a scheduled
recompute, or a backfill (Day 17) must not duplicate rows or leave the table in
a worse state. This project uses **Delta MERGE** (upsert) keyed on deterministic
grain columns.

## The mechanism: MERGE upsert

```sql
MERGE INTO gold AS t
USING recomputed_candles AS s
ON  t.product_id = s.product_id
AND t.interval_start = s.interval_start
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Implemented in [`idempotent.py`](../spark_jobs/idempotent.py) as `merge_upsert()`:

| Outcome | Meaning |
|---|---|
| **Key exists** | Row is **updated** with the new computation (same key, fresh values) |
| **Key new** | Row is **inserted** |
| **Second identical run** | Every key matches → updates are no-ops at the logical level; **row count unchanged** |

This differs from:

| Approach | Idempotent? | Problem |
|---|---|---|
| **Append** | No | Re-run duplicates rows |
| **Full overwrite** | Yes | Rewrites entire table every run — wasteful at scale, brief empty window |
| **MERGE on grain key** | Yes | Incremental, keyed, production pattern |

Streaming layers use **checkpoints + exactly-once sinks** (Day 10). Batch layers
use **MERGE** (Day 16). Same goal — no duplicates — different tool.

## Where it's wired

| Job | Key | Module |
|---|---|---|
| **Gold** (`gold_aggregate.py`) | `(product_id, interval_start)` | MERGE + `date` partition |
| **Silver batch** (`silver_batch.py`) | `(product_id, trade_id)` | MERGE (no partition) |
| **Silver stream** (`silver_transform.py`) | watermark dedup | streaming idempotency (Day 11) |

Gold no longer uses `mode("overwrite")` — it upserts in place.

## Prove it

[`prove_idempotency.py`](../spark_jobs/prove_idempotency.py) runs gold and silver
batch MERGE **twice** and asserts the full-table row count is unchanged. Each
phase uses **four Spark sessions** (count → MERGE → MERGE → count). Each MERGE
uses a **500-row BTC-USD subset** of the target table (same MERGE SQL as
`gold_aggregate.py` / `silver_batch.py`; full-table row counts must stay flat).

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g prove_idempotency.py
```

If the combined run OOMs (exit 137), run phases in separate JVMs:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    prove_idempotency.py --gold-only

docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \
    prove_idempotency.py --silver-only
```

Expect:

```
PASS (gold, silver): double MERGE -> identical row counts and grain
```

Manual check — run gold twice yourself:

```bash
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g gold_aggregate.py
# run again — candle count should not double
docker compose run --rm spark \
    /opt/spark/bin/spark-submit --master "local[*]" counts.py
```

## Delivery semantics map (interview cheat sheet)

| Stage | Semantics | Mechanism |
|---|---|---|
| Coinbase → producer | at-most-once | live socket, can't replay |
| Producer → Kafka | at-least-once / idempotent producer | acks=all, idempotence PID |
| Kafka → bronze (stream) | exactly-once | checkpoint + Delta batch id |
| Bronze → silver (stream) | effectively-once | checkpoint + dedup watermark |
| Bronze → silver (batch) | idempotent | MERGE on trade key |
| Silver → gold (batch) | idempotent | MERGE on candle grain |

End-to-end is only as strong as the weakest link (the live feed). Inside the
lakehouse, re-runs are safe.
