"""Day 15: run data quality checks on silver and gold tables.

Batch validation job — run after the pipeline (or on a schedule once Airflow
exists). Logs pass/fail per check, quarantines any row-level violations found,
and records row counts for drift detection on the next run.

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] dq_validate.py

Airflow (BATCH_MINIMAL=1): one SQL scan per layer, separate Spark sessions,
no quarantine writes — avoids OOM after gold aggregation tasks.
"""

from __future__ import annotations

import os
import sys

from common import build_spark
from dq import (
    alert,
    check_gold,
    check_gold_minimal,
    check_silver,
    check_silver_minimal,
    load_prior_row_count,
    quarantine_gold_violations,
    quarantine_silver_violations,
    save_metrics,
)

SILVER_PATH = "s3a://silver/trades"
GOLD_PATH = "s3a://gold/ohlc"


def _validate_silver_minimal() -> bool:
    spark = build_spark("dq-silver")
    spark.sparkContext.setLogLevel("WARN")
    prior = load_prior_row_count(spark, "silver/trades")
    results, row_count = check_silver_minimal(spark, SILVER_PATH, prior, skip_freshness=True)
    ok = alert(results, layer="silver")
    save_metrics(spark, "silver/trades", row_count)
    spark.stop()
    return ok


def _validate_gold_minimal() -> bool:
    spark = build_spark("dq-gold")
    spark.sparkContext.setLogLevel("WARN")
    prior = load_prior_row_count(spark, "gold/ohlc")
    results, row_count = check_gold_minimal(spark, GOLD_PATH, prior)
    ok = alert(results, layer="gold")
    save_metrics(spark, "gold/ohlc", row_count)
    spark.stop()
    return ok


def _validate_full() -> bool:
    spark = build_spark("dq-validate")
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(SILVER_PATH)
    silver_count = silver.count()
    prior_silver = load_prior_row_count(spark, "silver/trades")
    ok_silver = alert(
        check_silver(silver, silver_count, prior_silver),
        layer="silver",
    )
    quarantine_silver_violations(spark, SILVER_PATH)
    save_metrics(spark, "silver/trades", silver_count)

    gold = spark.read.format("delta").load(GOLD_PATH)
    gold_count = gold.count()
    prior_gold = load_prior_row_count(spark, "gold/ohlc")
    ok_gold = alert(
        check_gold(gold, gold_count, prior_gold),
        layer="gold",
    )
    quarantine_gold_violations(spark, GOLD_PATH)
    save_metrics(spark, "gold/ohlc", gold_count)

    spark.stop()
    return ok_silver and ok_gold


def main() -> None:
    minimal = os.getenv("BATCH_MINIMAL", "").lower() in ("1", "true", "yes")
    layer = os.getenv("DQ_LAYER", "all").lower()

    if minimal:
        ok = _validate_minimal(layer)
    else:
        if layer != "all":
            raise SystemExit("DQ_LAYER requires BATCH_MINIMAL=1 (use full validate locally)")
        ok = _validate_full()
    if not ok:
        sys.exit(1)


def _validate_minimal(layer: str) -> bool:
    run_silver = layer in ("all", "silver")
    run_gold = layer in ("all", "gold")
    if not run_silver and not run_gold:
        raise SystemExit(f"unknown DQ_LAYER={layer!r} (use all, silver, or gold)")

    ok_silver = _validate_silver_minimal() if run_silver else True
    ok_gold = _validate_gold_minimal() if run_gold else True
    return ok_silver and ok_gold


if __name__ == "__main__":
    main()
