"""Day 15: run data quality checks on silver and gold tables.

Batch validation job — run after the pipeline (or on a schedule once Airflow
exists). Logs pass/fail per check, quarantines any row-level violations found,
and records row counts for drift detection on the next run.

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] dq_validate.py
"""

from __future__ import annotations

import sys

from common import build_spark
from dq import (
    alert,
    check_gold,
    check_silver,
    load_prior_row_count,
    quarantine_gold_violations,
    quarantine_silver_violations,
    save_metrics,
)

SILVER_PATH = "s3a://silver/trades"
GOLD_PATH = "s3a://gold/ohlc"


def main() -> None:
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
    if not (ok_silver and ok_gold):
        sys.exit(1)


if __name__ == "__main__":
    main()
