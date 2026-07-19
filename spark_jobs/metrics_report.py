"""Day 20: observability report — the operations view of the lakehouse.

Reads the run-metrics table written by ``observe.record_run`` and prints the
latest state per (job, layer): row volume, freshness, and DQ pass/fail. This is
the "is it healthy?" glance until the FastAPI ``/metrics`` endpoint (Day 22)
serves the same table over HTTP.

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] metrics_report.py
"""

from __future__ import annotations

from common import build_spark
from observe import RUN_METRICS_PATH
from pyspark.sql import Window
from pyspark.sql import functions as F


def _fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    hours = seconds / 3600
    if hours >= 1:
        return f"{hours:.1f}h"
    return f"{seconds / 60:.1f}m"


def main() -> None:
    spark = build_spark("metrics-report")
    spark.sparkContext.setLogLevel("WARN")

    try:
        runs = spark.read.format("delta").load(RUN_METRICS_PATH)
    except Exception:
        print(f"no observability data yet at {RUN_METRICS_PATH}")
        print("run the batch DAG / dq_validate first.")
        spark.stop()
        return

    # Latest row per (job, layer).
    latest_rank = Window.partitionBy("job", "layer").orderBy(F.col("ts").desc())
    latest = (
        runs.withColumn("_r", F.row_number().over(latest_rank))
        .where(F.col("_r") == 1)
        .drop("_r")
        .orderBy("job", "layer")
        .collect()
    )

    print("\n==== lakehouse observability report ====")
    print(f"source: {RUN_METRICS_PATH}\n")
    header = f"{'job':<16} {'layer':<8} {'rows':>10} {'dq':>9} {'fresh':>8}  last_run (UTC)"
    print(header)
    print("-" * len(header))
    for r in latest:
        dq = "-"
        if r["dq_passed"] is not None or r["dq_failed"] is not None:
            passed = r["dq_passed"] or 0
            failed = r["dq_failed"] or 0
            dq = f"{passed}/{passed + failed}"
        rows = "-" if r["rows"] is None else f"{r['rows']:,}"
        ts = r["ts"].strftime("%Y-%m-%d %H:%M") if r["ts"] else "-"
        print(
            f"{r['job']:<16} {r['layer']:<8} {rows:>10} {dq:>9} "
            f"{_fmt_age(r['freshness_seconds']):>8}  {ts}"
        )

    # Any failing DQ in the latest snapshot is the top-line health signal.
    failing = [r for r in latest if (r["dq_failed"] or 0) > 0]
    print()
    if failing:
        for r in failing:
            print(f"  DQ FAILING: {r['job']}/{r['layer']} — {r['dq_failed']} checks failed")
    else:
        print("  all latest DQ checks passing")

    spark.stop()


if __name__ == "__main__":
    main()
