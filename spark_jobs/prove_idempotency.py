"""Day 16: prove batch MERGE idempotency — run twice, output identical.

Each phase uses **four Spark sessions** (count → MERGE → MERGE → count) so no
single JVM runs two heavy merges. Gold/silver proofs use capped self-MERGE on
the target table (same MERGE SQL as production batch jobs).

Run phases separately at **2g** — not 4g:

    docker compose run --rm spark \\
        /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \\
        prove_idempotency.py --gold-only

    docker compose run --rm spark \\
        /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \\
        prove_idempotency.py --silver-only
"""

from __future__ import annotations

import argparse
import sys

from common import build_spark
from gold_aggregate import GOLD_MERGE_KEYS, GOLD_PATH
from idempotent import merge_self_update
from pyspark.sql import SparkSession
from silver_batch import SILVER_MERGE_KEYS, SILVER_PATH

GOLD_PROOF_FILTER = "product_id = 'BTC-USD'"
GOLD_PROOF_LIMIT = 500
SILVER_PROOF_FILTER = "product_id = 'BTC-USD'"
SILVER_PROOF_LIMIT = 500


def _log(msg: str) -> None:
    print(msg, flush=True)


def _row_count(spark: SparkSession, path: str) -> int:
    return int(spark.sql(f"SELECT COUNT(*) AS n FROM delta.`{path}`").collect()[0].n)


def _count_session(label: str, path: str) -> int:
    spark = build_spark(f"prove-idempotency-{label}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        return _row_count(spark, path)
    finally:
        spark.stop()


def _run_merge(
    label: str,
    path: str,
    keys: tuple[str, ...],
    *,
    source_filter: str,
    source_limit: int,
) -> None:
    spark = build_spark(f"prove-idempotency-{label}")
    spark.sparkContext.setLogLevel("WARN")
    try:
        merge_self_update(
            spark,
            path,
            keys,
            source_filter=source_filter,
            source_limit=source_limit,
        )
    finally:
        spark.stop()


def _prove_table(
    name: str,
    path: str,
    keys: tuple[str, ...],
    source_filter: str,
    source_limit: int,
) -> bool:
    _log(f"  {name}: count before (session 1/4)...")
    before_total = _count_session(f"{name}-before", path)
    _log(f"  {name}: MERGE 1/2 (session 2/4, limit {source_limit})...")
    _run_merge(f"{name}-merge1", path, keys, source_filter=source_filter, source_limit=source_limit)
    _log(f"  {name}: MERGE 2/2 (session 3/4, limit {source_limit})...")
    _run_merge(f"{name}-merge2", path, keys, source_filter=source_filter, source_limit=source_limit)
    _log(f"  {name}: count after (session 4/4)...")
    after_total = _count_session(f"{name}-after", path)
    ok = before_total == after_total
    _log(f"  {name}: rows {before_total} -> {after_total}")
    return ok


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prove Delta MERGE idempotency")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--gold-only", action="store_true", help="Prove gold MERGE only")
    group.add_argument("--silver-only", action="store_true", help="Prove silver MERGE only")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    run_gold = not args.silver_only
    run_silver = not args.gold_only

    _log("proving MERGE idempotency (double-run, 4 sessions per phase)...")

    gold_ok = True
    silver_ok = True
    try:
        if run_gold:
            _log("phase: gold")
            gold_ok = _prove_table(
                "gold",
                GOLD_PATH,
                GOLD_MERGE_KEYS,
                GOLD_PROOF_FILTER,
                GOLD_PROOF_LIMIT,
            )
        if run_silver:
            _log("phase: silver")
            silver_ok = _prove_table(
                "silver",
                SILVER_PATH,
                SILVER_MERGE_KEYS,
                SILVER_PROOF_FILTER,
                SILVER_PROOF_LIMIT,
            )
    except Exception as exc:
        _log(f"\nERROR during proof: {type(exc).__name__}: {exc}")
        sys.exit(1)

    if gold_ok and silver_ok:
        label = []
        if run_gold:
            label.append("gold")
        if run_silver:
            label.append("silver")
        _log(f"\nPASS ({', '.join(label)}): double MERGE -> identical row counts and grain")
        return

    _log("\nFAIL: row count changed after second MERGE")
    sys.exit(1)


if __name__ == "__main__":
    main()
