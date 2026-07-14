"""Day 16: batch bronze -> silver via MERGE upsert (idempotent).

The streaming silver job (silver_transform.py) is for continuous ingestion.
This batch path is for **reprocessing**: read bronze, conform valid trades, and
MERGE into silver on (product_id, trade_id). Re-running after a logic fix or
during a backfill (Day 17) updates existing keys in place — no duplicates.

Invalid rows append to the quarantine table (same as the stream).

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g silver_batch.py
"""

from __future__ import annotations

from common import build_spark
from dq import SILVER_QUARANTINE_PATH
from idempotent import merge_condition, merge_upsert
from pyspark.sql import DataFrame, SparkSession
from silver_transform import (
    BRONZE_PATH,
    SILVER_PATH,
    conform_trades,
    parse_bronze,
    to_quarantine,
)

SILVER_MERGE_KEYS = ("product_id", "trade_id")


def write_silver(spark: SparkSession, silver: DataFrame) -> None:
    merge_upsert(
        spark,
        SILVER_PATH,
        silver,
        merge_condition(SILVER_MERGE_KEYS),
    )


def main() -> None:
    spark = build_spark("silver-batch")
    spark.sparkContext.setLogLevel("WARN")
    print(f"silver batch upsert: {BRONZE_PATH} -> {SILVER_PATH}")

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    parsed = parse_bronze(bronze)
    silver = conform_trades(parsed)
    write_silver(spark, silver)

    quarantine = to_quarantine(parsed)
    q_count = quarantine.count()
    if q_count > 0:
        quarantine.write.format("delta").mode("append").save(SILVER_QUARANTINE_PATH)
        print(f"  quarantined {q_count} invalid rows")

    total = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"silver rows after merge: {total}")

    spark.stop()


if __name__ == "__main__":
    main()
