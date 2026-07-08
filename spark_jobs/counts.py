"""Verification helper: batch-count the medallion tables.

A quick sanity check that layers line up:
  - bronze row count (raw, may include junk + duplicates)
  - silver row count and its distinct (product_id, trade_id) count
    (these two should be EQUAL -- proof the dedup worked)

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] counts.py
"""

from __future__ import annotations

from common import build_spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

BRONZE_PATH = "s3a://bronze/trades"
SILVER_PATH = "s3a://silver/trades"


def _count(spark: SparkSession, path: str) -> int:
    try:
        return spark.read.format("delta").load(path).count()
    except Exception as exc:
        print(f"  (could not read {path}: {exc})")
        return -1


def main() -> None:
    spark = build_spark("table-counts")
    spark.sparkContext.setLogLevel("WARN")

    bronze = _count(spark, BRONZE_PATH)
    print(f"bronze rows              : {bronze}")

    silver = _count(spark, SILVER_PATH)
    print(f"silver rows              : {silver}")

    if silver > 0:
        distinct = (
            spark.read.format("delta")
            .load(SILVER_PATH)
            .select(countDistinct("product_id", "trade_id"))
            .collect()[0][0]
        )
        print(f"silver distinct keys     : {distinct}")
        print(f"silver dedup clean?      : {distinct == silver}")

    spark.stop()


if __name__ == "__main__":
    main()
