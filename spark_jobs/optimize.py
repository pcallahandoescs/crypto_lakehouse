"""Measure the data-layout toolkit -- compaction, Z-order, VACUUM.

Makes the "small files problem" and data skipping concrete by reporting
before/after numbers on a Delta table:

  1. DESCRIBE DETAIL  -> numFiles + sizeInBytes (the small-files signal)
  2. partition layout (if the table is partitioned)
  3. a timed filtered query (rows returned + wall time)
  4. OPTIMIZE [ZORDER BY ...]  -> bin-pack small files (+ co-locate for skipping)
  5. DESCRIBE DETAIL + the timed query again -> the impact
  6. VACUUM ... DRY RUN -> what compaction left behind (and retention/time-travel)

Usage:
    # compaction on bronze (many small streaming files -> few):
    spark-submit ... optimize.py s3a://bronze/trades

    # compaction + Z-order on gold by product_id:
    spark-submit ... optimize.py s3a://gold/ohlc product_id
"""

from __future__ import annotations

import sys
import time

from common import build_spark
from pyspark.sql import SparkSession


def _columns(spark: SparkSession, path: str) -> set[str]:
    return {f.name for f in spark.read.format("delta").load(path).schema.fields}


def sample_filter(spark: SparkSession, path: str) -> str:
    """Pick a representative predicate based on the table's schema."""
    cols = _columns(spark, path)
    if "product_id" in cols:
        return "product_id = 'BTC-USD'"
    if "key" in cols:
        # bronze stores the Kafka key (product_id) as a string column.
        return "key = 'BTC-USD'"
    return "1=1"


def detail(spark: SparkSession, path: str) -> None:
    row = (
        spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
        .select(
            "numFiles",
            "sizeInBytes",
            "partitionColumns",
        )
        .collect()[0]
    )
    mb = (row["sizeInBytes"] or 0) / 1_048_576
    parts = row["partitionColumns"] or []
    print(f"  numFiles={row['numFiles']}  size={mb:.2f} MiB  partitions={parts}")


def list_partitions(spark: SparkSession, path: str) -> None:
    """List distinct partition values.

    Delta tables in our stack don't support SHOW PARTITIONS, so read distinct
    values from the partition column(s) instead.
    """
    parts = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]["partitionColumns"]
    if not parts:
        return
    print("  partition values:")
    (
        spark.read.format("delta")
        .load(path)
        .select(parts)
        .distinct()
        .orderBy(parts)
        .show(truncate=False)
    )


def timed_query(spark: SparkSession, path: str, predicate: str) -> None:
    start = time.perf_counter()
    n = spark.read.format("delta").load(path).where(predicate).count()
    elapsed = time.perf_counter() - start
    print(f"  query [{predicate}] -> {n} rows in {elapsed:.2f}s")


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: optimize.py <table_path> [zorder_col[,col2,...]]")
        sys.exit(1)
    path = sys.argv[1]
    zorder = sys.argv[2] if len(sys.argv) > 2 else None

    spark = build_spark("optimize")
    spark.sparkContext.setLogLevel("WARN")
    print(f"optimizing delta.`{path}`" + (f" ZORDER BY ({zorder})" if zorder else ""))

    predicate = sample_filter(spark, path)

    print("\n-- before --")
    detail(spark, path)
    list_partitions(spark, path)
    timed_query(spark, path, predicate)

    sql = f"OPTIMIZE delta.`{path}`"
    if zorder:
        sql += f" ZORDER BY ({zorder})"
    print(f"\nrunning: {sql}")
    result = spark.sql(sql).collect()
    if result:
        metrics = result[0]["metrics"]
        print(f"  files added={metrics['numFilesAdded']} removed={metrics['numFilesRemoved']}")
        if metrics.get("zOrderStats"):
            print(f"  zOrderStats={metrics['zOrderStats']}")

    print("\n-- after --")
    detail(spark, path)
    list_partitions(spark, path)
    timed_query(spark, path, predicate)

    # DRY RUN: list files VACUUM *would* delete at the default 7-day retention,
    # without deleting. Those are files unreferenced by the log after OPTIMIZE
    # rewrote them -- but kept for 7 days so time travel still works.
    print("\n-- vacuum dry run (retain 168h) --")
    dry = spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS DRY RUN")
    print(f"  files eligible for deletion after retention: {dry.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
