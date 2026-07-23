"""Silver -> gold (OHLC candles + VWAP + volume).

Gold is the analytical product: the thing consumers (a dashboard, an analyst, a
model) actually query. We roll individual trades up into **candles** -- one row
per (product, time interval) -- with the classic market-data metrics:

  - OHLC: open / high / low / close price within the interval
  - volume: total base-asset quantity traded (sum of size)
  - vwap: volume-weighted average price = sum(price*size) / sum(size)
  - trade_count: number of trades in the interval

Grain (the most important thing to be able to state): **one row per product per
interval**. The interval is a tumbling (non-overlapping) window, 1 minute by
default.

Batch job: read all of silver, recompute candles, **MERGE upsert** into gold on
(product_id, interval_start). Safe to re-run — matched rows update in place,
no duplicates.

Run:
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g gold_aggregate.py
"""

from __future__ import annotations

import os
import time

from common import build_spark
from dq import alert, check_gold, load_prior_row_count, save_metrics
from idempotent import merge_condition, merge_upsert
from observe import JobLogger, record_run
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = JobLogger("gold-aggregate")

SILVER_PATH = "s3a://silver/trades"
GOLD_PATH = "s3a://gold/ohlc"
GOLD_MERGE_KEYS = ("product_id", "interval_start")
# Tumbling-window size. Same job, different value, gives 5-minute candles, etc.
INTERVAL = os.getenv("GOLD_INTERVAL", "1 minute")


def load_silver(spark: SparkSession) -> DataFrame:
    """Read silver, optionally scoped by GOLD_PRODUCT_ID / GOLD_SINCE_DAYS env vars."""
    silver = spark.read.format("delta").load(SILVER_PATH)
    product_id = os.getenv("GOLD_PRODUCT_ID")
    since_days = os.getenv("GOLD_SINCE_DAYS")
    if since_days:
        silver = silver.where(
            F.to_date("event_time") >= F.date_sub(F.current_date(), int(since_days))
        )
    if product_id:
        silver = silver.where(F.col("product_id") == product_id)
    return silver


def to_gold(silver: DataFrame, interval: str) -> DataFrame:
    """Aggregate trades into OHLC/VWAP/volume candles, one row per product+interval."""
    # order-of-trade within the interval, for a deterministic open/close.
    order = F.struct("event_time", "sequence")

    candles = silver.groupBy(
        F.window("event_time", interval).alias("w"),
        "product_id",
    ).agg(
        # open/close need trade order; min_by/max_by pick the price at the
        # earliest/latest (event_time, sequence) deterministically.
        F.min_by("price", order).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order).alias("close"),
        F.sum("size").alias("volume"),
        F.count(F.lit(1)).alias("trade_count"),
        # VWAP is computed in double: multiplying two DECIMAL(38,18) values
        # overflows the precision budget, and a volume-weighted *average* is a
        # derived ratio where sub-cent float error is irrelevant. OHLC prices and
        # volume stay exact decimals; only this analytical mean is float.
        F.sum(F.col("price").cast("double") * F.col("size").cast("double")).alias("_quote"),
        F.sum(F.col("size").cast("double")).alias("_base"),
    )

    return candles.select(
        "product_id",
        F.col("w.start").alias("interval_start"),
        F.col("w.end").alias("interval_end"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        (F.col("_quote") / F.col("_base")).alias("vwap"),
        "trade_count",
    )


def write_gold(spark: SparkSession, gold: DataFrame) -> None:
    """Idempotent upsert on the grain key; partition by date for pruning."""
    merge_upsert(
        spark,
        GOLD_PATH,
        gold,
        merge_condition(GOLD_MERGE_KEYS),
        partition_by=["date"],
    )


def main() -> None:
    spark = build_spark("gold-aggregate")
    spark.sparkContext.setLogLevel("WARN")
    minimal = os.getenv("BATCH_MINIMAL", "").lower() in ("1", "true", "yes")
    product_id = os.getenv("GOLD_PRODUCT_ID", "")
    since_days = os.getenv("GOLD_SINCE_DAYS", "")
    start = time.monotonic()
    log.event(
        "started",
        source=SILVER_PATH,
        sink=GOLD_PATH,
        interval=INTERVAL,
        product_id=product_id or None,
        since_days=since_days or None,
        minimal=minimal,
    )

    silver = load_silver(spark)
    gold = to_gold(silver, INTERVAL).withColumn("date", F.to_date("interval_start"))
    write_gold(spark, gold)

    if minimal:
        # Minimal mode is memory-tight (Airflow / Docker Desktop). Emit a cheap
        # structured log only — no extra Delta write here (that would risk OOM
        # right after the MERGE). Volume/quality metrics are recorded by the
        # downstream dq_validate task instead.
        log.event("merge_complete", duration_seconds=round(time.monotonic() - start, 2))
        spark.stop()
        return

    total = _count(spark, GOLD_PATH)
    log.event("candles_written", rows=total, duration_seconds=round(time.monotonic() - start, 2))
    record_run(
        spark,
        job="gold-aggregate",
        layer="gold",
        event="candles_written",
        rows=total,
        duration_seconds=round(time.monotonic() - start, 2),
    )

    prior = load_prior_row_count(spark, "gold/ohlc")
    gold_df = spark.read.format("delta").load(GOLD_PATH)
    alert(check_gold(gold_df, total, prior), layer="gold")
    save_metrics(spark, "gold/ohlc", total)

    print("\nmost recent candles:")
    (
        spark.read.format("delta")
        .load(GOLD_PATH)
        .orderBy(F.col("interval_start").desc(), "product_id")
        .show(10, truncate=False)
    )

    spark.stop()


def _count(spark: SparkSession, path: str) -> int:
    return spark.read.format("delta").load(path).count()


if __name__ == "__main__":
    main()
