"""The Lambda speed layer -- real-time windowed metrics.

The batch path (bronze -> silver -> gold) is correct but slow: it lands, cleans,
and recomputes on a schedule. The **speed layer** is its low-latency complement.
It reads the *same source* (Kafka) directly and computes rolling metrics live, so
a dashboard can show "what's happening right now" without waiting for the batch.

Speed vs. batch (the Lambda trade-off):
  - **Speed:** seconds of latency, reads Kafka directly, approximate/provisional
    (a late trade can nudge a recent window). Optimized for freshness.
  - **Batch:** minutes+ latency, goes through the medallion layers, exact and
    fully reprocessable. Optimized for correctness.
Together they give both fresh *and* eventually-correct views of the same data.

Metrics per product over a **sliding** 1-minute window (recomputed every 15s):
rolling VWAP, trade count, volume, and short-window price volatility (stddev).
A **watermark** bounds state and defines how late a trade may arrive and still be
counted.

Run (needs the producer live -- `docker compose start producer`):
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] --driver-memory 2g speed_metrics.py
"""

from __future__ import annotations

import os

from common import TRADE_SCHEMA, build_spark
from observe import JobLogger, stream_progress_fields
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

REALTIME_PATH = "s3a://gold/realtime_metrics"
CHECKPOINT_PATH = "s3a://gold/_checkpoints/realtime_metrics"

log = JobLogger("speed-metrics")

WINDOW = "1 minute"  # length of each rolling window
SLIDE = "15 seconds"  # how often a new window starts (sliding => overlapping)
WATERMARK = "1 minute"  # how late a trade may arrive and still be counted


def to_metrics(trades: DataFrame) -> DataFrame:
    """Sliding-window rolling metrics per product, with a watermark for late data."""
    return (
        trades.withWatermark("event_time", WATERMARK)
        .groupBy(
            F.window("event_time", WINDOW, SLIDE).alias("w"),
            "product_id",
        )
        .agg(
            F.count(F.lit(1)).alias("trade_count"),
            F.sum("size").alias("volume"),
            # VWAP + volatility in double (see gold_aggregate.py for the decimal
            # rationale). Volatility = stddev of trade price in the window; it's
            # null for a single-trade window, which is correct.
            F.sum(F.col("price").cast("double") * F.col("size").cast("double")).alias("_quote"),
            F.sum(F.col("size").cast("double")).alias("_base"),
            F.stddev(F.col("price").cast("double")).alias("price_volatility"),
        )
        .select(
            "product_id",
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "trade_count",
            "volume",
            (F.col("_quote") / F.col("_base")).alias("vwap"),
            "price_volatility",
        )
    )


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "crypto.trades.raw")

    spark = build_spark("speed-metrics")
    spark.sparkContext.setLogLevel("WARN")
    log.event("started", source=f"{bootstrap}/{topic}", sink=REALTIME_PATH)

    # startingOffsets=latest: the speed layer cares about NOW, not history, so it
    # only processes trades that arrive after it starts (the batch path owns the
    # full historical record).
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    trades = (
        raw.select(F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"))
        .select(
            F.col("t.product_id").alias("product_id"),
            F.col("t.price").alias("price"),
            F.col("t.size").alias("size"),
            F.col("t.time").alias("event_time"),
        )
        .where(
            F.col("product_id").isNotNull()
            & F.col("price").isNotNull()
            & F.col("size").isNotNull()
            & F.col("event_time").isNotNull()
        )
    )

    metrics = to_metrics(trades)

    # Append mode + watermark: a window is written once the watermark passes its
    # end (i.e. it's final and no more late data will change it). That's the
    # durable, exactly-once-friendly choice for a Delta sink; the cost is that a
    # window appears ~watermark after it closes. (A lower-latency variant would
    # use foreachBatch + MERGE to upsert in-progress windows.)
    query = (
        metrics.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="15 seconds")
        .start(REALTIME_PATH)
    )

    last_batch = -1
    try:
        while query.isActive:
            query.awaitTermination(15)
            progress = query.lastProgress
            if progress is not None and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                log.event("batch", **stream_progress_fields(progress))
    except KeyboardInterrupt:
        log.event("stopping")
        query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
