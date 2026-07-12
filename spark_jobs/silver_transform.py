"""Day 11: bronze -> silver (parse, type, conform, deduplicate).

Bronze is the raw JSON string as it arrived. Silver is the *trustworthy* table:
each trade parsed into real typed columns, malformed rows dropped, and duplicates
removed on the natural key (product_id, trade_id). This is the first layer other
people/jobs are meant to actually query.

"Conformed" means: one canonical shape and semantics for a trade -- decimals for
money (never float), a real event-time timestamp, standardized column names --
regardless of the source's quirks. Everything downstream (gold aggregates, the
speed layer) can trust silver's schema and uniqueness.

Runs as a stream: readStream from the bronze Delta table, writeStream to silver.
Its own checkpoint (separate from bronze's) gives exactly-once + resume.

Run (let it stream, then Ctrl+C):
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] silver_transform.py
"""

from __future__ import annotations

from common import TRADE_SCHEMA, build_spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json

BRONZE_PATH = "s3a://bronze/trades"
SILVER_PATH = "s3a://silver/trades"
CHECKPOINT_PATH = "s3a://silver/_checkpoints/trades"


def to_silver(bronze: DataFrame) -> DataFrame:
    """Parse the raw JSON, keep only valid trades, conform + dedup."""
    parsed = bronze.select(
        from_json(col("value"), TRADE_SCHEMA).alias("t"),
        col("ingest_timestamp"),
    )

    # Drop malformed rows: unparseable JSON -> t is null; JSON missing the key
    # fields -> those fields are null. Requiring the identity + money + time
    # fields is enough to reject both the "hello kafka" and {"test":"trade"}
    # junk we planted on Day 3.
    valid = parsed.where(
        col("t.trade_id").isNotNull()
        & col("t.product_id").isNotNull()
        & col("t.price").isNotNull()
        & col("t.size").isNotNull()
        & col("t.time").isNotNull()
    )

    conformed = valid.select(
        col("t.product_id").alias("product_id"),
        col("t.trade_id").alias("trade_id"),
        col("t.side").alias("side"),
        col("t.price").alias("price"),
        col("t.size").alias("size"),
        col("t.sequence").alias("sequence"),
        col("t.maker_order_id").alias("maker_order_id"),
        col("t.taker_order_id").alias("taker_order_id"),
        col("t.time").alias("event_time"),
        col("ingest_timestamp"),
    ).withColumn("silver_timestamp", current_timestamp())

    # Deduplicate on the natural key. The watermark bounds how long we remember
    # seen keys (state), so this doesn't grow unboundedly: trades older than the
    # watermark are assumed no longer arriving and their keys are evicted.
    # dropDuplicatesWithinWatermark (Spark 3.5+) dedups by key within that window
    # without requiring event_time to be part of the key.
    return conformed.withWatermark("event_time", "1 hour").dropDuplicatesWithinWatermark(
        ["product_id", "trade_id"]
    )


def main() -> None:
    spark = build_spark("silver-transform")
    spark.sparkContext.setLogLevel("WARN")
    print(f"silver transform: {BRONZE_PATH} -> {SILVER_PATH}")

    # maxFilesPerTrigger bounds how many bronze files each micro-batch reads, so
    # the initial backlog is processed in chunks instead of one giant batch that
    # would blow the driver's heap (the Day-11 OOM lesson).
    bronze = spark.readStream.format("delta").option("maxFilesPerTrigger", "64").load(BRONZE_PATH)
    silver = to_silver(bronze)

    # No mergeSchema here -> Delta *enforces* silver's schema: a drifted/extra
    # column would make the write fail loudly rather than silently corrupt the
    # table. Schema *evolution* (opt-in mergeSchema) is exercised on Day 21.
    query = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )

    # Only print when the batch id changes: during a backlog, many batches run
    # between our 10s samples, and printing repeated idle ticks is misleading
    # (it looks like fewer rows were processed than actually were).
    last_batch = -1
    try:
        while query.isActive:
            query.awaitTermination(10)
            progress = query.lastProgress
            if progress is not None and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                print(
                    f"[progress] batch={progress['batchId']} "
                    f"inputRows={progress['numInputRows']} "
                    f"rows/s={progress.get('inputRowsPerSecond')}"
                )
    except KeyboardInterrupt:
        print("stopping stream...")
        query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
