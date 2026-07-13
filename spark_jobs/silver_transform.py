"""Day 11: bronze -> silver (parse, type, conform, deduplicate).

Bronze is the raw JSON string as it arrived. Silver is the *trustworthy* table:
each trade parsed into real typed columns, malformed rows quarantined (Day 15),
and duplicates removed on the natural key (product_id, trade_id). This is the
first layer other people/jobs are meant to actually query.

"Conformed" means: one canonical shape and semantics for a trade -- decimals for
money (never float), a real event-time timestamp, standardized column names --
regardless of the source's quirks. Everything downstream (gold aggregates, the
speed layer) can trust silver's schema and uniqueness.

Runs as a stream: readStream from the bronze Delta table, writeStream to silver.
Invalid parse/contract rows go to a **quarantine** table instead of being silently
dropped. Its own checkpoint (separate from bronze's) gives exactly-once + resume.

Run (let it stream, then Ctrl+C):
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] silver_transform.py
"""

from __future__ import annotations

from common import TRADE_SCHEMA, build_spark
from dq import SILVER_QUARANTINE_PATH
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, current_timestamp, from_json, lit

BRONZE_PATH = "s3a://bronze/trades"
SILVER_PATH = "s3a://silver/trades"
CHECKPOINT_PATH = "s3a://silver/_checkpoints/trades"
QUARANTINE_CHECKPOINT_PATH = "s3a://silver/_checkpoints/quarantine"


def _is_valid_trade() -> Column:
    return (
        col("t.trade_id").isNotNull()
        & col("t.product_id").isNotNull()
        & col("t.price").isNotNull()
        & col("t.size").isNotNull()
        & col("t.time").isNotNull()
    )


def parse_bronze(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        col("value"),
        col("ingest_timestamp"),
        from_json(col("value"), TRADE_SCHEMA).alias("t"),
    )


def to_silver(parsed: DataFrame) -> DataFrame:
    """Parse the raw JSON, keep only valid trades, conform + dedup."""
    valid = parsed.where(_is_valid_trade())

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

    return conformed.withWatermark("event_time", "1 hour").dropDuplicatesWithinWatermark(
        ["product_id", "trade_id"]
    )


def to_quarantine(parsed: DataFrame) -> DataFrame:
    """Rows that fail parse/contract checks — auditable, not silently dropped."""
    return parsed.where(~_is_valid_trade()).select(
        col("value"),
        col("ingest_timestamp"),
        lit("parse_or_contract_failure").alias("reason"),
        current_timestamp().alias("quarantined_at"),
    )


def main() -> None:
    spark = build_spark("silver-transform")
    spark.sparkContext.setLogLevel("WARN")
    print(f"silver transform: {BRONZE_PATH} -> {SILVER_PATH}")
    print(f"  quarantine: {SILVER_QUARANTINE_PATH}")

    bronze = spark.readStream.format("delta").option("maxFilesPerTrigger", "64").load(BRONZE_PATH)
    parsed = parse_bronze(bronze)
    silver = to_silver(parsed)
    quarantine = to_quarantine(parsed)

    silver_query = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(SILVER_PATH)
    )

    quarantine_query = (
        quarantine.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", QUARANTINE_CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(SILVER_QUARANTINE_PATH)
    )

    last_batch = -1
    try:
        while silver_query.isActive:
            silver_query.awaitTermination(10)
            progress = silver_query.lastProgress
            if progress is not None and progress["batchId"] != last_batch:
                last_batch = progress["batchId"]
                print(
                    f"[progress] batch={progress['batchId']} "
                    f"inputRows={progress['numInputRows']} "
                    f"rows/s={progress.get('inputRowsPerSecond')}"
                )
    except KeyboardInterrupt:
        print("stopping stream...")
        silver_query.stop()
        quarantine_query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
