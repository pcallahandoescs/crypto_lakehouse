"""Day 10: stream Kafka -> bronze Delta (append-only, exactly-once).

Reads `crypto.trades.raw` and lands each message *verbatim* in a bronze Delta
table, adding Kafka provenance (topic/partition/offset/timestamp) and an
ingestion timestamp. No parsing or typing -- that's silver (Day 11). Bronze is an
immutable, faithful copy of the source: our replay + audit foundation.

Exactly-once: Structured Streaming records the Kafka offsets for each micro-batch
in the checkpoint, and the Delta sink commits each batch atomically and
idempotently (keyed by batch id). So a crash-and-retry re-processes the same
offsets into the same Delta commit -- no drops (which pure at-most-once risks) and
no duplicates (which naive at-least-once risks).

Run (let it stream, then Ctrl+C):
    docker compose run --rm spark \
        /opt/spark/bin/spark-submit --master local[*] bronze_ingest.py
"""

from __future__ import annotations

import os

from common import build_spark
from pyspark.sql.functions import current_timestamp

BRONZE_PATH = "s3a://bronze/trades"
CHECKPOINT_PATH = "s3a://bronze/_checkpoints/trades"


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "crypto.trades.raw")

    spark = build_spark("bronze-ingest")
    spark.sparkContext.setLogLevel("WARN")
    print(f"bronze ingest: {bootstrap}/{topic} -> {BRONZE_PATH}")

    # Read the topic as a stream. startingOffsets=earliest only applies on the
    # first run (no checkpoint yet); afterwards the checkpoint decides where to
    # resume. failOnDataLoss=false tolerates retention-expired offsets in dev.
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Bronze = raw value as-is (kept as a string for inspectability) + Kafka
    # provenance + our ingestion time. Deliberately no JSON parsing here.
    bronze = raw.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "timestampType AS kafka_timestamp_type",
    ).withColumn("ingest_timestamp", current_timestamp())

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )

    # Print per-batch progress so the ingestion is visible; Ctrl+C to stop.
    try:
        while query.isActive:
            query.awaitTermination(10)
            progress = query.lastProgress
            if progress is not None:
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
