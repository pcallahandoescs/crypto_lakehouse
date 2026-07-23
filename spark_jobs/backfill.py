"""Backfill bronze -> silver -> gold for an event-time date range.

Reprocesses history on demand by reading **immutable bronze** (the replay log),
conforming trades, and MERGE-upserting into silver and gold. Safe to
re-run the same range — matched keys update in place, no duplicates.

Event-time bounds: [--start, --end) as UTC dates (end exclusive).
Silver and gold run in **separate Spark sessions** to stay within Docker memory.

Run:
    docker compose run --rm spark \\
        /opt/spark/bin/spark-submit --master "local[*]" backfill.py --show-range

    docker compose run --rm spark \\
        /opt/spark/bin/spark-submit --master "local[*]" --driver-memory 2g \\
        backfill.py --start 2026-07-05 --end 2026-07-06
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone

from common import build_spark
from dq import SILVER_QUARANTINE_PATH
from gold_aggregate import INTERVAL, to_gold, write_gold
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from silver_batch import SILVER_PATH, write_silver
from silver_transform import BRONZE_PATH, conform_trades, parse_bronze, to_quarantine

# Coarse bronze pre-filter on ingest time — shrinks parse volume; event-time
# filter still applied after JSON parse (ingest ≈ event time for live feed).
_INGEST_BUFFER = timedelta(days=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r} — use YYYY-MM-DD") from exc


def _utc_bounds(start: date, end: date) -> tuple[datetime, datetime]:
    if end <= start:
        raise ValueError(f"--end must be after --start (got {start} .. {end})")
    start_ts = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)
    end_ts = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    return start_ts, end_ts


def _interval_padding(interval: str) -> timedelta:
    if interval.endswith(" minute") or interval.endswith(" minutes"):
        return timedelta(minutes=int(interval.split()[0]))
    if interval.endswith(" hour") or interval.endswith(" hours"):
        return timedelta(hours=int(interval.split()[0]))
    return timedelta(minutes=1)


def _prefilter_bronze_ingest(bronze: DataFrame, start: datetime, end: datetime) -> DataFrame:
    return bronze.where(
        (F.col("ingest_timestamp") >= F.lit(start - _INGEST_BUFFER))
        & (F.col("ingest_timestamp") < F.lit(end + _INGEST_BUFFER))
    )


def _filter_parsed_event_time(parsed: DataFrame, start: datetime, end: datetime) -> DataFrame:
    return parsed.where((F.col("t.time") >= F.lit(start)) & (F.col("t.time") < F.lit(end)))


def _filter_silver_event_time(silver: DataFrame, start: datetime, end: datetime) -> DataFrame:
    return silver.where((F.col("event_time") >= F.lit(start)) & (F.col("event_time") < F.lit(end)))


def backfill_silver(spark: SparkSession, start: datetime, end: datetime) -> None:
    """Bronze -> silver MERGE for trades with event_time in [start, end)."""
    bronze = _prefilter_bronze_ingest(spark.read.format("delta").load(BRONZE_PATH), start, end)
    parsed = _filter_parsed_event_time(parse_bronze(bronze), start, end)
    write_silver(spark, conform_trades(parsed))

    quarantine = to_quarantine(parsed)
    if quarantine.take(1):
        quarantine.write.format("delta").mode("append").save(SILVER_QUARANTINE_PATH)
        print("  quarantined invalid rows (see silver quarantine table)")


def backfill_gold(spark: SparkSession, start: datetime, end: datetime) -> None:
    """Silver -> gold MERGE for candles with interval_start in [start, end)."""
    pad = _interval_padding(INTERVAL)
    silver = spark.read.format("delta").load(SILVER_PATH)
    silver = _filter_silver_event_time(silver, start - pad, end + pad)

    gold = (
        to_gold(silver, INTERVAL)
        .withColumn("date", F.to_date("interval_start"))
        .where((F.col("interval_start") >= F.lit(start)) & (F.col("interval_start") < F.lit(end)))
    )
    write_gold(spark, gold)


def show_event_time_range(spark: SparkSession) -> None:
    """Print min/max trade event_time in bronze for picking backfill dates."""
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    parsed = parse_bronze(bronze)
    row = parsed.agg(
        F.min("t.time").alias("min_time"),
        F.max("t.time").alias("max_time"),
        F.count(F.lit(1)).alias("bronze_rows"),
    ).collect()[0]
    print(f"bronze rows (all):     {row.bronze_rows}")
    print(f"event_time min (UTC):  {row.min_time}")
    print(f"event_time max (UTC):  {row.max_time}")
    if row.min_time and row.max_time:
        span_start = row.min_time.date()
        span_end = row.max_time.date() + timedelta(days=1)
        print("\nexample backfill for full span:")
        print(f"  backfill.py --start {span_start} --end {span_end}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill silver and gold for an event-time date range"
    )
    parser.add_argument("--start", type=_parse_date, help="Start date (inclusive, UTC)")
    parser.add_argument("--end", type=_parse_date, help="End date (exclusive, UTC)")
    parser.add_argument(
        "--skip-silver", action="store_true", help="Only rebuild gold from existing silver"
    )
    parser.add_argument("--skip-gold", action="store_true", help="Only rebuild silver from bronze")
    parser.add_argument(
        "--show-range",
        action="store_true",
        help="Print min/max event_time in bronze and exit (pick --start/--end)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.show_range:
        spark = build_spark("backfill-range")
        spark.sparkContext.setLogLevel("WARN")
        try:
            show_event_time_range(spark)
        finally:
            spark.stop()
        return

    if args.start is None or args.end is None:
        print("ERROR: --start and --end are required (or use --show-range)")
        sys.exit(1)
    if args.skip_silver and args.skip_gold:
        print("ERROR: cannot skip both silver and gold")
        sys.exit(1)

    try:
        start, end = _utc_bounds(args.start, args.end)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"backfill event_time [{start.isoformat()}, {end.isoformat()})")

    try:
        if not args.skip_silver:
            print("  bronze -> silver (MERGE, session 1)...")
            spark = build_spark("backfill-silver")
            spark.sparkContext.setLogLevel("WARN")
            try:
                backfill_silver(spark, start, end)
            finally:
                spark.stop()
            print("  silver: merge complete")

        if not args.skip_gold:
            print(f"  silver -> gold (MERGE, session 2, interval={INTERVAL})...")
            spark = build_spark("backfill-gold")
            spark.sparkContext.setLogLevel("WARN")
            try:
                backfill_gold(spark, start, end)
            finally:
                spark.stop()
            print("  gold: merge complete")
    except Exception as exc:
        print(f"\nERROR: {type(exc).__name__}: {exc}")
        sys.exit(1)

    print("\nbackfill complete (idempotent — safe to re-run same range)")
    print(
        "verify: docker compose run --rm spark "
        '/opt/spark/bin/spark-submit --master "local[*]" counts.py'
    )


if __name__ == "__main__":
    main()
