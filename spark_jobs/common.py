"""Shared Spark setup for all jobs: a SparkSession wired for Delta + MinIO (S3A).

Kept in one place so every job (smoke test, bronze, silver, ...) uses the exact
same Delta extensions and S3A settings. Imported by sibling job scripts, which
works because spark-submit puts the job's directory on sys.path.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Spark-side mirror of the Pydantic `Trade` contract (consumer/schema.py). Used
# by every job that parses the raw Coinbase JSON: silver (from bronze) and the
# speed layer (from Kafka). from_json casts JSON values into these types; bad or
# missing values become null and get filtered downstream.
#   - price/size: DECIMAL, never DOUBLE -- exact money, no float drift.
#   - time: real TIMESTAMP (event time), parsed from the ISO-8601 string.
TRADE_SCHEMA = StructType(
    [
        StructField("type", StringType()),
        StructField("trade_id", LongType()),
        StructField("maker_order_id", StringType()),
        StructField("taker_order_id", StringType()),
        StructField("side", StringType()),
        StructField("size", DecimalType(38, 18)),
        StructField("price", DecimalType(38, 18)),
        StructField("product_id", StringType()),
        StructField("sequence", LongType()),
        StructField("time", TimestampType()),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    """A SparkSession configured for Delta Lake tables on MinIO over S3A."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName(app_name)
        # Single-node right-sizing: the default 200 shuffle partitions is a
        # cluster default. On one laptop JVM it's pure overhead -- and for a
        # *stateful* streaming query it means 200 state-store instances, which
        # OOMs a small driver. 8 is plenty for local dev.
        .config("spark.sql.shuffle.partitions", "8")
        # Delta SQL support (format("delta"), time travel, MERGE, ...).
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # S3A -> MinIO. path-style + ssl off are the MinIO-specific bits.
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
