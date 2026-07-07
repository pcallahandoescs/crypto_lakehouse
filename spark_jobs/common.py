"""Shared Spark setup for all jobs: a SparkSession wired for Delta + MinIO (S3A).

Kept in one place so every job (smoke test, bronze, silver, ...) uses the exact
same Delta extensions and S3A settings. Imported by sibling job scripts, which
works because spark-submit puts the job's directory on sys.path.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_name: str) -> SparkSession:
    """A SparkSession configured for Delta Lake tables on MinIO over S3A."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName(app_name)
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
