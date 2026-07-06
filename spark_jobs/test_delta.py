"""Day 9 smoke test: prove Spark + Delta + MinIO (S3A) work together.

Writes a small Delta table to MinIO, appends a second commit, reads it back,
demonstrates time travel, and prints the _delta_log contents -- the transaction
log that makes Delta ACID on plain object storage.

Run:
    docker compose run --rm spark
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

TABLE_PATH = "s3a://bronze/_smoke/test_delta"
SCHEMA = "id INT, product_id STRING, price DOUBLE"


def build_spark() -> SparkSession:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    return (
        SparkSession.builder.appName("delta-minio-smoke")
        # Delta SQL support (enables format("delta"), time travel, etc.).
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


def list_delta_log(spark: SparkSession, table_path: str) -> None:
    """Print files in the table's _delta_log via the Hadoop FileSystem API."""
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    log_path = jvm.org.apache.hadoop.fs.Path(f"{table_path}/_delta_log")
    fs = log_path.getFileSystem(hadoop_conf)
    print(f"\n_delta_log contents at {table_path}/_delta_log:")
    for status in fs.listStatus(log_path):
        print(f"  {status.getPath().getName()}")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"Spark {spark.version} started; target = {TABLE_PATH}")

    # Commit 0: overwrite with two rows.
    df0 = spark.createDataFrame(
        [(1, "BTC-USD", 63000.0), (2, "ETH-USD", 1800.0)],
        schema=SCHEMA,
    )
    df0.write.format("delta").mode("overwrite").save(TABLE_PATH)
    print("commit 0 written (overwrite, 2 rows)")

    # Commit 1: append one more row.
    df1 = spark.createDataFrame([(3, "BTC-USD", 63010.5)], schema=SCHEMA)
    df1.write.format("delta").mode("append").save(TABLE_PATH)
    print("commit 1 written (append, 1 row)")

    # Read back the current table.
    current = spark.read.format("delta").load(TABLE_PATH)
    print(f"\ncurrent table ({current.count()} rows):")
    current.orderBy("id").show()

    # Time travel: read the table as of version 0 (proves the log is history).
    v0 = spark.read.format("delta").option("versionAsOf", 0).load(TABLE_PATH)
    print(f"version 0 via time travel ({v0.count()} rows):")
    v0.orderBy("id").show()

    # Inspect the transaction log -- the JSON commits ARE the table's truth.
    list_delta_log(spark, TABLE_PATH)
    print("\nlog actions (protocol / metaData / add entries per commit):")
    spark.read.json(f"{TABLE_PATH}/_delta_log/*.json").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
