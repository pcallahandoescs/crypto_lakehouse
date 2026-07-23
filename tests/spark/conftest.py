"""Fixtures for the JVM-backed Spark tests.

A single local SparkSession is shared across the module's tests (session start
is the expensive part). These tests exercise the real transformation and
data-quality functions from ``spark_jobs/`` on small in-memory DataFrames — no
MinIO/Delta required, so they run anywhere a JVM + PySpark are available.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.master("local[1]")
        .appName("lakehouse-tests")
        # Keep the local session lean and deterministic.
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    try:
        yield session
    finally:
        session.stop()
