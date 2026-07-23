"""Serving configuration — env-driven, same pattern as the producer.

The API needs two things: where the gold tables live, and the S3 credentials to
read them. delta-rs talks to MinIO through its object-store S3 backend, which is
configured entirely via ``storage_options`` (the ``AWS_*`` keys below) — the same
bucket layout Spark writes to, reached the same S3-compatible way.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _products() -> tuple[str, ...]:
    raw = os.getenv("PRODUCTS", "BTC-USD,ETH-USD")
    return tuple(p.strip() for p in raw.split(",") if p.strip())


@dataclass(frozen=True)
class Settings:
    """Immutable, fully-resolved serving config (built once at startup)."""

    ohlc_uri: str
    realtime_uri: str
    runs_uri: str
    products: tuple[str, ...]
    storage_options: dict[str, str]


def load_settings() -> Settings:
    """Read the environment into a :class:`Settings`.

    Defaults target the docker-compose network (``minio:9000``); override
    ``MINIO_ENDPOINT`` to point at real S3/GCS/ADLS with no code change.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    base = os.getenv("GOLD_BASE_URI", "s3://gold")
    storage_options = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        # MinIO is plain HTTP in local dev; delta-rs refuses http:// without this.
        "AWS_ALLOW_HTTP": "true",
    }
    return Settings(
        ohlc_uri=f"{base}/ohlc",
        realtime_uri=f"{base}/realtime_metrics",
        runs_uri=f"{base}/_observability/runs",
        products=_products(),
        storage_options=storage_options,
    )
