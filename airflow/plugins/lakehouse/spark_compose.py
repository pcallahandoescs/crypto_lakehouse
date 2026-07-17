"""Shared helpers for submitting Spark batch jobs from Airflow via Docker.

Uses ``docker run`` (not ``docker compose run``) so volume paths resolve on the
**host** — required when the scheduler talks to Docker Desktop through a mounted
socket (``/opt/project/...`` is not a valid Mac host path).
"""

from __future__ import annotations

import os

HOST_DIR = os.environ.get("LAKEHOUSE_HOST_DIR", "/opt/project")
NETWORK = os.environ.get("LAKEHOUSE_DOCKER_NETWORK", "crypto_pipeline_project_default")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "crypto-lakehouse-spark:3.5.3")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
DOCKER_MEMORY = os.environ.get("SPARK_DOCKER_MEMORY", "4g")


def spark_bash_command(
    script: str,
    extra_args: str = "",
    extra_env: dict[str, str] | None = None,
) -> str:
    """Return bash that runs one Spark job in the lakehouse Spark image."""
    args_suffix = f" {extra_args}" if extra_args else ""
    env_pairs = {"BATCH_MINIMAL": "1", **(extra_env or {})}
    env_flags = " ".join(f'-e {key}="{value}"' for key, value in env_pairs.items())
    return (
        "docker run --rm "
        f"--memory={DOCKER_MEMORY} --shm-size=512m "
        f'--network "{NETWORK}" '
        f'-v "{HOST_DIR}/spark_jobs:/opt/spark/work-dir" '
        f"{env_flags} "
        "-e MINIO_ENDPOINT=http://minio:9000 "
        "-e MINIO_ACCESS_KEY=minioadmin "
        "-e MINIO_SECRET_KEY=minioadmin "
        "-e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 "
        "-e KAFKA_TOPIC=crypto.trades.raw "
        "-w /opt/spark/work-dir "
        f"{SPARK_IMAGE} "
        f'/opt/spark/bin/spark-submit --master "local[*]" '
        f"--driver-memory {SPARK_DRIVER_MEMORY} "
        f"{script}{args_suffix}"
    )
