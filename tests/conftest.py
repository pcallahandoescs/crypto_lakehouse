"""Shared pytest configuration.

The tests under ``tests/spark/`` need a JVM + PySpark. They are skipped
automatically wherever PySpark isn't installed (the default fast gate), so
``make check`` stays JVM-free. Install the extra and run them with
``make test-spark`` (``uv sync --group spark``).
"""

from __future__ import annotations

from importlib.util import find_spec

collect_ignore: list[str] = []

if find_spec("pyspark") is None:
    collect_ignore.append("spark")
