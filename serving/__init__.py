"""Serving layer: a read-only FastAPI over the gold Delta tables.

This is the lakehouse's front door for consumers (a dashboard, an analyst, a
model). It reads gold directly with delta-rs — no Spark, no JVM — because a
request/response API needs millisecond-cheap opens, not a heavyweight session.
"""
