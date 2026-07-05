"""Verification-consumer configuration, from env vars with sane defaults.

Mirrors producer/config.py so the same binary runs on the host (localhost:9092)
or in a container (kafka:29092) with only env changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumerSettings:
    kafka_bootstrap: str
    topic: str
    group_id: str


def load_settings() -> ConsumerSettings:
    return ConsumerSettings(
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "crypto.trades.raw"),
        group_id=os.getenv("CONSUMER_GROUP", "crypto-verify"),
    )
