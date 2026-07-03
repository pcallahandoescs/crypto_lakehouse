"""Producer configuration, sourced from environment variables with sane defaults.

Env-driven so the same code runs unchanged on the host (Kafka at localhost:9092)
and later inside a container (Kafka at kafka:29092) — we just set env vars.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    ws_url: str
    products: tuple[str, ...]
    kafka_bootstrap: str
    topic: str


def load_settings() -> Settings:
    products = os.getenv("PRODUCTS", "BTC-USD,ETH-USD")
    return Settings(
        ws_url=os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com"),
        products=tuple(p.strip() for p in products.split(",") if p.strip()),
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        topic=os.getenv("KAFKA_TOPIC", "crypto.trades.raw"),
    )
