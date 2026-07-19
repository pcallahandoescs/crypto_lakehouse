"""Producer service: stream live Coinbase trades into Kafka.

Reads the Coinbase Exchange `matches` feed over WebSocket and publishes each
trade to the `crypto.trades.raw` topic, keyed by `product_id` so each market's
trades keep their order (and land in a stable partition).

Design notes:
- **Auto-reconnect:** the `websockets` reconnecting iterator re-establishes the
  connection with backoff when it drops (it will).
- **Idempotent producer:** `enable.idempotence` + `acks=all` avoid duplicate or
  lost messages on retry, without sacrificing throughput.
- **Raw fidelity:** we forward the exact bytes Coinbase sent. Parsing, typing,
  and timestamps happen downstream (bronze/silver, Days 10-11), so bronze stays
  a faithful record of the source.

Run (host):
    uv run python -m producer.main
Stop with Ctrl+C (flushes in-flight messages before exiting).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import sys
import time
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from producer.config import Settings, load_settings

logger = logging.getLogger("producer")

TRADE_TYPES = ("match", "last_match")

# How often (in messages) to emit a throughput metric.
THROUGHPUT_EVERY = 100


class _JsonFormatter(logging.Formatter):
    """One JSON object per log line (job, event, level, ts, + extra fields)."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(),
            "level": record.levelname,
            "job": "producer",
            "event": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if fields:
            payload.update(fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _log(event: str, level: int = logging.INFO, **fields: Any) -> None:
    logger.log(level, event, extra={"fields": fields})


def build_producer(settings: Settings) -> Producer:
    """Create a durable, idempotent Kafka producer."""
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "client.id": "crypto-trades-producer",
            # Idempotence -> no duplicates on internal retries; implies acks=all
            # and preserves per-key ordering.
            "enable.idempotence": True,
            "acks": "all",
            # Small batching window + compression: cheaper, higher throughput,
            # negligible added latency for our rate.
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


def _on_delivery(err: Any, msg: Any) -> None:
    """Delivery callback: log only failures (success is the common case)."""
    if err is not None:
        _log("delivery_failed", level=logging.ERROR, key=str(msg.key()), error=str(err))


def _subscribe_message(settings: Settings) -> str:
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": list(settings.products),
            "channels": ["matches"],
        }
    )


async def run() -> None:
    settings = load_settings()
    producer = build_producer(settings)
    subscribe = _subscribe_message(settings)

    # Graceful shutdown: SIGINT (Ctrl+C) and SIGTERM (Docker stop) set the flag.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    _log(
        "starting",
        ws=settings.ws_url,
        topic=settings.topic,
        bootstrap=settings.kafka_bootstrap,
        products=list(settings.products),
    )

    published = 0
    window_start = time.monotonic()
    # This async-for reconnects automatically when the connection drops.
    async for ws in connect(settings.ws_url, ping_interval=20, ping_timeout=20):
        try:
            await ws.send(subscribe)
            _log("subscribed", channel="matches", products=list(settings.products))

            async for raw in ws:
                if stop.is_set():
                    break

                msg: dict[str, Any] = json.loads(raw)
                if msg.get("type") not in TRADE_TYPES:
                    continue

                product = msg.get("product_id")
                if not isinstance(product, str):
                    continue

                value = raw if isinstance(raw, bytes) else raw.encode()
                try:
                    producer.produce(
                        settings.topic,
                        key=product.encode(),
                        value=value,
                        on_delivery=_on_delivery,
                    )
                except BufferError:
                    # Local queue full: let the client drain, then retry once.
                    producer.poll(0.5)
                    producer.produce(
                        settings.topic,
                        key=product.encode(),
                        value=value,
                        on_delivery=_on_delivery,
                    )

                # Serve delivery callbacks without blocking.
                producer.poll(0)
                published += 1
                if published % THROUGHPUT_EVERY == 0:
                    now = time.monotonic()
                    elapsed = now - window_start
                    rate = THROUGHPUT_EVERY / elapsed if elapsed > 0 else None
                    _log(
                        "throughput",
                        published=published,
                        trades_per_sec=round(rate, 2) if rate is not None else None,
                    )
                    window_start = now

            if stop.is_set():
                break
        except ConnectionClosed:
            _log("websocket_closed", level=logging.WARNING)
            continue
        finally:
            producer.flush(5)

    _log("stopped", published=published)


def main() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    # SIGINT is handled inside run(); this is a belt-and-suspenders fallback.
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
