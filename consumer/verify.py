"""Verification consumer: prove ingestion is correct before we build on it.

Reads every message from ``crypto.trades.raw`` and validates each one against
the :class:`~consumer.schema.Trade` data contract, tracking:
  - total messages seen, valid vs. invalid (contract violations),
  - per-product counts,
  - duplicates by the ``(product_id, trade_id)`` dedup key,
  - when each partition is fully drained ("caught up").

This is a *diagnostic* tool, not part of the pipeline: it always reads from the
beginning (``auto.offset.reset=earliest``) and never commits offsets, so every
run re-checks the whole retained topic deterministically.

Run (host):
    uv run python -m consumer.verify
Stop with Ctrl+C to print the final report.
"""

from __future__ import annotations

import logging
import signal
from collections import Counter
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from consumer.config import ConsumerSettings, load_settings
from consumer.schema import Trade

logger = logging.getLogger("verify")

_PARTITIONS = 6  # matches the topic's partition count (Day 3)


class Stats:
    """Mutable running tally of what the consumer has verified so far."""

    def __init__(self) -> None:
        self.total = 0
        self.valid = 0
        self.invalid = 0
        self.duplicates = 0
        self.per_product: Counter[str] = Counter()
        self.seen_keys: set[tuple[str, int]] = set()
        self.eof_partitions: set[int] = set()
        self.sample_errors: list[str] = []

    def record(self, trade: Trade) -> None:
        self.valid += 1
        self.per_product[trade.product_id] += 1
        key = trade.dedup_key
        if key in self.seen_keys:
            self.duplicates += 1
        else:
            self.seen_keys.add(key)

    def record_invalid(self, error: str) -> None:
        self.invalid += 1
        if len(self.sample_errors) < 5:
            self.sample_errors.append(error)


def _build_consumer(settings: ConsumerSettings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.group_id,
            "client.id": "crypto-verify",
            # Read the whole topic from the start, every run.
            "auto.offset.reset": "earliest",
            # Diagnostic tool: never advance committed offsets.
            "enable.auto.commit": False,
            # Emit an EOF event per partition so we can report "caught up".
            "enable.partition.eof": True,
        }
    )


def _report(stats: Stats) -> None:
    logger.info("---- verification report ----")
    logger.info("messages checked : %d", stats.total)
    logger.info("valid (contract) : %d", stats.valid)
    logger.info("invalid          : %d", stats.invalid)
    logger.info("duplicates (key) : %d", stats.duplicates)
    logger.info("per product      : %s", dict(stats.per_product))
    if stats.sample_errors:
        logger.info("sample contract violations (up to 5):")
        for err in stats.sample_errors:
            logger.info("  - %s", err.replace("\n", " "))
    verdict = "PASS" if stats.invalid == 0 and stats.total > 0 else "CHECK"
    logger.info("verdict          : %s", verdict)


def run() -> None:
    settings = load_settings()
    consumer = _build_consumer(settings)
    consumer.subscribe([settings.topic])

    stop = False

    def _handle_signal(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    stats = Stats()
    logger.info(
        "verifying topic=%s bootstrap=%s (Ctrl+C to stop)",
        settings.topic,
        settings.kafka_bootstrap,
    )

    try:
        while not stop:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    partition = msg.partition()
                    if partition is not None:
                        stats.eof_partitions.add(partition)
                    logger.info(
                        "caught up: partition %s drained (%d/%d)",
                        partition,
                        len(stats.eof_partitions),
                        _PARTITIONS,
                    )
                    continue
                raise KafkaException(err)

            stats.total += 1
            value = msg.value()
            if value is None:
                # Producer never emits null values; a tombstone here is unexpected.
                stats.record_invalid("null message value")
                continue
            try:
                trade = Trade.model_validate_json(value)
            except ValidationError as exc:
                stats.record_invalid(str(exc))
            else:
                stats.record(trade)

            if stats.total % 500 == 0:
                logger.info(
                    "checked=%d valid=%d invalid=%d dupes=%d %s",
                    stats.total,
                    stats.valid,
                    stats.invalid,
                    stats.duplicates,
                    dict(stats.per_product),
                )
    finally:
        consumer.close()
        _report(stats)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run()


if __name__ == "__main__":
    main()
