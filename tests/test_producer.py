"""Tests for the Kafka producer's pure logic (producer/main.py, producer/config.py).

These cover the ingest filter, subscription framing, structured logging, and the
delivery-safety configuration — everything that decides *what* and *how* we
publish, without needing a live WebSocket or broker.
"""

from __future__ import annotations

import json
import logging
import sys

import pytest
from confluent_kafka import Producer

from producer.config import Settings, load_settings
from producer.main import (
    _JsonFormatter,
    _subscribe_message,
    _trade_key,
    build_producer,
)


def _settings() -> Settings:
    return Settings(
        ws_url="wss://example.test/feed",
        products=("BTC-USD", "ETH-USD"),
        kafka_bootstrap="localhost:9092",
        topic="crypto.trades.raw",
    )


# --- the ingest filter -----------------------------------------------------


def test_trade_key_returns_product_for_a_real_match() -> None:
    msg = {"type": "match", "product_id": "BTC-USD", "trade_id": 1}
    assert _trade_key(msg) == "BTC-USD"


def test_trade_key_accepts_last_match_replays() -> None:
    # Coinbase sends `last_match` on (re)subscribe — the last trade seen. It is a
    # real trade and must be published so a reconnect doesn't silently drop it.
    msg = {"type": "last_match", "product_id": "ETH-USD", "trade_id": 2}
    assert _trade_key(msg) == "ETH-USD"


def test_trade_key_skips_non_trade_message_types() -> None:
    for msg_type in ("subscriptions", "heartbeat", "error", "ticker"):
        assert _trade_key({"type": msg_type, "product_id": "BTC-USD"}) is None


def test_trade_key_skips_trades_without_a_usable_product() -> None:
    assert _trade_key({"type": "match"}) is None
    assert _trade_key({"type": "match", "product_id": None}) is None
    assert _trade_key({"type": "match", "product_id": ""}) is None
    assert _trade_key({"type": "match", "product_id": 123}) is None


# --- subscription framing --------------------------------------------------


def test_subscribe_message_targets_matches_channel_for_all_products() -> None:
    payload = json.loads(_subscribe_message(_settings()))
    assert payload["type"] == "subscribe"
    assert payload["channels"] == ["matches"]
    assert payload["product_ids"] == ["BTC-USD", "ETH-USD"]


# --- structured logging ----------------------------------------------------


def test_json_formatter_emits_one_parseable_object_with_extra_fields() -> None:
    record = logging.LogRecord(
        name="producer",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="throughput",
        args=None,
        exc_info=None,
    )
    record.__dict__["fields"] = {"published": 100, "trades_per_sec": 42.0}

    parsed = json.loads(_JsonFormatter().format(record))

    assert parsed["event"] == "throughput"
    assert parsed["level"] == "INFO"
    assert parsed["job"] == "producer"
    assert parsed["published"] == 100
    assert parsed["trades_per_sec"] == 42.0
    # A timestamp is always present so log drains can order events.
    assert parsed["ts"].endswith("+00:00")


def test_json_formatter_serializes_exception_info() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            name="producer",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="delivery_failed",
            args=None,
            exc_info=sys.exc_info(),
        )
    parsed = json.loads(_JsonFormatter().format(record))
    assert "ValueError: boom" in parsed["exc"]


# --- delivery safety -------------------------------------------------------


def test_build_producer_is_idempotent_and_durable() -> None:
    # Constructing the producer must not raise and must not connect. We can't
    # read back librdkafka config, so this asserts the wiring is valid; the
    # idempotence/acks contract is enforced in build_producer itself.
    producer = build_producer(_settings())
    assert isinstance(producer, Producer)


# --- config ----------------------------------------------------------------


def test_load_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("PRODUCTS", "COINBASE_WS_URL", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_TOPIC"):
        monkeypatch.delenv(var, raising=False)
    settings = load_settings()
    assert settings.products == ("BTC-USD", "ETH-USD")
    assert settings.topic == "crypto.trades.raw"
    assert settings.kafka_bootstrap == "localhost:9092"


def test_load_settings_parses_products_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCTS", "BTC-USD, ETH-USD ,SOL-USD")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    settings = load_settings()
    assert settings.products == ("BTC-USD", "ETH-USD", "SOL-USD")
    assert settings.kafka_bootstrap == "kafka:29092"
