"""Exploration script: watch live trades stream from the Coinbase WebSocket.

This is a *throwaway* tool for Day 2 ("understand the source") — NOT the
production producer (that's Day 4). Its only job is to let us observe the real
message schema, field types, and message velocity before we design any
infrastructure around the feed.

Run it:
    uv run python -m producer.explore_feed

Stop with Ctrl+C; it prints a short velocity summary on exit.

Coinbase Exchange WebSocket is a public, read-only market-data feed — no
account, API key, or auth required.
Docs: https://docs.cdp.coinbase.com/exchange/docs/websocket-overview
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from websockets.asyncio.client import connect

WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]
# The "matches" channel pushes every individual trade (a taker order crossing a
# resting maker order). That raw trade event is exactly what our pipeline is
# built on — unlike "ticker", which is aggregated snapshots.
CHANNEL = "matches"
STATS_EVERY_SECONDS = 5.0


def build_subscribe_message() -> str:
    """The JSON handshake that tells Coinbase what to stream to us."""
    return json.dumps(
        {
            "type": "subscribe",
            "product_ids": PRODUCTS,
            "channels": [CHANNEL],
        }
    )


def format_trade(msg: dict[str, Any]) -> str:
    """Render one trade message as a compact, aligned line."""
    ts = str(msg.get("time", ""))[:23]
    product = msg.get("product_id", "?")
    side = msg.get("side", "?")
    price = msg.get("price", "?")
    size = msg.get("size", "?")
    trade_id = msg.get("trade_id", "?")
    arrow = "^" if side == "buy" else "v"
    return (
        f"{ts:<23}  {product:<8}  {arrow} {side:<4}  "
        f"price={price:>12}  size={size:>14}  id={trade_id}"
    )


async def stream_trades() -> None:
    total = 0
    per_product: dict[str, int] = dict.fromkeys(PRODUCTS, 0)
    start = time.monotonic()
    last_stats = start
    printed_sample = False

    print(f"Connecting to {WS_URL} ...")
    async with connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(build_subscribe_message())
        print(f"Subscribed to '{CHANNEL}' for {', '.join(PRODUCTS)}. Ctrl+C to stop.\n")

        async for raw in ws:
            msg: dict[str, Any] = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "subscriptions":
                print(f"[server] subscription confirmed: {msg.get('channels')}\n")
                continue
            if msg_type == "error":
                print(f"[server error] {msg.get('message')}: {msg.get('reason')}")
                continue

            if msg_type in ("match", "last_match"):
                # Dump the very first trade in full so we can document the schema.
                if not printed_sample:
                    print("First raw trade message (for schema study):")
                    print(json.dumps(msg, indent=2))
                    print("-" * 80)
                    printed_sample = True

                total += 1
                product = msg.get("product_id", "")
                if product in per_product:
                    per_product[product] += 1
                print(format_trade(msg))

            now = time.monotonic()
            if now - last_stats >= STATS_EVERY_SECONDS:
                elapsed = now - start
                rate = total / elapsed if elapsed > 0 else 0.0
                breakdown = ", ".join(f"{p}={c}" for p, c in per_product.items())
                print(
                    f"\n[stats] {total} trades in {elapsed:.1f}s "
                    f"-> {rate:.1f} msg/s  ({breakdown})\n"
                )
                last_stats = now


def main() -> None:
    try:
        asyncio.run(stream_trades())
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
