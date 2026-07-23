"""Exploration script: watch live trades stream from the Coinbase WebSocket.

This is a *throwaway* tool for understanding the source — NOT the production
producer (see ``producer/main.py``). Its only job is to let us observe the real
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
from collections import deque
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
    total_at_last_stats = 0
    per_product: dict[str, int] = dict.fromkeys(PRODUCTS, 0)
    start = time.monotonic()
    last_stats = start
    printed_sample = False

    # Rolling 1-second window of trade arrival times, to catch true burst peaks
    # that a cumulative average hides.
    recent: deque[float] = deque()
    peak_1s = 0

    print(f"Connecting to {WS_URL} ...")
    async with connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(build_subscribe_message())
        print(f"Subscribed to '{CHANNEL}' for {', '.join(PRODUCTS)}. Ctrl+C to stop.\n")

        async for raw in ws:
            now = time.monotonic()
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

                # Trades in the last 1s = instantaneous rate; track the max seen.
                recent.append(now)
                while recent and now - recent[0] > 1.0:
                    recent.popleft()
                peak_1s = max(peak_1s, len(recent))

                print(format_trade(msg))

            if now - last_stats >= STATS_EVERY_SECONDS:
                elapsed = now - start
                avg = total / elapsed if elapsed > 0 else 0.0
                interval_count = total - total_at_last_stats
                interval_rate = interval_count / (now - last_stats)
                breakdown = ", ".join(f"{p}={c}" for p, c in per_product.items())
                print(
                    f"\n[stats] total={total} in {elapsed:.0f}s  "
                    f"avg={avg:.1f}/s  recent={interval_rate:.1f}/s  "
                    f"peak(1s)={peak_1s}/s  ({breakdown})\n"
                )
                last_stats = now
                total_at_last_stats = total


def main() -> None:
    try:
        asyncio.run(stream_trades())
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
