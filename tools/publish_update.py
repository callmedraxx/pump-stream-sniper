#!/usr/bin/env python3
"""Simple publisher to send a MessagePack token update to Redis for testing.

Usage examples:
  python tools/publish_update.py --mint HsX... --mcap 123456
  python tools/publish_update.py --repeat 5 --interval 1.0
"""
import asyncio
import argparse
import os
import time
import msgpack
import redis.asyncio as redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
CHANNEL = "token_updates"


async def publish_once(redis_url: str, payload: dict):
    client = redis.from_url(redis_url)
    try:
        packed = msgpack.packb(payload, use_bin_type=True)
        await client.publish(CHANNEL, packed)
        print(f"Published payload to {CHANNEL}: {payload}")
    finally:
        try:
            await client.close()
        except Exception:
            pass


def build_payload(mint: str, mcap: float = None, price_5m: float = None, event_type: str = "token_updated"):
    # Build either a token_updated minimal nested payload or an mcap_update payload
    if event_type == "mcap_update":
        payload = {
            "type": "mcap_update",
            "data": {
                "mint_address": mint,
                "mcap": mcap if mcap is not None else 0.0,
                "progress": 0.0,
                "ath": price_5m if price_5m is not None else 0.0,
                "is_live": True,
            },
            "timestamp": time.time(),
        }
        return payload

    # Default: token_updated minimal nested payload matching the backend's minimal update shape
    payload = {
        "type": "token_updated",
        "mint_address": mint,
        "data": {},
        "timestamp": time.time(),
    }
    if mcap is not None:
        payload["data"]["mcap"] = mcap
    if price_5m is not None:
        payload["data"].setdefault("price_changes", {})["5m"] = price_5m
    return payload


async def main_async(args):
    payload = build_payload(args.mint, args.mcap, args.price5m, event_type=args.event_type)

    if args.repeat <= 1:
        await publish_once(REDIS_URL, payload)
    else:
        for i in range(args.repeat):
            payload["timestamp"] = time.time()
            await publish_once(REDIS_URL, payload)
            await asyncio.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser(description="Publish a test msgpack token update to Redis")
    parser.add_argument("--mint", required=True, help="Mint address of token")
    parser.add_argument("--mcap", type=float, default=None, help="Optional market cap to include")
    parser.add_argument("--price5m", type=float, default=None, help="Optional 5m price change")
    parser.add_argument("--repeat", type=int, default=1, help="How many times to publish")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between repeats (seconds)")
    parser.add_argument("--event-type", choices=["token_updated", "mcap_update"], default="token_updated",
                        help="Type of event to publish to Redis channel token_updates")

    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
