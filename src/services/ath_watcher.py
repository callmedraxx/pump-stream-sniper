import asyncio
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List

from .fetch_ath import ATHService
from ..models import Token
from ..models.database import get_db

logger = logging.getLogger(__name__)

# File to record last full refresh timestamp
LAST_FULL_REFRESH_FILE = os.path.join(os.getcwd(), "generated", "last_ath_full_refresh.txt")


def _read_last_full_refresh() -> datetime:
    try:
        if os.path.exists(LAST_FULL_REFRESH_FILE):
            with open(LAST_FULL_REFRESH_FILE, "r") as f:
                text = f.read().strip()
                if text:
                    return datetime.fromisoformat(text)
    except Exception:
        pass
    return datetime.fromtimestamp(0)


def _write_last_full_refresh(dt: datetime):
    try:
        os.makedirs(os.path.dirname(LAST_FULL_REFRESH_FILE), exist_ok=True)
        with open(LAST_FULL_REFRESH_FILE, "w") as f:
            f.write(dt.isoformat())
    except Exception as e:
        logger.warning(f"Failed to write last full refresh file: {e}")


async def fetch_and_update_for_addresses(addresses: List[str], batch_size: int = 50, delay: float = 1.0):
    if not addresses:
        return {"total_tokens": 0, "updated_tokens": 0, "failed_tokens": 0, "duration_seconds": 0}

    service = ATHService(batch_size=batch_size, delay_between_batches=delay)
    return await service.update_ath_for_addresses(addresses)


async def run_once(batch_size: int = 50, delay: float = 1.0, full_refresh_hours: int = 24):
    start = time.time()
    db = next(get_db())

    try:
        # Only fetch ATH for new tokens (ath == 0.0) - one-time setup
        new_tokens = db.query(Token).filter(Token.ath == 0.0).all()
        new_addresses = [t.mint_address for t in new_tokens]
        if new_addresses:
            logger.info(f"ATH Watcher: found {len(new_addresses)} new tokens with ath=0.0; fetching initial ATH in batches")
            stats = await fetch_and_update_for_addresses(new_addresses, batch_size=batch_size, delay=delay)
            logger.info(f"ATH Watcher: updated new tokens: {stats}")

        # Note: Real-time ATH updates are now handled by the websocket mcap updates in stream.py
        # ATH is only increased when mcap exceeds current ATH, and progress is recalculated in real-time.

        duration = time.time() - start
        return {"duration_seconds": duration}

    except Exception as e:
        logger.error(f"ATH Watcher run_once error: {e}")
        raise
    finally:
        try:
            db.close()
        except Exception:
            pass


async def run_loop(interval_seconds: int = 300, batch_size: int = 50, delay: float = 1.0, full_refresh_hours: int = 24):
    logger.info("Starting ATH Watcher loop")
    while True:
        try:
            await run_once(batch_size=batch_size, delay=delay, full_refresh_hours=full_refresh_hours)
        except Exception as e:
            logger.error(f"ATH Watcher error during run: {e}")
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    import argparse

    parser = argparse.ArgumentParser(description="ATH Watcher: fetch ATH for new and candidate tokens and full refresh periodically")
    parser.add_argument("--interval", type=int, default=300, help="Loop interval seconds between checks (default 300)")
    parser.add_argument("--batch", type=int, default=50, help="Batch size for ATH requests (default 50)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between batches in seconds (default 1.0)")
    parser.add_argument("--full-hours", type=int, default=24, help="Full refresh interval in hours (default 24)")
    parser.add_argument("--once", action="store_true", help="Run once and exit")

    args = parser.parse_args()

    if args.once:
        asyncio.run(run_once(batch_size=args.batch, delay=args.delay, full_refresh_hours=args.full_hours))
    else:
        asyncio.run(run_loop(interval_seconds=args.interval, batch_size=args.batch, delay=args.delay, full_refresh_hours=args.full_hours))
