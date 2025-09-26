import asyncio
import logging
from datetime import timedelta
from typing import List

from sqlalchemy.orm import Session

from src.models import Token
from src.models.database import SessionLocal
from .fetch_activities import fetch_creator_created_count, fetch_creator_created_counts
from .event_broadcaster import broadcaster

logger = logging.getLogger(__name__)


class CreatorCountService:
    """Background service to refresh creator created coin counts every 10 minutes.

    It updates tokens in batches with a small concurrency limit.
    """

    def __init__(self, db: Session = None, batch_size: int = 20, interval_seconds: int = 600, api_delay_seconds: float = 1):
        # Do NOT keep a long-lived SessionLocal() instance. Create sessions per
        # operation to avoid holding connections open for long periods.
        self._provided_db = db  # optional externally-provided session (for testing)
        self.batch_size = batch_size
        self.interval_seconds = interval_seconds
    # Delay between batch API calls to avoid rate limits. Can be increased; okay to take minutes.
        self.api_delay_seconds = api_delay_seconds
        self._running = False

    async def run_loop(self):
        self._running = True
        while self._running:
            try:
                await self.refresh_all_tokens()
            except Exception as e:
                logger.error(f"CreatorCountService run error: {e}")
            await asyncio.sleep(self.interval_seconds)

    async def refresh_all_tokens(self):
        # Get all tokens to update created_coin_count. Open a short-lived session to fetch tokens.
        db = self._provided_db or SessionLocal()
        try:
            tokens = db.query(Token).all()
        finally:
            if self._provided_db is None:
                try:
                    db.close()
                except Exception:
                    pass

        if not tokens:
            return

        logger.info(f"Refreshing created_coin_count for {len(tokens)} tokens for {len(set([t.creator for t in tokens if t.creator]))} creators")

        # Group tokens by creator to minimize API calls
        creator_map = {}
        for t in tokens:
            if not t.creator:
                continue
            creator_map.setdefault(t.creator, []).append(t)

        creators = list(creator_map.keys())

        # Fetch counts for all creators in batches, spacing requests to avoid rate limits.
        all_counts = {}
        for i in range(0, len(creators), self.batch_size):
            chunk = creators[i : i + self.batch_size]
            try:
                # Prefer batch fetch for speed, but still respect spacing between batches
                chunk_counts = await fetch_creator_created_counts(chunk, concurrency=4, max_retries=3)
            except Exception as e:
                logger.warning(f"Batch fetch created counts failed for chunk starting at {i}: {e}")
                # Fallback to sequential fetch for each creator in the chunk to be robust and spaced
                chunk_counts = {}
                for creator in chunk:
                    try:
                        c = await fetch_creator_created_count(creator)
                    except Exception as ee:
                        logger.warning(f"Failed to fetch created count for creator {creator}: {ee}")
                        c = 0
                    chunk_counts[creator] = c or 0

            # Normalize and store counts
            for creator, raw_count in (chunk_counts or {}).items():
                try:
                    count_int = int(raw_count or 0)
                except Exception:
                    count_int = 0
                if count_int <= 0:
                    count_int = 1
                all_counts[creator] = count_int

            # Respect delay between API batch calls to avoid rate limiting
            try:
                await asyncio.sleep(self.api_delay_seconds)
            except Exception:
                pass

        if not all_counts:
            logger.info("No creator counts fetched; skipping DB update")
            return

        # Now update ALL tokens in a single DB session/transaction
        db2 = self._provided_db or SessionLocal()
        try:
            # Fetch tokens that have creators in our map (one query)
            tokens_to_update = db2.query(Token).filter(Token.creator.in_(list(all_counts.keys()))).all()
            if not tokens_to_update:
                logger.info("No tokens found to update in DB for fetched creators")
                return

            updated = 0
            for t in tokens_to_update:
                new_count = all_counts.get(t.creator)
                if new_count is None:
                    continue
                # Only update if different to reduce dirty work
                try:
                    if t.created_coin_count != new_count:
                        t.created_coin_count = new_count
                        updated += 1
                except Exception:
                    # In case of unexpected types
                    try:
                        t.created_coin_count = int(new_count)
                        updated += 1
                    except Exception:
                        logger.debug(f"Could not set created_coin_count for token id {t.id}")

            try:
                db2.commit()
            except Exception as e:
                logger.warning(f"Failed to commit created_coin_count updates for all creators: {e}")
                try:
                    db2.rollback()
                except Exception:
                    pass
            else:
                logger.info(f"Committed created_coin_count updates for {updated} tokens")
        except Exception as e:
            logger.warning(f"Failed to apply created_coin_count updates in single transaction: {e}")
        finally:
            if self._provided_db is None:
                try:
                    db2.close()
                except Exception:
                    pass

    # per-token update method removed: we now fetch all creators and update all tokens in a single transaction

    def stop(self):
        self._running = False
