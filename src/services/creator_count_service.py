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

    def __init__(self, db: Session = None, batch_size: int = 50, interval_seconds: int = 600):
        self.db = db or SessionLocal()
        self.batch_size = batch_size
        self.interval_seconds = interval_seconds
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
        # Get all tokens to update created_coin_count
        tokens = self.db.query(Token).all()
        if not tokens:
            return

        logger.info(f"Refreshing created_coin_count for {len(tokens)} tokens in batches of {self.batch_size}")

        # Group tokens by creator to minimize API calls
        creator_map = {}
        for t in tokens:
            if not t.creator:
                continue
            creator_map.setdefault(t.creator, []).append(t)

        # Process creators in chunks to avoid overwhelming API
        creators = list(creator_map.keys())
        for i in range(0, len(creators), self.batch_size):
            chunk = creators[i : i + self.batch_size]
            try:
                counts = await fetch_creator_created_counts(chunk, concurrency=4, max_retries=3)
            except Exception as e:
                logger.error(f"Batch fetch created counts failed for chunk starting at {i}: {e}")
                counts = {c: 0 for c in chunk}

            # Apply results to tokens
            for creator in chunk:
                count = counts.get(creator, 0)
                try:
                    count_int = int(count or 0)
                except Exception:
                    count_int = 0
                if count_int <= 0:
                    count_int = 1

                for token in creator_map.get(creator, []):
                    try:
                        old_count = token.created_coin_count
                        token.created_coin_count = count_int
                        self.db.commit()
                        #logger.info(f"Refreshed created_coin_count for {token.mint_address}: {count_int}")
                        
                        # Publish token_updated event if count changed
                        if old_count != count_int:
                            try:
                                payload = {
                                    "type": "token_updated",
                                    "data": {
                                        "mint_address": token.mint_address,
                                        "created_coin_count": token.created_coin_count,
                                        "is_live": token.is_live,
                                        "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                                    },
                                }
                                ok = broadcaster.schedule_publish("token_updated", payload)
                                if not ok:
                                    logger.warning("broadcaster.schedule_publish returned False for creator count update %s", token.mint_address)
                            except Exception:
                                logger.exception("Failed to publish token_updated from creator_count_service for %s", token.mint_address)
                                
                    except Exception as e:
                        logger.warning(f"Failed to update DB for {token.mint_address}: {e}")

    async def _update_token_created_count(self, mint_address: str, creator_address: str):
        try:
            count = await fetch_creator_created_count(creator_address)
            if count is None:
                return

            # Enforce business rule: created_coin_count cannot be None or 0
            try:
                count_int = int(count or 0)
            except Exception:
                count_int = 0

            if count_int <= 0:
                count_int = 1

            # Update DB record
            try:
                token = self.db.query(Token).filter(Token.mint_address == mint_address).first()
                if token:
                    old_count = token.created_coin_count
                    token.created_coin_count = int(count_int)
                    self.db.commit()
                    #logger.info(f"Refreshed created_coin_count for {mint_address}: {count_int}")
                    
                    # Publish token_updated event if count changed
                    if old_count != count_int:
                        try:
                            payload = {
                                "type": "token_updated",
                                "data": {
                                    "mint_address": token.mint_address,
                                    "created_coin_count": token.created_coin_count,
                                    "is_live": token.is_live,
                                    "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                                },
                            }
                            ok = broadcaster.schedule_publish("token_updated", payload)
                            if not ok:
                                logger.warning("broadcaster.schedule_publish returned False for creator count update %s", mint_address)
                        except Exception:
                            logger.exception("Failed to publish token_updated from creator_count_service for %s", mint_address)
                            
            except Exception as e:
                logger.warning(f"Failed to update DB for {mint_address}: {e}")
        except Exception as e:
            logger.error(f"Error fetching created count for {creator_address}: {e}")

    def stop(self):
        self._running = False
