import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..models import Token, get_db
from .event_broadcaster import broadcaster

logger = logging.getLogger(__name__)

# In-memory cache for the latest snapshot
_latest_snapshot: Optional[Dict[str, Any]] = None
_lock = asyncio.Lock()


async def _build_snapshot(db) -> Dict[str, Any]:
    """Build a full snapshot of live tokens (same shape as SSE event data.data)."""
    # Query all live tokens
    tokens = db.query(Token).filter(Token.is_live == True).all()

    token_data: List[Dict[str, Any]] = []
    for t in tokens:
        token_dict = {
            "id": t.id,
            "mint_address": t.mint_address,
            "name": t.name,
            "symbol": t.symbol,
            "image_url": t.image_url,
            "age": t.age.isoformat() if t.age else None,
            "mcap": t.mcap,
            "ath": t.ath,
            "creator": t.creator,
            "total_supply": t.total_supply,
            "pump_swap_pool": t.pump_swap_pool,
            "viewers": t.viewers,
            "progress": t.progress,
            "liquidity": t.liquidity,
            "is_live": t.is_live,
            "is_active": t.is_active,
            "nsfw": t.nsfw,
            "social_links": t.social_links,
            "price_changes": {
                "5m": t.price_change_5m,
                "1h": t.price_change_1h,
                "6h": t.price_change_6h,
                "24h": t.price_change_24h,
            },
            "traders": {
                "5m": t.traders_5m,
                "1h": t.traders_1h,
                "6h": t.traders_6h,
                "24h": t.traders_24h,
            },
            "volume": {
                "5m": t.volume_5m,
                "1h": t.volume_1h,
                "6h": t.volume_6h,
                "24h": t.volume_24h,
            },
            "txns": {
                "5m": t.txns_5m,
                "1h": t.txns_1h,
                "6h": t.txns_6h,
                "24h": t.txns_24h,
            },
            "pool_info": {
                "raydium_pool": t.raydium_pool,
                "virtual_sol_reserves": t.virtual_sol_reserves,
                "real_sol_reserves": t.real_sol_reserves,
                "virtual_token_reserves": t.virtual_token_reserves,
                "real_token_reserves": t.real_token_reserves,
                "complete": t.complete,
            },
            "activity": {
                "reply_count": t.reply_count,
                "last_reply": t.last_reply.isoformat() if t.last_reply else None,
                "last_trade_timestamp": t.last_trade_timestamp.isoformat() if t.last_trade_timestamp else None,
            },
            "holders": {
                "top_holders": t.top_holders,
                "creator_holding_amount": t.creator_holding_amount,
                "creator_holding_percentage": t.creator_holding_percentage,
                "creator_is_top_holder": t.creator_is_top_holder,
            },
            "timestamps": {
                "created_at": t.created_at.isoformat(),
                "updated_at": t.updated_at.isoformat(),
            },
        }
        token_data.append(token_dict)

    snapshot = {
        "event": "tokens_update",
        "timestamp": datetime.now().isoformat(),
        "format": "full",
        "data": {
            "tokens": token_data,
            "pagination": {
                "total": len(token_data),
                "limit": len(token_data),
                "offset": 0,
                "has_more": False,
            },
            "sorting": {},
            "filters": {},
        },
    }

    return snapshot


async def run_forever():
    """Background task: subscribe to sync events and update cached snapshot."""
    global _latest_snapshot

    # Subscribe
    q = await broadcaster.subscribe("sync_completed")
    logger.info("sync_snapshot_service subscribed to 'sync_completed' events")
    try:
        while True:
            payload = await q.get()
            logger.info("sync_snapshot_service received sync_completed event: %s", payload)
            # Build snapshot when a sync completes
            try:
                db = next(get_db())
            except Exception:
                db = None

            if db is not None:
                try:
                    snapshot = await _build_snapshot(db)
                    async with _lock:
                        _latest_snapshot = snapshot
                    logger.info("sync_snapshot_service rebuilt snapshot with %d tokens", len(snapshot.get('data', {}).get('tokens', [])))
                except Exception as e:
                    logger.exception("error building snapshot: %s", e)
                finally:
                    try:
                        db.close()
                    except Exception:
                        pass
    finally:
        try:
            await broadcaster.unsubscribe("sync_completed", q)
        except Exception:
            pass


def get_latest_snapshot() -> Optional[Dict[str, Any]]:
    return _latest_snapshot
