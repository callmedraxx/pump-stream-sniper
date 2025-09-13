import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, asc, desc, or_
from sqlalchemy.orm import Session

from ..models import Token, get_db
logger = logging.getLogger(__name__)

router = APIRouter()

# Global state for SSE connections
active_connections = set()


class TokenSortService:
    """Service for sorting and filtering tokens with database optimization"""

    @staticmethod
    def get_sort_column(sort_by: str, time_period: Optional[str] = None):
        """Get the appropriate column for sorting"""
        sort_mappings = {
            "age": Token.age,
            "mcap": Token.mcap,
            "viewers": Token.viewers,
            "creator": Token.creator,
            "txns": getattr(Token, f'txns_{time_period or "24h"}'),
            "volume": getattr(Token, f'volume_{time_period or "24h"}'),
            "traders": getattr(Token, f'traders_{time_period or "24h"}'),
        }
        return sort_mappings.get(sort_by, Token.age)

    @staticmethod
    def apply_sorting(
        query, sort_by: str, sort_order: str, time_period: Optional[str] = None
    ):
        """Apply sorting to the query"""
        column = TokenSortService.get_sort_column(sort_by, time_period)

        if sort_order == "desc":
            return query.order_by(desc(column))
        else:
            return query.order_by(asc(column))

    @staticmethod
    def apply_filters(query, filters: Dict[str, Any]):
        """Apply filters to the query"""
        if filters.get("is_live") is not None:
            query = query.filter(Token.is_live == filters["is_live"])

        if filters.get("is_active") is not None:
            query = query.filter(Token.is_active == filters["is_active"])

        if filters.get("nsfw") is not None:
            query = query.filter(Token.nsfw == filters["nsfw"])

        if filters.get("creator"):
            query = query.filter(Token.creator == filters["creator"])

        if filters.get("min_mcap"):
            query = query.filter(Token.mcap >= filters["min_mcap"])

        if filters.get("max_mcap"):
            query = query.filter(Token.mcap <= filters["max_mcap"])

        return query


@router.get("/tokens")
async def get_tokens(
    # Sorting parameters
    sort_by: str = Query(
        "age",
        description="Sort field: age, mcap, viewers, txns, volume, traders, creator",
    ),
    sort_order: str = Query("desc", description="Sort order: asc or desc"),
    time_period: Optional[str] = Query(
        None, description="Time period for txns/volume/traders: 5m, 1h, 6h, 24h"
    ),
    # Filtering parameters
    is_live: Optional[bool] = Query(None, description="Filter by live status"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    nsfw: Optional[bool] = Query(None, description="Filter by NSFW content"),
    creator: Optional[str] = Query(None, description="Filter by creator address"),
    min_mcap: Optional[float] = Query(None, description="Minimum market cap filter"),
    max_mcap: Optional[float] = Query(None, description="Maximum market cap filter"),
    # Pagination
    limit: int = Query(50, description="Number of tokens to return", ge=1, le=1000),
    offset: int = Query(0, description="Offset for pagination", ge=0),
    # Response format
    format: str = Query("full", description="Response format: full, compact, minimal"),
):
    """
    Get sorted and filtered tokens from database with pagination
    """
    try:
        db = next(get_db())

        # Build base query
        query = db.query(Token)

        # Apply filters
        filters = {
            "is_live": is_live,
            "is_active": is_active,
            "nsfw": nsfw,
            "creator": creator,
            "min_mcap": min_mcap,
            "max_mcap": max_mcap,
        }
        query = TokenSortService.apply_filters(query, filters)

        # Apply sorting
        query = TokenSortService.apply_sorting(query, sort_by, sort_order, time_period)

        # Apply pagination
        total_count = query.count()
        tokens = query.offset(offset).limit(limit).all()

        # Format response based on requested format
        if format == "minimal":
            token_data = [
                {
                    "mint_address": t.mint_address,
                    "name": t.name,
                    "symbol": t.symbol,
                    "mcap": t.mcap,
                    "age": t.age.isoformat() if t.age else None,
                    "viewers": t.viewers,
                    "is_live": t.is_live,
                }
                for t in tokens
            ]
        elif format == "compact":
            token_data = []
            for t in tokens:
                token_dict = {
                    "mint_address": t.mint_address,
                    "name": t.name,
                    "symbol": t.symbol,
                    "image_url": t.image_url,
                    "mcap": t.mcap,
                    "ath": t.ath,
                    "progress": t.progress,
                    "age": t.age.isoformat() if t.age else None,
                    "viewers": t.viewers,
                    "creator": t.creator,
                    "is_live": t.is_live,
                    "is_active": t.is_active,
                }

                # Add time-specific data based on time_period only
                if time_period:
                    # Only include the specific time period data
                    token_dict.update({
                        f"volume_{time_period}": getattr(t, f"volume_{time_period}"),
                        f"txns_{time_period}": getattr(t, f"txns_{time_period}"),
                        f"traders_{time_period}": getattr(t, f"traders_{time_period}"),
                    })
                else:
                    # Include all time periods (default behavior)
                    token_dict.update({
                        "volume_24h": t.volume_24h,
                        "txns_24h": t.txns_24h,
                        "traders_24h": t.traders_24h,
                    })

                # Add creator holding data
                token_dict.update({
                    "creator_holding_percentage": t.creator_holding_percentage,
                    "creator_is_top_holder": t.creator_is_top_holder,
                })

                token_data.append(token_dict)
        else:  # full format
            token_data = []
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
                }

                # Handle time-specific data for price_changes, traders, volume, txns
                if time_period:
                    # Only include the specific time period data
                    token_dict["price_changes"] = {
                        time_period: getattr(t, f"price_change_{time_period}")
                    }
                    token_dict["traders"] = {
                        time_period: getattr(t, f"traders_{time_period}")
                    }
                    token_dict["volume"] = {
                        time_period: getattr(t, f"volume_{time_period}")
                    }
                    token_dict["txns"] = {
                        time_period: getattr(t, f"txns_{time_period}")
                    }
                else:
                    # Include all time periods (default behavior)
                    token_dict["price_changes"] = {
                        "5m": t.price_change_5m,
                        "1h": t.price_change_1h,
                        "6h": t.price_change_6h,
                        "24h": t.price_change_24h,
                    }
                    token_dict["traders"] = {
                        "5m": t.traders_5m,
                        "1h": t.traders_1h,
                        "6h": t.traders_6h,
                        "24h": t.traders_24h,
                    }
                    token_dict["volume"] = {
                        "5m": t.volume_5m,
                        "1h": t.volume_1h,
                        "6h": t.volume_6h,
                        "24h": t.volume_24h,
                    }
                    token_dict["txns"] = {
                        "5m": t.txns_5m,
                        "1h": t.txns_1h,
                        "6h": t.txns_6h,
                        "24h": t.txns_24h,
                    }

                # Add remaining fields
                token_dict.update({
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
                        "last_reply": (
                            t.last_reply.isoformat() if t.last_reply else None
                        ),
                        "last_trade_timestamp": (
                            t.last_trade_timestamp.isoformat()
                            if t.last_trade_timestamp
                            else None
                        ),
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
                })

                token_data.append(token_dict)

        return {
            "success": True,
            "data": {
                "tokens": token_data,
                "pagination": {
                    "total": total_count,
                    "limit": limit,
                    "offset": offset,
                    "has_more": (offset + limit) < total_count,
                },
                "sorting": {
                    "sort_by": sort_by,
                    "sort_order": sort_order,
                    "time_period": time_period,
                },
                "filters": {k: v for k, v in filters.items() if v is not None},
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/tokens/stream")
async def stream_tokens(
    # Sorting parameters - Enhanced with more options
    sort_by: str = Query(
        "age",
        description="Sort field: age, mcap, viewers, txns, volume, traders, creator",
        enum=["age", "mcap", "viewers", "txns", "volume", "traders", "creator"]
    ),
    sort_order: str = Query(
        "desc",
        description="Sort order: asc or desc",
        enum=["asc", "desc"]
    ),
    time_period: Optional[str] = Query(
        None,
        description="Time period for txns/volume/traders: 5m, 1h, 6h, 24h"
    ),
    # Filtering parameters
    is_live: Optional[bool] = Query(None, description="Filter by live status"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    nsfw: Optional[bool] = Query(None, description="Filter by NSFW content"),
    creator: Optional[str] = Query(None, description="Filter by creator address"),
    min_mcap: Optional[float] = Query(None, description="Minimum market cap filter"),
    max_mcap: Optional[float] = Query(None, description="Maximum market cap filter"),
    # Pagination
    limit: int = Query(1000, description="Number of tokens to return", ge=1, le=1000),
    offset: int = Query(0, description="Offset for pagination", ge=0),
    # Stream parameters
    update_interval: int = Query(
        1, description="Update interval in seconds", ge=1, le=30
    ),
    # Response format
    format: str = Query(
        "full",
        description="Response format: full",
        enum=["full"]
    ),
):
    """
    Server-Sent Events stream for real-time token updates with dynamic sorting.

    **Format Parameter:**
    - `compact`: Returns essential token data (id, symbol, name, price, mcap, volume, age, traders)
    - `full`: Returns complete token data including all fields from the /tokens endpoint

    **Data Structure:**
    ```json
    {
        "event": "tokens_update",
        "data": {
            "tokens": [...], // Array of token objects
            "sorting": {
                "sort_by": "mcap",
                "sort_order": "desc",
                "time_period": "24h",
                "format": "compact"
            },
            "timestamp": "2024-01-01T12:00:00Z",
            "change_detected": true
        },
        "format": "compact"
    }
    ```    **Client Usage:**
    ```javascript
    class TokenStreamer {
        constructor() {
            this.currentSource = null;
            this.sorting = {
                sort_by: 'age',
                sort_order: 'desc',
                time_period: null,
                format: 'compact'
            };
        }

        // Change sorting by creating new connection
        changeSorting(newSorting) {
            // Close current connection
            if (this.currentSource) {
                this.currentSource.close();
            }

            // Update sorting parameters
            this.sorting = { ...this.sorting, ...newSorting };

            // Build URL with new parameters
            const params = new URLSearchParams();
            Object.entries(this.sorting).forEach(([key, value]) => {
                if (value !== null && value !== undefined) {
                    params.append(key, value);
                }
            });

            // Create new connection
            this.currentSource = new EventSource(`/tokens/stream?${params}`);

            this.currentSource.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.event === 'tokens_update') {
                    console.log('Tokens updated:', data.data.tokens);
                    console.log('Current sorting:', data.data.sorting);
                    console.log('Format:', data.format);
                }
            };

            return this.currentSource;
        }

        // Convenience methods
        sortByMarketCap() {
            return this.changeSorting({ sort_by: 'mcap', sort_order: 'desc' });
        }

        sortByVolume(timePeriod = '24h') {
            return this.changeSorting({
                sort_by: 'volume',
                time_period: timePeriod,
                sort_order: 'desc'
            });
        }

        sortByTraders(timePeriod = '1h') {
            return this.changeSorting({
                sort_by: 'traders',
                time_period: timePeriod,
                sort_order: 'desc'
            });
        }

        useFullFormat() {
            return this.changeSorting({ format: 'full' });
        }

        useCompactFormat() {
            return this.changeSorting({ format: 'compact' });
        }

        disconnect() {
            if (this.currentSource) {
                this.currentSource.close();
                this.currentSource = null;
            }
        }
    }

    // Usage example:
    const streamer = new TokenStreamer();

    // Start with default sorting
    streamer.changeSorting({});

    // Change to market cap sorting
    setTimeout(() => streamer.sortByMarketCap(), 5000);

    // Change to volume sorting
    setTimeout(() => streamer.sortByVolume('1h'), 10000);

    // Switch to full format
    setTimeout(() => streamer.useFullFormat(), 15000);
    ```
    """

    async def generate():
        """Generate SSE events with token data"""
        connection_id = id(asyncio.current_task())
        active_connections.add(connection_id)

        try:
            db = next(get_db())
            last_update_time = None

            # Subscribe to in-process sync events so we can push immediately when sync completes
            from ..services.event_broadcaster import broadcaster
            from ..services.sync_snapshot_service import get_latest_snapshot
            sync_queue = await broadcaster.subscribe("sync_completed")
            logger.info("SSE connection %s subscribed to sync_completed", connection_id)

            # If there is a cached snapshot (from a sync that happened while no clients were connected), send it immediately
            try:
                snapshot = get_latest_snapshot()
                if snapshot is not None:
                    # Determine latest update time to avoid duplicate sends
                    latest_update = (
                        db.query(Token.updated_at).order_by(desc(Token.updated_at)).first()
                    )
                    last_update_time = latest_update[0] if latest_update else None
                    logger.info("SSE connection %s sending cached snapshot (%d tokens)", connection_id, len(snapshot.get('data', {}).get('tokens', [])))
                    yield f"data: {json.dumps(snapshot)}\n\n"
            except Exception:
                # If anything goes wrong sending snapshot, continue normal loop
                pass

            while True:
                try:
                    # Build query
                    query = db.query(Token)

                    # Apply filters
                    filters = {
                        "is_live": is_live,
                        "is_active": is_active,
                        "nsfw": nsfw,
                        "creator": creator,
                        "min_mcap": min_mcap,
                        "max_mcap": max_mcap,
                    }
                    query = TokenSortService.apply_filters(query, filters)

                    # Apply sorting
                    query = TokenSortService.apply_sorting(
                        query, sort_by, sort_order, time_period
                    )

                    # Check for updates
                    latest_update = (
                        db.query(Token.updated_at)
                        .order_by(desc(Token.updated_at))
                        .first()
                    )
                    current_update_time = latest_update[0] if latest_update else None

                    # Only send update if data has changed
                    # If a sync event arrived, force send immediately
                    forced_send = False
                    try:
                        # non-blocking get
                        payload = sync_queue.get_nowait()
                        forced_send = True
                        logger.info("SSE connection %s received sync event payload: %s", connection_id, payload)
                    except asyncio.QueueEmpty:
                        forced_send = False

                    if current_update_time != last_update_time or forced_send:
                        # Apply pagination and get tokens
                        total_count = query.count()
                        tokens = query.offset(offset).limit(limit).all()

                        # Format tokens (always full format)
                        token_data = []
                        # Full format - all fields (same as /tokens endpoint)
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
                            }

                            # Handle time-specific data for price_changes, traders, volume, txns
                            if time_period:
                                # Only include the specific time period data
                                token_dict["price_changes"] = {
                                    time_period: getattr(t, f"price_change_{time_period}")
                                }
                                token_dict["traders"] = {
                                    time_period: getattr(t, f"traders_{time_period}")
                                }
                                token_dict["volume"] = {
                                    time_period: getattr(t, f"volume_{time_period}")
                                }
                                token_dict["txns"] = {
                                    time_period: getattr(t, f"txns_{time_period}")
                                }
                            else:
                                # Include all time periods (default behavior)
                                token_dict["price_changes"] = {
                                    "5m": t.price_change_5m,
                                    "1h": t.price_change_1h,
                                    "6h": t.price_change_6h,
                                    "24h": t.price_change_24h,
                                }
                                token_dict["traders"] = {
                                    "5m": t.traders_5m,
                                    "1h": t.traders_1h,
                                    "6h": t.traders_6h,
                                    "24h": t.traders_24h,
                                }
                                token_dict["volume"] = {
                                    "5m": t.volume_5m,
                                    "1h": t.volume_1h,
                                    "6h": t.volume_6h,
                                    "24h": t.volume_24h,
                                }
                                token_dict["txns"] = {
                                    "5m": t.txns_5m,
                                    "1h": t.txns_1h,
                                    "6h": t.txns_6h,
                                    "24h": t.txns_24h,
                                }

                            # Add remaining fields
                            token_dict.update({
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
                                    "last_reply": (
                                        t.last_reply.isoformat() if t.last_reply else None
                                    ),
                                    "last_trade_timestamp": (
                                        t.last_trade_timestamp.isoformat()
                                        if t.last_trade_timestamp
                                        else None
                                    ),
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
                            })

                            token_data.append(token_dict)

                        # Create SSE event with same structure as /tokens endpoint
                        event_data = {
                            "event": "tokens_update",
                            "timestamp": datetime.now().isoformat(),
                            "format": format,  # Include format information
                            "data": {
                                "tokens": token_data,
                                "pagination": {
                                    "total": total_count,
                                    "limit": limit,
                                    "offset": offset,
                                    "has_more": (offset + limit) < total_count,
                                },
                                "sorting": {
                                    "sort_by": sort_by,
                                    "sort_order": sort_order,
                                    "time_period": time_period,
                                },
                                "filters": {k: v for k, v in filters.items() if v is not None},
                            },
                        }

                        logger.info("SSE connection %s yielding tokens_update (tokens=%d)", connection_id, len(token_data))
                        yield f"data: {json.dumps(event_data)}\n\n"
                        last_update_time = current_update_time

                    # Wait before next update
                    await asyncio.sleep(update_interval)

                except Exception as e:
                    error_event = {
                        "event": "error",
                        "timestamp": datetime.now().isoformat(),
                        "data": {
                            "error": str(e),
                            "message": "Error fetching token data",
                        },
                    }
                    yield f"data: {json.dumps(error_event)}\n\n"
                    await asyncio.sleep(update_interval)

        finally:
            # Clean up subscription
            try:
                await broadcaster.unsubscribe("sync_completed", sync_queue)
            except Exception:
                pass
            # Clean up subscription
            try:
                await broadcaster.unsubscribe("sync_completed", sync_queue)
            except Exception:
                pass
            active_connections.discard(connection_id)
            logger.info("SSE connection %s unsubscribed and closed", connection_id)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control",
        },
    )


@router.get("/tokens/stats")
async def get_token_stats():
    """
    Get overall statistics about tokens in the database
    """
    try:
        db = next(get_db())

        # Get basic counts
        total_tokens = db.query(Token).count()
        live_tokens = db.query(Token).filter(Token.is_live == True).count()
        active_tokens = db.query(Token).filter(Token.is_active == True).count()

        # Get top statistics
        top_by_mcap = db.query(Token).order_by(desc(Token.mcap)).first()
        top_by_volume = db.query(Token).order_by(desc(Token.volume_24h)).first()
        top_by_traders = db.query(Token).order_by(desc(Token.traders_24h)).first()
        top_by_viewers = db.query(Token).order_by(desc(Token.viewers)).first()

        # Get recent activity (last 24h)
        recent_cutoff = datetime.now() - timedelta(hours=24)
        recent_tokens = (
            db.query(Token).filter(Token.created_at >= recent_cutoff).count()
        )

        return {
            "success": True,
            "data": {
                "counts": {
                    "total": total_tokens,
                    "live": live_tokens,
                    "active": active_tokens,
                    "recent_24h": recent_tokens,
                },
                "top_tokens": {
                    "by_mcap": (
                        {
                            "name": top_by_mcap.name if top_by_mcap else None,
                            "symbol": top_by_mcap.symbol if top_by_mcap else None,
                            "mcap": top_by_mcap.mcap if top_by_mcap else 0,
                        }
                        if top_by_mcap
                        else None
                    ),
                    "by_volume_24h": (
                        {
                            "name": top_by_volume.name if top_by_volume else None,
                            "symbol": top_by_volume.symbol if top_by_volume else None,
                            "volume": top_by_volume.volume_24h if top_by_volume else 0,
                        }
                        if top_by_volume
                        else None
                    ),
                    "by_traders_24h": (
                        {
                            "name": top_by_traders.name if top_by_traders else None,
                            "symbol": top_by_traders.symbol if top_by_traders else None,
                            "traders": (
                                top_by_traders.traders_24h if top_by_traders else None
                            ),
                        }
                        if top_by_traders
                        else None
                    ),
                    "by_viewers": (
                        {
                            "name": top_by_viewers.name if top_by_viewers else None,
                            "symbol": top_by_viewers.symbol if top_by_viewers else None,
                            "viewers": top_by_viewers.viewers if top_by_viewers else 0,
                        }
                        if top_by_viewers
                        else None
                    ),
                },
                "last_updated": datetime.now().isoformat(),
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/tokens/{mint_address}")
async def get_token_by_mint(
    mint_address: str,
    time_period: Optional[str] = Query(None, description="Time period for txns/volume/traders: 5m, 1h, 6h, 24h"),
    sort_by: str = Query("age", description="Sort context (affects which time data to show)"),
):
    """
    Get a specific token by mint address
    """
    try:
        db = next(get_db())
        token = db.query(Token).filter(Token.mint_address == mint_address).first()

        if not token:
            raise HTTPException(status_code=404, detail="Token not found")

        # Return full token data
        token_dict = {
            "id": token.id,
            "mint_address": token.mint_address,
            "name": token.name,
            "symbol": token.symbol,
            "image_url": token.image_url,
            "age": token.age.isoformat() if token.age else None,
            "mcap": token.mcap,
            "ath": token.ath,
            "creator": token.creator,
            "total_supply": token.total_supply,
            "pump_swap_pool": token.pump_swap_pool,
            "viewers": token.viewers,
            "progress": token.progress,
            "liquidity": token.liquidity,
            "is_live": token.is_live,
            "is_active": token.is_active,
            "nsfw": token.nsfw,
            "social_links": token.social_links,
        }

        # Handle time-specific data for price_changes, traders, volume, txns
        if time_period:
            # Only include the specific time period data
            token_dict["price_changes"] = {
                time_period: getattr(token, f"price_change_{time_period}")
            }
            token_dict["traders"] = {
                time_period: getattr(token, f"traders_{time_period}")
            }
            token_dict["volume"] = {
                time_period: getattr(token, f"volume_{time_period}")
            }
            token_dict["txns"] = {
                time_period: getattr(token, f"txns_{time_period}")
            }
        else:
            # Include all time periods (default behavior)
            token_dict["price_changes"] = {
                "5m": token.price_change_5m,
                "1h": token.price_change_1h,
                "6h": token.price_change_6h,
                "24h": token.price_change_24h,
            }
            token_dict["traders"] = {
                "5m": token.traders_5m,
                "1h": token.traders_1h,
                "6h": token.traders_6h,
                "24h": token.traders_24h,
            }
            token_dict["volume"] = {
                "5m": token.volume_5m,
                "1h": token.volume_1h,
                "6h": token.volume_6h,
                "24h": token.volume_24h,
            }
            token_dict["txns"] = {
                "5m": token.txns_5m,
                "1h": token.txns_1h,
                "6h": token.txns_6h,
                "24h": token.txns_24h,
            }

        # Add remaining fields
        token_dict.update({
            "pool_info": {
                "raydium_pool": token.raydium_pool,
                "virtual_sol_reserves": token.virtual_sol_reserves,
                "real_sol_reserves": token.real_sol_reserves,
                "virtual_token_reserves": token.virtual_token_reserves,
                "real_token_reserves": token.real_token_reserves,
                "complete": token.complete,
            },
            "activity": {
                "reply_count": token.reply_count,
                "last_reply": (
                    token.last_reply.isoformat() if token.last_reply else None
                ),
                "last_trade_timestamp": (
                    token.last_trade_timestamp.isoformat()
                    if token.last_trade_timestamp
                    else None
                ),
            },
            "holders": {
                "top_holders": token.top_holders,
                "creator_holding_amount": token.creator_holding_amount,
                "creator_holding_percentage": token.creator_holding_percentage,
                "creator_is_top_holder": token.creator_is_top_holder,
            },
            "timestamps": {
                "created_at": token.created_at.isoformat(),
                "updated_at": token.updated_at.isoformat(),
            },
        })

        return {
            "success": True,
            "data": token_dict,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/tokens/{mint_address}/holders")
async def get_token_holders(mint_address: str):
    """
    Get top holders data for a specific token
    """
    try:
        db = next(get_db())
        token = db.query(Token).filter(Token.mint_address == mint_address).first()

        if not token:
            raise HTTPException(status_code=404, detail="Token not found")

        # If holders data is stale (older than 5 minutes), refresh it
        from datetime import datetime, timedelta

        five_minutes_ago = datetime.now() - timedelta(minutes=5)

        if not token.top_holders or (
            token.updated_at and token.updated_at < five_minutes_ago
        ):
            # Refresh holders data
            from ..services.fetch_top_holders import update_token_holders_data

            await update_token_holders_data(mint_address)

            # Re-fetch token data
            token = db.query(Token).filter(Token.mint_address == mint_address).first()

        return {
            "success": True,
            "data": {
                "mint_address": token.mint_address,
                "name": token.name,
                "symbol": token.symbol,
                "total_supply": token.total_supply,
                "top_holders": token.top_holders,
                "creator": {
                    "address": token.creator,
                    "holding_amount": token.creator_holding_amount,
                    "holding_percentage": token.creator_holding_percentage,
                    "is_top_holder": token.creator_is_top_holder,
                },
                "last_updated": (
                    token.updated_at.isoformat() if token.updated_at else None
                ),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.post("/tokens/{mint_address}/holders/refresh")
async def refresh_token_holders(mint_address: str):
    """
    Manually refresh holders data for a specific token
    """
    try:
        from ..services.fetch_top_holders import update_token_holders_data

        success = await update_token_holders_data(mint_address)

        if success:
            return {
                "success": True,
                "message": f"Holders data refreshed for {mint_address[:8]}...",
            }
        else:
            raise HTTPException(
                status_code=500, detail="Failed to refresh holders data"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error refreshing holders: {str(e)}"
        )
