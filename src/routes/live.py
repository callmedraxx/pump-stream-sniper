import asyncio
import json
import logging
import copy
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, asc, desc, or_
from sqlalchemy.orm import Session

from ..models import Token, get_db
logger = logging.getLogger(__name__)

router = APIRouter()

# Global state for SSE connections
active_connections = set()
# Global state for WebSocket connections
ws_active_connections = set()


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
    limit: int = Query(2000, description="Number of tokens to return", ge=1, le=1000),
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



@router.websocket("/ws/tokens_stream")
async def websocket_tokens_stream(websocket: WebSocket):
    """WebSocket endpoint that streams full tokens snapshot on DB changes."""
    conn_id = id(websocket)
    sync_queues = {}
    
    try:
        await websocket.accept()
        ws_active_connections.add(conn_id)
        logger.info("WebSocket %s connected to tokens_stream", conn_id)
    except WebSocketDisconnect:
        logger.info("WebSocket connection closed before accept()")
        return
    except Exception as e:
        logger.exception("WebSocket accept failed: %s", e)
        return

    from ..services.event_broadcaster import broadcaster
    from ..services.sync_snapshot_service import get_latest_snapshot

    try:
        # Subscribe to both snapshot and token-level updates so we can react to either
        q_snapshot = await asyncio.wait_for(broadcaster.subscribe("snapshot_updated"), timeout=5.0)
        q_token = await asyncio.wait_for(broadcaster.subscribe("token_updated"), timeout=5.0)
        sync_queues = {"snapshot_updated": q_snapshot, "token_updated": q_token}
        logger.info("WebSocket %s subscribed to snapshot_updated and token_updated (snapshot_queue=%s, token_queue=%s)", 
                   conn_id, id(q_snapshot), id(q_token))
    except asyncio.TimeoutError:
        logger.error("WebSocket %s subscription timeout", conn_id)
        await _cleanup_connection(websocket, conn_id, None)
        return
    except Exception as e:
        logger.exception("WebSocket %s failed to subscribe: %s", conn_id, e)
        await _cleanup_connection(websocket, conn_id, None)
        return

    try:
        # Send cached snapshot immediately
        await _send_initial_snapshot(websocket, conn_id, sync_queues)

        # Monitor connection health
        logger.info("WebSocket %s starting main event loop, client_state=%s", conn_id, websocket.client_state)

        # Main event loop with better error handling (listening to both queues)
        await _handle_websocket_events(websocket, conn_id, sync_queues)

    except WebSocketDisconnect:
        logger.info("WebSocket %s disconnected by client", conn_id)
    except Exception as e:
        logger.exception("WebSocket %s unexpected error: %s", conn_id, e)
    finally:
        # cleanup using the dict of queues (or None)
        try:
            await _cleanup_connection(websocket, conn_id, sync_queues)
        except Exception:
            await _cleanup_connection(websocket, conn_id, None)


async def _send_initial_snapshot(websocket: WebSocket, conn_id: int, sync_queues: dict):
    """Send initial snapshot and drain queue to prevent double-sends."""
    try:
        from ..services.sync_snapshot_service import get_latest_snapshot

        snapshot = get_latest_snapshot()
        if snapshot is not None:
            # Deepcopy to avoid mutating the cached snapshot
            snapshot_to_send = copy.deepcopy(snapshot)
            tokens = snapshot_to_send.get('data', {}).get('tokens', []) or []
            # Filter only tokens with is_live == True
            filtered = [t for t in tokens if t.get('is_live')]
            snapshot_to_send['data']['tokens'] = filtered
            # Update pagination total/has_more if present
            try:
                if 'pagination' in snapshot_to_send.get('data', {}):
                    snapshot_to_send['data']['pagination']['total'] = len(filtered)
                    snapshot_to_send['data']['pagination']['has_more'] = False
            except Exception:
                pass

            logger.info("WebSocket %s sending INITIAL snapshot to frontend (%d live tokens)", 
                       conn_id, len(filtered))

            # Serialize JSON off the event loop to avoid blocking large dumps
            try:
                payload_text = await asyncio.to_thread(json.dumps, snapshot_to_send)
            except Exception:
                # Fallback to direct serialization if thread offload fails
                payload_text = json.dumps(snapshot_to_send)

            logger.info("WebSocket %s initial snapshot serialized (%d bytes), sending to frontend", 
                       conn_id, len(payload_text))

            # Allow more time for large payloads; don't disconnect on a single slow send
            try:
                await asyncio.wait_for(
                    websocket.send_text(payload_text),
                    timeout=15.0,
                )
                logger.info("WebSocket %s successfully sent INITIAL snapshot to frontend", conn_id)
            except asyncio.TimeoutError:
                logger.warning("WebSocket %s initial snapshot send timeout (will retry later)", conn_id)
            except Exception as e:
                # Re-raise disconnect-related exceptions so upstream handles cleanup
                if isinstance(e, WebSocketDisconnect):
                    raise
                logger.exception("WebSocket %s error sending initial snapshot: %s", conn_id, e)
            
            # Drain queued events to prevent duplicates from both queues
            drained = 0
            try:
                while drained < 200:  # Prevent infinite loop across two queues
                    empty = True
                    for q in sync_queues.values():
                        try:
                            q.get_nowait()
                            drained += 1
                            empty = False
                        except asyncio.QueueEmpty:
                            continue
                    if empty:
                        break
            except Exception:
                pass

            if drained > 0:
                logger.debug("WebSocket %s drained %d queued events", conn_id, drained)
                
    except asyncio.TimeoutError:
        logger.warning("WebSocket %s initial snapshot send timeout", conn_id)
        raise WebSocketDisconnect
    except (RuntimeError, WebSocketDisconnect, ConnectionResetError):
        logger.info("WebSocket %s disconnected during initial snapshot", conn_id)
        raise WebSocketDisconnect
    except Exception as e:
        logger.exception("WebSocket %s initial snapshot error: %s", conn_id, e)
        # Don't raise - continue to main loop


async def _handle_websocket_events(websocket: WebSocket, conn_id: int, sync_queues: dict):
    """Main event loop with improved error handling and timeouts."""
    import time
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    last_send_time = 0.0  # Track when we last sent a snapshot to avoid overwhelming client
    loop_iterations = 0
    last_activity_time = time.time()  # Track last successful activity
    pending_events = []  # Batch events before sending
    
    while True:
        loop_iterations += 1
        current_time = time.time()
        
        # Check for websocket health - if no activity for too long, break
        if current_time - last_activity_time > 300:  # 5 minutes without activity
            logger.error("WebSocket %s no activity for 5 minutes, assuming dead connection", conn_id)
            break
            
        try:
            # Wait for sync event with timeout for heartbeat
            try:
                # Wait for either queue (snapshot_updated or token_updated)
                # Create tasks explicitly to avoid the coroutine error
                logger.info("WebSocket %s waiting for events from queues... (iteration %d)", conn_id, loop_iterations)
                
                # Validate queues are still alive
                for name, q in sync_queues.items():
                    if q is None:
                        logger.error("WebSocket %s queue %s is None, reconnecting...", conn_id, name)
                        raise RuntimeError(f"Queue {name} is None")
                
                tasks = [asyncio.create_task(sync_queues[ev].get()) for ev in sync_queues]
                done, pending = await asyncio.wait(
                    tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=25.0,
                )

                if not done:
                    # Timeout - send heartbeat and check for batched events to flush
                    logger.info("WebSocket %s event timeout after 25s, sending heartbeat (client_state=%s)", 
                              conn_id, websocket.client_state)
                    
                    # Check if websocket is actually still alive before sending heartbeat
                    if (websocket.client_state == websocket.client_state.DISCONNECTED or
                        hasattr(websocket, '_close_sent') and websocket._close_sent):
                        logger.warning("WebSocket %s detected disconnection during heartbeat check", conn_id)
                        break
                    
                    # Flush any pending batched events on timeout
                    if pending_events and time.time() - last_send_time >= 1.0:
                        try:
                            latest_payload = pending_events[-1]['payload']
                            logger.info("WebSocket %s flushing %d batched events on timeout", conn_id, len(pending_events))
                            await _send_snapshot_update(websocket, conn_id, latest_payload)
                            last_send_time = time.time()
                            pending_events = []
                        except Exception as e:
                            logger.exception("WebSocket %s error flushing batched events: %s", conn_id, e)
                        
                    await _send_heartbeat(websocket, conn_id)
                    consecutive_errors = max(0, consecutive_errors - 1)  # Reduce error count on successful heartbeat
                    last_activity_time = time.time()  # Update activity time
                else:
                    logger.info("WebSocket %s received %d completed events", conn_id, len(done))
                    # Retrieve payload from completed future(s)
                    for fut in done:
                        try:
                            payload = fut.result()
                        except Exception as e:
                            logger.warning("WebSocket %s error getting result from future: %s", conn_id, e)
                            payload = None

                        if payload is None:
                            logger.warning("WebSocket %s received None payload from queue", conn_id)
                            continue

                        consecutive_errors = 0
                        last_activity_time = time.time()  # Update activity time
                        event_type = payload.get('type', 'unknown') if isinstance(payload, dict) else 'non-dict'
                        
                        # Add event to pending batch
                        pending_events.append({'type': event_type, 'payload': payload, 'time': time.time()})
                        logger.debug("WebSocket %s queued event: %s (batch size: %d)", conn_id, event_type, len(pending_events))
                    
                    # Process batched events - only send if enough time has passed or batch is large
                    if pending_events:
                        current_time = time.time()
                        time_since_last_send = current_time - last_send_time
                        batch_size = len(pending_events)
                        
                        should_send = (
                            time_since_last_send >= 1.0 or  # Minimum 1 second between sends
                            (batch_size >= 10 and time_since_last_send >= 0.5) or  # Large batch after 500ms
                            time_since_last_send >= 3.0  # Force send after 3 seconds regardless
                        )
                        
                        if should_send:
                            try:
                                # Check queue sizes to detect backup
                                queue_sizes = {}
                                for name, q in sync_queues.items():
                                    queue_sizes[name] = q.qsize()
                                if any(size > 10 for size in queue_sizes.values()):
                                    logger.warning("WebSocket %s queue backup detected: %s", conn_id, queue_sizes)
                                
                                # Send snapshot for the batched events (use the latest payload)
                                latest_payload = pending_events[-1]['payload']
                                logger.info("WebSocket %s sending snapshot for %d batched events (%.1fs since last send)", 
                                           conn_id, batch_size, time_since_last_send)
                                await _send_snapshot_update(websocket, conn_id, latest_payload)
                                last_send_time = current_time
                                pending_events = []  # Clear the batch
                            except WebSocketDisconnect:
                                logger.info("WebSocket %s disconnected while processing batched events", conn_id)
                                raise  # Re-raise to break out of the main loop
                            except Exception as e:
                                logger.exception("WebSocket %s error processing batched events: %s", conn_id, e)
                        else:
                            logger.debug("WebSocket %s batching events (%d queued, %.1fs remaining)", 
                                       conn_id, batch_size, 1.0 - time_since_last_send)
                        
                    # Cancel any pending gets to avoid leaked tasks
                    for p in pending:
                        try:
                            p.cancel()
                        except Exception:
                            pass
                            
            except WebSocketDisconnect:
                logger.info("WebSocket %s client disconnected", conn_id)
                break
            except asyncio.CancelledError:
                logger.info("WebSocket %s event loop cancelled", conn_id)
                break
            except Exception as e:
                consecutive_errors += 1
                logger.exception("WebSocket %s error in event wait loop (%d/%d): %s", 
                               conn_id, consecutive_errors, max_consecutive_errors, e)
                
                # Try to recover by recreating subscriptions if needed
                if "queue" in str(e).lower() or "subscription" in str(e).lower():
                    try:
                        logger.warning("WebSocket %s attempting to recover subscriptions...", conn_id)
                        from ..services.event_broadcaster import broadcaster
                        # Recreate subscriptions
                        q_snapshot = await asyncio.wait_for(broadcaster.subscribe("snapshot_updated"), timeout=5.0)
                        q_token = await asyncio.wait_for(broadcaster.subscribe("token_updated"), timeout=5.0)
                        sync_queues["snapshot_updated"] = q_snapshot
                        sync_queues["token_updated"] = q_token
                        logger.info("WebSocket %s successfully recovered subscriptions", conn_id)
                        consecutive_errors = 0  # Reset error count on successful recovery
                        continue
                    except Exception as recovery_error:
                        logger.exception("WebSocket %s subscription recovery failed: %s", conn_id, recovery_error)
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("WebSocket %s too many consecutive errors, closing", conn_id)
                    break
                
                # Brief sleep to prevent tight error loops
                await asyncio.sleep(min(consecutive_errors, 5))
                
        except Exception as e:
            consecutive_errors += 1
            logger.exception("WebSocket %s unexpected error in main loop (%d/%d): %s", 
                           conn_id, consecutive_errors, max_consecutive_errors, e)
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error("WebSocket %s too many consecutive errors, closing", conn_id)
                break
                
            # Brief sleep to prevent tight error loops
            await asyncio.sleep(min(consecutive_errors, 5))


async def _send_snapshot_update(websocket: WebSocket, conn_id: int, payload):
    """Send snapshot update with timeout."""
    try:
        # Check if websocket is still connected before attempting to send
        if (websocket.client_state == websocket.client_state.DISCONNECTED or 
            websocket.client_state == websocket.client_state.CONNECTING or
            hasattr(websocket, '_close_sent') and websocket._close_sent):
            logger.warning("WebSocket %s is disconnected/closing, cannot send snapshot", conn_id)
            raise WebSocketDisconnect
        
        from ..services.sync_snapshot_service import get_latest_snapshot
        
        snapshot = get_latest_snapshot()
        if snapshot is None:
            # Send acknowledgment if no snapshot available
            ack_msg = {"event": "sync_ack", "data": payload, "timestamp": datetime.now().isoformat()}
            try:
                ack_text = await asyncio.to_thread(json.dumps, ack_msg)
            except Exception:
                ack_text = json.dumps(ack_msg)
            try:
                await asyncio.wait_for(
                    websocket.send_text(ack_text),
                    timeout=10.0,
                )
            except asyncio.TimeoutError:
                logger.warning("WebSocket %s ack send timeout", conn_id)
            except Exception as e:
                if isinstance(e, WebSocketDisconnect):
                    raise
                logger.exception("WebSocket %s error sending ack: %s", conn_id, e)
        else:
            # Filter snapshot to only live tokens for websocket (to reduce payload size)
            try:
                filtered_snapshot = copy.deepcopy(snapshot)
                if isinstance(filtered_snapshot, dict) and 'data' in filtered_snapshot:
                    tokens = filtered_snapshot['data'].get('tokens', [])
                    if tokens:
                        live_tokens = [t for t in tokens if t.get('is_live')]
                        filtered_snapshot['data']['tokens'] = live_tokens
                        # Update pagination if present
                        if 'pagination' in filtered_snapshot['data']:
                            filtered_snapshot['data']['pagination']['total'] = len(live_tokens)
                            filtered_snapshot['data']['pagination']['has_more'] = False
                        logger.debug("WebSocket %s filtered to %d live tokens (from %d total)", 
                                   conn_id, len(live_tokens), len(tokens))
                
                payload_text = await asyncio.to_thread(json.dumps, filtered_snapshot)
            except Exception:
                payload_text = json.dumps(snapshot)
            
            # Log the snapshot being sent
            tokens_count = len(snapshot.get('data', {}).get('tokens', [])) if isinstance(snapshot, dict) else 0
            payload_size = len(payload_text)
            logger.info("WebSocket %s sending updated snapshot to frontend (%d tokens, %d bytes)", 
                       conn_id, tokens_count, payload_size)
            
            # Check for large payloads that might cause issues
            if payload_size > 1024 * 1024:  # 1MB
                logger.warning("WebSocket %s very large payload (%d MB), this may cause connection issues", 
                             conn_id, payload_size // (1024 * 1024))
            elif payload_size > 500 * 1024:  # 500KB  
                logger.info("WebSocket %s large payload (%d KB), monitoring for issues", 
                           conn_id, payload_size // 1024)
            
            try:
                # Double-check websocket state right before sending
                if (websocket.client_state == websocket.client_state.DISCONNECTED or
                    hasattr(websocket, '_close_sent') and websocket._close_sent):
                    logger.warning("WebSocket %s closed while preparing to send", conn_id)
                    raise WebSocketDisconnect
                
                # Use longer timeout for large payloads
                send_timeout = 30.0 if payload_size > 500 * 1024 else 15.0
                send_start_time = time.time()
                
                await asyncio.wait_for(
                    websocket.send_text(payload_text),
                    timeout=send_timeout,
                )
                
                send_duration = time.time() - send_start_time
                logger.info("WebSocket %s successfully sent snapshot to frontend (%.2fs)", conn_id, send_duration)
                
                # Log slow sends that might indicate network issues
                if send_duration > 5.0:
                    logger.warning("WebSocket %s slow send detected (%.2fs for %d bytes)", 
                                 conn_id, send_duration, payload_size)
            except asyncio.TimeoutError:
                logger.warning("WebSocket %s snapshot send timeout (will retry)", conn_id)
            except Exception as e:
                if isinstance(e, WebSocketDisconnect):
                    raise
                # Handle the specific case where websocket is closed
                if "Cannot call \"send\" once a close message has been sent" in str(e):
                    logger.error("WebSocket %s connection unexpectedly closed during send (payload: %d bytes, client should still be connected!)", 
                               conn_id, payload_size)
                    raise WebSocketDisconnect
                logger.exception("WebSocket %s error sending snapshot (payload: %d bytes): %s", conn_id, payload_size, e)
            
    except asyncio.TimeoutError:
        logger.warning("WebSocket %s snapshot send timeout", conn_id)
        raise WebSocketDisconnect
    except (RuntimeError, WebSocketDisconnect, ConnectionResetError):
        logger.info("WebSocket %s disconnected during snapshot send", conn_id)
        raise WebSocketDisconnect


async def _send_heartbeat(websocket: WebSocket, conn_id: int):
    """Send heartbeat with timeout."""
    try:
        # Check websocket state before sending heartbeat
        if (websocket.client_state == websocket.client_state.DISCONNECTED or
            hasattr(websocket, '_close_sent') and websocket._close_sent):
            logger.warning("WebSocket %s is closed, cannot send heartbeat", conn_id)
            raise WebSocketDisconnect
            
        heartbeat_msg = {"event": "heartbeat", "timestamp": datetime.now().isoformat()}
        try:
            heartbeat_text = await asyncio.to_thread(json.dumps, heartbeat_msg)
        except Exception:
            heartbeat_text = json.dumps(heartbeat_msg)
        try:
            await asyncio.wait_for(
                websocket.send_text(heartbeat_text),
                timeout=10.0,
            )
            logger.debug("WebSocket %s heartbeat sent", conn_id)
        except asyncio.TimeoutError:
            logger.warning("WebSocket %s heartbeat timeout (will continue)", conn_id)
        except Exception as e:
            if isinstance(e, WebSocketDisconnect):
                raise
            # Handle the specific case where websocket is closed
            if "Cannot call \"send\" once a close message has been sent" in str(e):
                logger.warning("WebSocket %s connection closed during heartbeat", conn_id)
                raise WebSocketDisconnect
            logger.exception("WebSocket %s heartbeat failed: %s", conn_id, e)
            # For unexpected errors, raise to trigger cleanup
            raise
    except asyncio.TimeoutError:
        logger.warning("WebSocket %s overall timeout in event loop", conn_id)
        raise WebSocketDisconnect


async def _cleanup_connection(websocket: WebSocket, conn_id: int, sync_queue=None):
    """Clean up WebSocket connection and resources."""
    logger.info("WebSocket %s cleaning up", conn_id)
    
    # Clean up broadcaster subscription
    # support either a dict of queues or a single queue (legacy)
    if sync_queue is not None:
        try:
            from ..services.event_broadcaster import broadcaster
            if isinstance(sync_queue, dict):
                for name, q in sync_queue.items():
                    try:
                        await asyncio.wait_for(broadcaster.unsubscribe(name, q), timeout=3.0)
                        logger.info("WebSocket %s unsubscribed from %s (queue=%s)", conn_id, name, id(q))
                    except Exception:
                        logger.debug("WebSocket %s unsubscribe error for %s", conn_id, name)
            else:
                try:
                    await asyncio.wait_for(broadcaster.unsubscribe("snapshot_updated", sync_queue), timeout=3.0)
                except Exception:
                    logger.debug("WebSocket %s unsubscribe error (legacy)", conn_id)
            logger.debug("WebSocket %s unsubscribed from broadcaster", conn_id)
        except Exception as e:
            logger.exception("WebSocket %s unsubscribe error: %s", conn_id, e)
    
    # Remove from active connections
    ws_active_connections.discard(conn_id)
    
    # Close WebSocket connection
    try:
        if websocket.client_state != websocket.client_state.DISCONNECTED:
            await asyncio.wait_for(websocket.close(), timeout=3.0)
    except Exception as e:
        logger.debug("WebSocket %s close error (expected): %s", conn_id, e)
    
    logger.info("WebSocket %s cleanup completed", conn_id)