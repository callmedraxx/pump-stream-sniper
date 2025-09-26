# import asyncio
# import json
# import logging
# import copy
# import time
# import msgpack
# from datetime import datetime, timedelta
# from typing import Any, Dict, List, Optional

# from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
# from fastapi.responses import StreamingResponse
# from sqlalchemy import and_, asc, desc, or_
# from sqlalchemy.orm import Session

# from ..models import Token, get_db
# logger = logging.getLogger(__name__)

# router = APIRouter()


# async def _safe_send_bytes(websocket: WebSocket, data: bytes, conn_id: int, timeout: float = 5.0):
#     """Safely send bytes over websocket, mapping runtime send-errors to WebSocketDisconnect.

#     This centralizes handling of the starlette RuntimeError "Cannot call \"send\" once a close
#     message has been sent." and ensures callers receive WebSocketDisconnect for cleanup.
#     """
#     try:
#         await asyncio.wait_for(websocket.send_bytes(data), timeout=timeout)
#         return True
#     except asyncio.TimeoutError:
#         logger.warning("WebSocket %s send timeout (timeout=%.1fs)", conn_id, timeout)
#         raise WebSocketDisconnect
#     except RuntimeError as e:
#         # Map the specific starlette message to WebSocketDisconnect so callers clean up.
#         if "Cannot call \"send\" once a close message has been sent" in str(e):
#             logger.info("WebSocket %s attempted send after close message; treating as disconnect", conn_id)
#             raise WebSocketDisconnect
#         raise
#     except WebSocketDisconnect:
#         raise
#     except Exception as e:
#         logger.exception("WebSocket %s unexpected send error: %s", conn_id, e)
#         # Convert unexpected send errors into a disconnect to trigger cleanup upstream
#         raise WebSocketDisconnect from e

# # Global state for SSE connections
# active_connections = set()
# # Global state for WebSocket connections
# ws_active_connections = set()


# class TokenSortService:
#     """Service for sorting and filtering tokens with database optimization"""

#     @staticmethod
#     def get_sort_column(sort_by: str, time_period: Optional[str] = None):
#         """Get the appropriate column for sorting"""
#         sort_mappings = {
#             "age": Token.age,
#             "mcap": Token.mcap,
#             "viewers": Token.viewers,
#             "creator": Token.creator,
#             "txns": getattr(Token, f'txns_{time_period or "24h"}'),
#             "volume": getattr(Token, f'volume_{time_period or "24h"}'),
#             "traders": getattr(Token, f'traders_{time_period or "24h"}'),
#         }
#         return sort_mappings.get(sort_by, Token.age)

#     @staticmethod
#     def apply_sorting(
#         query, sort_by: str, sort_order: str, time_period: Optional[str] = None
#     ):
#         """Apply sorting to the query"""
#         column = TokenSortService.get_sort_column(sort_by, time_period)

#         if sort_order == "desc":
#             return query.order_by(desc(column))
#         else:
#             return query.order_by(asc(column))

#     @staticmethod
#     def apply_filters(query, filters: Dict[str, Any]):
#         """Apply filters to the query"""
#         if filters.get("is_live") is not None:
#             query = query.filter(Token.is_live == filters["is_live"])

#         if filters.get("is_active") is not None:
#             query = query.filter(Token.is_active == filters["is_active"])

#         if filters.get("nsfw") is not None:
#             query = query.filter(Token.nsfw == filters["nsfw"])

#         if filters.get("creator"):
#             query = query.filter(Token.creator == filters["creator"])

#         if filters.get("min_mcap"):
#             query = query.filter(Token.mcap >= filters["min_mcap"])

#         if filters.get("max_mcap"):
#             query = query.filter(Token.mcap <= filters["max_mcap"])

#         return query


# @router.get("/tokens")
# async def get_tokens(
#     # Sorting parameters
#     sort_by: str = Query(
#         "age",
#         description="Sort field: age, mcap, viewers, txns, volume, traders, creator",
#     ),
#     sort_order: str = Query("desc", description="Sort order: asc or desc"),
#     time_period: Optional[str] = Query(
#         None, description="Time period for txns/volume/traders: 5m, 1h, 6h, 24h"
#     ),
#     # Filtering parameters
#     is_live: Optional[bool] = Query(None, description="Filter by live status"),
#     is_active: Optional[bool] = Query(None, description="Filter by active status"),
#     nsfw: Optional[bool] = Query(None, description="Filter by NSFW content"),
#     creator: Optional[str] = Query(None, description="Filter by creator address"),
#     min_mcap: Optional[float] = Query(None, description="Minimum market cap filter"),
#     max_mcap: Optional[float] = Query(None, description="Maximum market cap filter"),
#     # Pagination
#     limit: int = Query(2000, description="Number of tokens to return", ge=1, le=1000),
#     offset: int = Query(0, description="Offset for pagination", ge=0),
#     # Response format
#     format: str = Query("full", description="Response format: full, compact, minimal"),
# ):
#     """
#     Get sorted and filtered tokens from database with pagination
#     """
#     try:
#         db = next(get_db())

#         # Build base query
#         query = db.query(Token)

#         # Apply filters
#         filters = {
#             "is_live": is_live,
#             "is_active": is_active,
#             "nsfw": nsfw,
#             "creator": creator,
#             "min_mcap": min_mcap,
#             "max_mcap": max_mcap,
#         }
#         query = TokenSortService.apply_filters(query, filters)

#         # Apply sorting
#         query = TokenSortService.apply_sorting(query, sort_by, sort_order, time_period)

#         # Apply pagination
#         total_count = query.count()
#         tokens = query.offset(offset).limit(limit).all()

#         # Format response based on requested format
#         if format == "minimal":
#             token_data = [
#                 {
#                     "mint_address": t.mint_address,
#                     "name": t.name,
#                     "symbol": t.symbol,
#                     "mcap": t.mcap,
#                     "age": t.age.isoformat() if t.age else None,
#                     "viewers": t.viewers,
#                     "is_live": t.is_live,
#                 }
#                 for t in tokens
#             ]
#         elif format == "compact":
#             token_data = []
#             for t in tokens:
#                 token_dict = {
#                     "mint_address": t.mint_address,
#                     "name": t.name,
#                     "symbol": t.symbol,
#                     "image_url": t.image_url,
#                     "mcap": t.mcap,
#                     "ath": t.ath,
#                     "progress": t.progress,
#                     "age": t.age.isoformat() if t.age else None,
#                     "viewers": t.viewers,
#                     "creator": t.creator,
#                     "is_live": t.is_live,
#                     "is_active": t.is_active,
#                 }

#                 # Add time-specific data based on time_period only
#                 if time_period:
#                     # Only include the specific time period data
#                     token_dict.update({
#                         f"volume_{time_period}": getattr(t, f"volume_{time_period}"),
#                         f"txns_{time_period}": getattr(t, f"txns_{time_period}"),
#                         f"traders_{time_period}": getattr(t, f"traders_{time_period}"),
#                     })
#                 else:
#                     # Include all time periods (default behavior)
#                     token_dict.update({
#                         "volume_24h": t.volume_24h,
#                         "txns_24h": t.txns_24h,
#                         "traders_24h": t.traders_24h,
#                     })

#                 # Add creator holding data
#                 token_dict.update({
#                     "creator_holding_percentage": t.creator_holding_percentage,
#                     "creator_is_top_holder": t.creator_is_top_holder,
#                 })

#                 token_data.append(token_dict)
#         else:  # full format
#             token_data = []
#             for t in tokens:
#                 token_dict = {
#                     "id": t.id,
#                     "mint_address": t.mint_address,
#                     "name": t.name,
#                     "symbol": t.symbol,
#                     "image_url": t.image_url,
#                     "age": t.age.isoformat() if t.age else None,
#                     "mcap": t.mcap,
#                     "ath": t.ath,
#                     "creator": t.creator,
#                     "total_supply": t.total_supply,
#                     "pump_swap_pool": t.pump_swap_pool,
#                     "viewers": t.viewers,
#                     "progress": t.progress,
#                     "liquidity": t.liquidity,
#                     "is_live": t.is_live,
#                     "is_active": t.is_active,
#                     "nsfw": t.nsfw,
#                     "social_links": t.social_links,
#                 }

#                 # Handle time-specific data for price_changes, traders, volume, txns
#                 if time_period:
#                     # Only include the specific time period data
#                     token_dict["price_changes"] = {
#                         time_period: getattr(t, f"price_change_{time_period}")
#                     }
#                     token_dict["traders"] = {
#                         time_period: getattr(t, f"traders_{time_period}")
#                     }
#                     token_dict["volume"] = {
#                         time_period: getattr(t, f"volume_{time_period}")
#                     }
#                     token_dict["txns"] = {
#                         time_period: getattr(t, f"txns_{time_period}")
#                     }
#                 else:
#                     # Include all time periods (default behavior)
#                     token_dict["price_changes"] = {
#                         "5m": t.price_change_5m,
#                         "1h": t.price_change_1h,
#                         "6h": t.price_change_6h,
#                         "24h": t.price_change_24h,
#                     }
#                     token_dict["traders"] = {
#                         "5m": t.traders_5m,
#                         "1h": t.traders_1h,
#                         "6h": t.traders_6h,
#                         "24h": t.traders_24h,
#                     }
#                     token_dict["volume"] = {
#                         "5m": t.volume_5m,
#                         "1h": t.volume_1h,
#                         "6h": t.volume_6h,
#                         "24h": t.volume_24h,
#                     }
#                     token_dict["txns"] = {
#                         "5m": t.txns_5m,
#                         "1h": t.txns_1h,
#                         "6h": t.txns_6h,
#                         "24h": t.txns_24h,
#                     }

#                 # Add remaining fields
#                 token_dict.update({
#                     "pool_info": {
#                         "raydium_pool": t.raydium_pool,
#                         "virtual_sol_reserves": t.virtual_sol_reserves,
#                         "real_sol_reserves": t.real_sol_reserves,
#                         "virtual_token_reserves": t.virtual_token_reserves,
#                         "real_token_reserves": t.real_token_reserves,
#                         "complete": t.complete,
#                     },
#                     "activity": {
#                         "reply_count": t.reply_count,
#                         "last_reply": (
#                             t.last_reply.isoformat() if t.last_reply else None
#                         ),
#                         "last_trade_timestamp": (
#                             t.last_trade_timestamp.isoformat()
#                             if t.last_trade_timestamp
#                             else None
#                         ),
#                     },
#                     "holders": {
#                         "top_holders": t.top_holders,
#                         "creator_holding_amount": t.creator_holding_amount,
#                         "creator_holding_percentage": t.creator_holding_percentage,
#                         "creator_is_top_holder": t.creator_is_top_holder,
#                     },
#                     "timestamps": {
#                         "created_at": t.created_at.isoformat(),
#                         "updated_at": t.updated_at.isoformat(),
#                     },
#                 })

#                 token_data.append(token_dict)

#         return {
#             "success": True,
#             "data": {
#                 "tokens": token_data,
#                 "pagination": {
#                     "total": total_count,
#                     "limit": limit,
#                     "offset": offset,
#                     "has_more": (offset + limit) < total_count,
#                 },
#                 "sorting": {
#                     "sort_by": sort_by,
#                     "sort_order": sort_order,
#                     "time_period": time_period,
#                 },
#                 "filters": {k: v for k, v in filters.items() if v is not None},
#             },
#         }

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")



# @router.websocket("/ws/tokens_stream")
# async def websocket_tokens_stream(websocket: WebSocket):
#     """WebSocket endpoint that streams full tokens snapshot on DB changes."""
#     conn_id = id(websocket)
#     sync_queues = {}
    
#     try:
#         await websocket.accept()
#         ws_active_connections.add(conn_id)
#         logger.info("WebSocket %s connected to tokens_stream", conn_id)
#     except WebSocketDisconnect:
#         logger.info("WebSocket connection closed before accept()")
#         return
#     except Exception as e:
#         logger.exception("WebSocket accept failed: %s", e)
#         return

#     from ..services.event_broadcaster import broadcaster
#     from ..services.sync_snapshot_service import get_latest_snapshot

#     try:
#         # Subscribe to individual token updates for real-time streaming
#         q_token = await asyncio.wait_for(broadcaster.subscribe("token_updated"), timeout=5.0)
#         # Subscribe to mcap_update events for compact mcap/ath/progress notifications
#         q_mcap = await asyncio.wait_for(broadcaster.subscribe("mcap_update"), timeout=5.0)
#         sync_queues = {"token_updated": q_token}
#         sync_queues = {"token_updated": q_token, "mcap_update": q_mcap}
#         logger.info("WebSocket %s subscribed to token_updated and mcap_update events (queues: %s,%s)", 
#                    conn_id, id(q_token), id(q_mcap))
#     except asyncio.TimeoutError:
#         logger.error("WebSocket %s subscription timeout", conn_id)
#         await _cleanup_connection(websocket, conn_id, None)
#         return
#     except Exception as e:
#         logger.exception("WebSocket %s failed to subscribe: %s", conn_id, e)
#         await _cleanup_connection(websocket, conn_id, None)
#         return

#     try:
#         # Send cached snapshot immediately
#         await _send_initial_snapshot(websocket, conn_id, sync_queues)

#         # Monitor connection health
#         logger.info("WebSocket %s starting main event loop, client_state=%s", conn_id, websocket.client_state)

#         # Main event loop with better error handling (listening to both queues)
#         await _handle_websocket_events(websocket, conn_id, sync_queues)

#     except WebSocketDisconnect:
#         logger.info("WebSocket %s disconnected by client", conn_id)
#     except Exception as e:
#         logger.exception("WebSocket %s unexpected error: %s", conn_id, e)
#     finally:
#         # cleanup using the dict of queues (or None)
#         try:
#             await _cleanup_connection(websocket, conn_id, sync_queues)
#         except Exception:
#             await _cleanup_connection(websocket, conn_id, None)


# async def _send_initial_snapshot(websocket: WebSocket, conn_id: int, sync_queues: dict):
#     """Send initial snapshot and drain queue to prevent double-sends."""
#     try:
#         from ..services.sync_snapshot_service import get_latest_snapshot

#         snapshot = get_latest_snapshot()
#         if snapshot is not None:
#             # Deepcopy to avoid mutating the cached snapshot
#             snapshot_to_send = copy.deepcopy(snapshot)
#             tokens = snapshot_to_send.get('data', {}).get('tokens', []) or []
#             # Filter only tokens with is_live == True
#             filtered = [t for t in tokens if t.get('is_live')]
#             snapshot_to_send['data']['tokens'] = filtered
#             # Update pagination total/has_more if present
#             try:
#                 if 'pagination' in snapshot_to_send.get('data', {}):
#                     snapshot_to_send['data']['pagination']['total'] = len(filtered)
#                     snapshot_to_send['data']['pagination']['has_more'] = False
#             except Exception:
#                 pass

#             logger.info("WebSocket %s sending INITIAL snapshot to frontend (%d live tokens)", 
#                        conn_id, len(filtered))

#             # Wrap snapshot in event structure for frontend compatibility
#             snapshot_event = {
#                 "event": "tokens_update",
#                 "timestamp": datetime.now().isoformat(),
#                 "data": snapshot_to_send
#             }
            
#             # Serialize to binary using msgpack
#             try:
#                 payload_bytes = await asyncio.to_thread(msgpack.packb, snapshot_event, use_bin_type=True)
#             except Exception:
#                 # Fallback to direct serialization if thread offload fails
#                 payload_bytes = msgpack.packb(snapshot_event, use_bin_type=True)

#             logger.info("WebSocket %s initial snapshot serialized (%d bytes), sending to frontend", 
#                        conn_id, len(payload_bytes))

#             # Allow more time for large payloads; don't disconnect on a single slow send
#             try:
#                 await _safe_send_bytes(websocket, payload_bytes, conn_id, timeout=15.0)
#                 logger.info("WebSocket %s successfully sent INITIAL snapshot to frontend", conn_id)
#             except asyncio.TimeoutError:
#                 logger.warning("WebSocket %s initial snapshot send timeout (will retry later)", conn_id)
#             except Exception as e:
#                 # Re-raise disconnect-related exceptions so upstream handles cleanup
#                 if isinstance(e, WebSocketDisconnect):
#                     raise
#                 logger.exception("WebSocket %s error sending initial snapshot: %s", conn_id, e)
            
#             # Drain queued events to prevent duplicates from both queues
#             drained = 0
#             try:
#                 while drained < 200:  # Prevent infinite loop across two queues
#                     empty = True
#                     for q in sync_queues.values():
#                         try:
#                             q.get_nowait()
#                             drained += 1
#                             empty = False
#                         except asyncio.QueueEmpty:
#                             continue
#                     if empty:
#                         break
#             except Exception:
#                 pass

#             if drained > 0:
#                 logger.debug("WebSocket %s drained %d queued events", conn_id, drained)
                
#     except asyncio.TimeoutError:
#         logger.warning("WebSocket %s initial snapshot send timeout", conn_id)
#         raise WebSocketDisconnect
#     except (RuntimeError, WebSocketDisconnect, ConnectionResetError):
#         logger.info("WebSocket %s disconnected during initial snapshot", conn_id)
#         raise WebSocketDisconnect
#     except Exception as e:
#         logger.exception("WebSocket %s initial snapshot error: %s", conn_id, e)
#         # Don't raise - continue to main loop


# async def _handle_websocket_events(websocket: WebSocket, conn_id: int, sync_queues: dict):
#     """Main event loop with improved error handling and individual token updates."""
#     import time
    
#     consecutive_errors = 0
#     max_consecutive_errors = 5
#     last_send_time = 0.0
#     loop_iterations = 0
#     last_activity_time = time.time()
#     pending_token_updates = []  # Buffer individual token updates
    
#     while True:
#         loop_iterations += 1
#         current_time = time.time()
        
#         # Check for websocket health - if no activity for too long, break
#         if current_time - last_activity_time > 300:  # 5 minutes without activity
#             logger.error("WebSocket %s no activity for 5 minutes, assuming dead connection", conn_id)
#             break
            
#         try:
#             # Wait for sync event with timeout for heartbeat
#             try:
#                 # Wait for either queue (snapshot_updated or token_updated)
#                 logger.debug("WebSocket %s waiting for events from queues... (iteration %d)", conn_id, loop_iterations)
                
#                 # Validate queues are still alive
#                 for name, q in sync_queues.items():
#                     if q is None:
#                         logger.error("WebSocket %s queue %s is None, reconnecting...", conn_id, name)
#                         raise RuntimeError(f"Queue {name} is None")
                
#                 async def _get_with_event(name, q):
#                     return {"__event_name": name, "payload": await q.get()}

#                 tasks = [asyncio.create_task(_get_with_event(ev, sync_queues[ev])) for ev in sync_queues]
#                 done, pending_tasks = await asyncio.wait(
#                     tasks,
#                     return_when=asyncio.FIRST_COMPLETED,
#                     timeout=25.0,
#                 )

#                 if not done:
#                     # Timeout - send heartbeat and flush pending updates if any
#                     logger.debug("WebSocket %s event timeout after 25s, sending heartbeat", conn_id)
                    
#                     # Check if websocket is actually still alive
#                     if not _is_websocket_connected(websocket):
#                         logger.info("WebSocket %s detected disconnection during timeout check", conn_id)
#                         break
                    
#                     # Flush any pending token updates on timeout (only if still connected)
#                     if (pending_token_updates and time.time() - last_send_time >= 0.5 and
#                         _is_websocket_connected(websocket)):
#                         try:
#                             await _send_batched_token_updates(websocket, conn_id, pending_token_updates)
#                             last_send_time = time.time()
#                             pending_token_updates = []
#                         except (WebSocketDisconnect, RuntimeError) as e:
#                             logger.info("WebSocket %s disconnected while flushing updates: %s", conn_id, e)
#                             break
#                         except Exception as e:
#                             logger.exception("WebSocket %s error flushing batched token updates: %s", conn_id, e)
                        
#                     await _send_heartbeat(websocket, conn_id)
#                     consecutive_errors = max(0, consecutive_errors - 1)
#                     last_activity_time = time.time()
#                 else:
#                     logger.debug("WebSocket %s received %d completed events", conn_id, len(done))
#                     # Process completed futures
#                     for fut in done:
#                         try:
#                             res = fut.result()
#                         except Exception as e:
#                             logger.warning("WebSocket %s error getting result from future: %s", conn_id, e)
#                             res = None

#                         if res is None:
#                             logger.warning("WebSocket %s received None payload from queue", conn_id)
#                             continue

#                         consecutive_errors = 0
#                         last_activity_time = time.time()

#                         evt_name = res.get('__event_name') if isinstance(res, dict) else None
#                         payload = res.get('payload') if isinstance(res, dict) else res

#                         # Handle mcap_update events immediately and separately
#                         if evt_name == 'mcap_update' or (isinstance(payload, dict) and payload.get('type') == 'mcap_update'):
#                             # payload is expected to contain mint_address, mcap, progress, ath, is_live
#                             try:
#                                 mcap_payload = payload if evt_name == 'mcap_update' else payload.get('data', payload)
#                                 event_msg = {
#                                     "event": "mcap_update",
#                                     "timestamp": datetime.now().isoformat(),
#                                     "data": mcap_payload,
#                                 }
#                                 try:
#                                     bytes_msg = await asyncio.to_thread(msgpack.packb, event_msg, use_bin_type=True)
#                                 except Exception:
#                                     bytes_msg = msgpack.packb(event_msg, use_bin_type=True)
#                                 await _safe_send_bytes(websocket, bytes_msg, conn_id, timeout=5.0)
#                                 logger.info("WebSocket %s sent mcap_update for %s (%d bytes)", conn_id, mcap_payload.get('mint_address'), len(bytes_msg))
#                             except Exception as e:
#                                 logger.exception("WebSocket %s failed to send mcap_update: %s", conn_id, e)
#                             continue

#                         # Handle different event types for other payloads
#                         if isinstance(payload, dict):
#                             event_type = payload.get('type', 'unknown')

#                             if event_type == 'token_updated' or event_type == 'token_created':
#                                 # Individual token update - add to buffer
#                                 #pending_token_updates.append(payload)
#                                 logger.debug("WebSocket %s buffered token update for %s (buffer size: %d)", 
#                                            conn_id, payload.get('mint_address', 'unknown'), len(pending_token_updates))
                                
#                             elif event_type == 'bulk_token_updated':
#                                 # Bulk updates are handled by snapshot service internally.
#                                 # For websocket clients we do NOT forward full snapshot updates.
#                                 # Keep this logged for observability but intentionally ignore.
#                                 logger.info("WebSocket %s received bulk token update - ignoring full snapshot for websocket clients", conn_id)
                                
#                             elif 'snapshot' in payload:
#                                 # This is a snapshot_updated event from sync_snapshot_service
#                                 # We intentionally DO NOT forward full snapshot updates to websocket clients.
#                                 # Clients receive the initial full snapshot on connect and then rely on
#                                 # individual token updates to stay in sync. Log and drop the event.
#                                 logger.debug("WebSocket %s received snapshot update event - dropping (websocket clients only receive initial snapshot)", conn_id)
#                                 # Do not call _send_snapshot_update; drop it silently.
#                                 continue
                                
#                             else:
#                                 # Other events (like sync_completed stats) - just log and ignore
#                                 logger.debug("WebSocket %s received %s event - ignoring", conn_id, 
#                                            event_type if event_type != 'unknown' else type(payload).__name__)
                    
#                     # Send batched token updates - max batch size of 2, otherwise send per 1 after timeout
#                     if pending_token_updates:
#                         current_time = time.time()
#                         time_since_last_send = current_time - last_send_time
#                         buffer_size = len(pending_token_updates)
                        
#                         should_send = (
#                             buffer_size >= 2 or  # Send immediately when batch reaches 2 tokens
#                             (buffer_size >= 1 and time_since_last_send >= 0.5)  # Send single token after timeout
#                         )
                        
#                         if should_send and _is_websocket_connected(websocket):
#                             try:
#                                 await _send_batched_token_updates(websocket, conn_id, pending_token_updates)
#                                 last_send_time = time.time()
#                                 pending_token_updates = []
#                             except (WebSocketDisconnect, RuntimeError) as e:
#                                 logger.info("WebSocket %s disconnected while sending updates: %s", conn_id, e)
#                                 break
#                             except Exception as e:
#                                 logger.exception("WebSocket %s error sending batched updates: %s", conn_id, e)
                    
#                     # Cancel any pending gets to avoid leaked tasks
#                     for p in pending_tasks:
#                         try:
#                             p.cancel()
#                         except Exception:
#                             pass
                            
#             except WebSocketDisconnect:
#                 logger.info("WebSocket %s client disconnected", conn_id)
#                 break
#             except asyncio.CancelledError:
#                 logger.info("WebSocket %s event loop cancelled", conn_id)
#                 break
#             except Exception as e:
#                 consecutive_errors += 1
#                 logger.exception("WebSocket %s error in event wait loop (%d/%d): %s", 
#                                conn_id, consecutive_errors, max_consecutive_errors, e)
                
#                 # Try to recover by recreating subscriptions if needed
#                 if "queue" in str(e).lower() or "subscription" in str(e).lower():
#                     try:
#                         logger.warning("WebSocket %s attempting to recover subscriptions...", conn_id)
#                         from ..services.event_broadcaster import broadcaster
#                         # Recreate token update subscription
#                         q_token = await asyncio.wait_for(broadcaster.subscribe("token_updated"), timeout=5.0)
#                         sync_queues["token_updated"] = q_token
#                         logger.info("WebSocket %s successfully recovered token_updated subscription", conn_id)
#                         consecutive_errors = 0  # Reset error count on successful recovery
#                         continue
#                     except Exception as recovery_error:
#                         logger.exception("WebSocket %s subscription recovery failed: %s", conn_id, recovery_error)
                
#                 if consecutive_errors >= max_consecutive_errors:
#                     logger.error("WebSocket %s too many consecutive errors, closing", conn_id)
#                     break
                
#                 # Brief sleep to prevent tight error loops
#                 await asyncio.sleep(min(consecutive_errors, 5))
                
#         except Exception as e:
#             consecutive_errors += 1
#             logger.exception("WebSocket %s unexpected error in main loop (%d/%d): %s", 
#                            conn_id, consecutive_errors, max_consecutive_errors, e)
            
#             if consecutive_errors >= max_consecutive_errors:
#                 logger.error("WebSocket %s too many consecutive errors, closing", conn_id)
#                 break
                
#             # Brief sleep to prevent tight error loops
#             await asyncio.sleep(min(consecutive_errors, 5))

# def _is_websocket_connected(websocket: WebSocket) -> bool:
#     """Check if WebSocket is still connected and usable."""
#     try:
#         return (websocket.client_state == websocket.client_state.CONNECTED and 
#                 not (hasattr(websocket, '_close_sent') and websocket._close_sent) and
#                 not (hasattr(websocket, 'connection_state') and websocket.connection_state != 'CONNECTED'))
#     except Exception:
#         return False


# async def _send_batched_token_updates(websocket: WebSocket, conn_id: int, token_updates: list):
#     """Send batched individual token updates in binary format."""
#     try:
#         # Check if websocket is still connected before sending
#         if not _is_websocket_connected(websocket):
#             logger.debug("WebSocket %s is disconnected/closing, skipping token updates", conn_id)
#             raise WebSocketDisconnect
                 
#         if not token_updates:
#             return
                 
#         # Prepare the batch message
#         batch_message = {
#             "event": "token_updates_batch",
#             "timestamp": datetime.now().isoformat(),
#             "data": {
#                 "updates": token_updates,  # Now token_updates contains the minimal payloads directly
#                 "batch_size": len(token_updates)
#             }
#         }
                 
#         # Serialize to binary using msgpack
#         try:
#             message_bytes = await asyncio.to_thread(msgpack.packb, batch_message, use_bin_type=True)
#         except Exception:
#             message_bytes = msgpack.packb(batch_message, use_bin_type=True)
                 
#         await _safe_send_bytes(websocket, message_bytes, conn_id, timeout=5.0)
#         logger.info("WebSocket %s sent token updates batch (%d tokens, %d bytes)",
#                     conn_id, len(token_updates), len(message_bytes))
             
#     except asyncio.TimeoutError:
#         logger.warning("WebSocket %s token updates batch send timeout", conn_id)
#         raise WebSocketDisconnect
#     except (RuntimeError, WebSocketDisconnect, ConnectionResetError):
#         raise
#     except Exception as e:
#         logger.exception("WebSocket %s error sending token updates batch: %s", conn_id, e)
#         raise WebSocketDisconnect


# async def _send_snapshot_update(websocket: WebSocket, conn_id: int, payload):
#     """Send snapshot update with timeout."""
#     try:
#         # Check if websocket is still connected before attempting to send
#         if (websocket.client_state == websocket.client_state.DISCONNECTED or 
#             websocket.client_state == websocket.client_state.CONNECTING or
#             hasattr(websocket, '_close_sent') and websocket._close_sent):
#             logger.warning("WebSocket %s is disconnected/closing, cannot send snapshot", conn_id)
#             raise WebSocketDisconnect
        
#         from ..services.sync_snapshot_service import get_latest_snapshot
        
#         snapshot = get_latest_snapshot()
#         if snapshot is None:
#             # Send acknowledgment if no snapshot available
#             ack_msg = {"event": "sync_ack", "data": payload, "timestamp": datetime.now().isoformat()}
#             try:
#                 ack_bytes = await asyncio.to_thread(msgpack.packb, ack_msg, use_bin_type=True)
#             except Exception:
#                 ack_bytes = msgpack.packb(ack_msg, use_bin_type=True)
#             try:
#                 await _safe_send_bytes(websocket, ack_bytes, conn_id, timeout=10.0)
#             except asyncio.TimeoutError:
#                 logger.warning("WebSocket %s ack send timeout", conn_id)
#             except Exception as e:
#                 if isinstance(e, WebSocketDisconnect):
#                     raise
#                 logger.exception("WebSocket %s error sending ack: %s", conn_id, e)
#         else:
#             # Filter snapshot to only live tokens for websocket (to reduce payload size)
#             try:
#                 filtered_snapshot = copy.deepcopy(snapshot)
#                 if isinstance(filtered_snapshot, dict) and 'data' in filtered_snapshot:
#                     tokens = filtered_snapshot['data'].get('tokens', [])
#                     if tokens:
#                         live_tokens = [t for t in tokens if t.get('is_live')]
#                         filtered_snapshot['data']['tokens'] = live_tokens
#                         # Update pagination if present
#                         if 'pagination' in filtered_snapshot['data']:
#                             filtered_snapshot['data']['pagination']['total'] = len(live_tokens)
#                             filtered_snapshot['data']['pagination']['has_more'] = False
#                         logger.debug("WebSocket %s filtered to %d live tokens (from %d total)", 
#                                    conn_id, len(live_tokens), len(tokens))
                
#                 payload_bytes = await asyncio.to_thread(msgpack.packb, filtered_snapshot, use_bin_type=True)
#             except Exception:
#                 payload_bytes = msgpack.packb(snapshot, use_bin_type=True)
            
#             # Wrap in event structure for frontend compatibility
#             snapshot_event = {
#                 "event": "snapshot_updated", 
#                 "timestamp": datetime.now().isoformat(),
#                 "data": filtered_snapshot if 'filtered_snapshot' in locals() else snapshot
#             }
            
#             try:
#                 event_bytes = await asyncio.to_thread(msgpack.packb, snapshot_event, use_bin_type=True)
#             except Exception:
#                 event_bytes = msgpack.packb(snapshot_event, use_bin_type=True)
            
#             # Log the snapshot being sent
#             tokens_count = len(snapshot.get('data', {}).get('tokens', [])) if isinstance(snapshot, dict) else 0
#             payload_size = len(event_bytes)
#             logger.info("WebSocket %s sending updated snapshot to frontend (%d tokens, %d bytes)", 
#                        conn_id, tokens_count, payload_size)
            
#             # Check for large payloads that might cause issues
#             if payload_size > 1024 * 1024:  # 1MB
#                 logger.warning("WebSocket %s very large payload (%d MB), this may cause connection issues", 
#                              conn_id, payload_size // (1024 * 1024))
#             elif payload_size > 500 * 1024:  # 500KB  
#                 logger.info("WebSocket %s large payload (%d KB), monitoring for issues", 
#                            conn_id, payload_size // 1024)
            
#             try:
#                 # Double-check websocket state right before sending
#                 if (websocket.client_state == websocket.client_state.DISCONNECTED or
#                     hasattr(websocket, '_close_sent') and websocket._close_sent):
#                     logger.warning("WebSocket %s closed while preparing to send", conn_id)
#                     raise WebSocketDisconnect
                
#                 # Use longer timeout for large payloads
#                 send_timeout = 30.0 if payload_size > 500 * 1024 else 15.0
#                 send_start_time = time.time()
                
#                 await _safe_send_bytes(websocket, event_bytes, conn_id, timeout=send_timeout)
                
#                 send_duration = time.time() - send_start_time
#                 logger.info("WebSocket %s successfully sent snapshot to frontend (%.2fs)", conn_id, send_duration)
                
#                 # Log slow sends that might indicate network issues
#                 if send_duration > 5.0:
#                     logger.warning("WebSocket %s slow send detected (%.2fs for %d bytes)", 
#                                  conn_id, send_duration, payload_size)
#             except asyncio.TimeoutError:
#                 logger.warning("WebSocket %s snapshot send timeout (will retry)", conn_id)
#             except Exception as e:
#                 if isinstance(e, WebSocketDisconnect):
#                     raise
#                 # Handle the specific case where websocket is closed
#                 if "Cannot call \"send\" once a close message has been sent" in str(e):
#                     logger.error("WebSocket %s connection unexpectedly closed during send (payload: %d bytes, client should still be connected!)", 
#                                conn_id, payload_size)
#                     raise WebSocketDisconnect
#                 logger.exception("WebSocket %s error sending snapshot (payload: %d bytes): %s", conn_id, payload_size, e)
            
#     except asyncio.TimeoutError:
#         logger.warning("WebSocket %s snapshot send timeout", conn_id)
#         raise WebSocketDisconnect
#     except (RuntimeError, WebSocketDisconnect, ConnectionResetError):
#         logger.info("WebSocket %s disconnected during snapshot send", conn_id)
#         raise WebSocketDisconnect


# async def _send_heartbeat(websocket: WebSocket, conn_id: int):
#     """Send heartbeat with timeout."""
#     try:
#         # Check websocket state before sending heartbeat
#         if not _is_websocket_connected(websocket):
#             logger.debug("WebSocket %s is closed/closing, cannot send heartbeat", conn_id)
#             raise WebSocketDisconnect
                     
#         heartbeat_msg = {"event": "heartbeat", "timestamp": datetime.now().isoformat()}
#         try:
#             heartbeat_bytes = await asyncio.to_thread(msgpack.packb, heartbeat_msg, use_bin_type=True)
#         except Exception:
#             heartbeat_bytes = msgpack.packb(heartbeat_msg, use_bin_type=True)
            
#         try:
#             await _safe_send_bytes(websocket, heartbeat_bytes, conn_id, timeout=10.0)
#             logger.debug("WebSocket %s heartbeat sent", conn_id)
#         except asyncio.TimeoutError:
#             logger.warning("WebSocket %s heartbeat timeout (will continue)", conn_id)
#         except Exception as e:
#             if isinstance(e, WebSocketDisconnect):
#                 raise
#             # Handle the specific case where websocket is closed
#             if ("Cannot call \"send\" once a close message has been sent" in str(e) or
#                 "Connection closed" in str(e) or "WebSocket is closed" in str(e)):
#                 logger.debug("WebSocket %s connection closed during heartbeat", conn_id)
#                 raise WebSocketDisconnect
#             logger.exception("WebSocket %s heartbeat failed: %s", conn_id, e)
#             # For unexpected errors, raise to trigger cleanup
#             raise
#     except asyncio.TimeoutError:
#         logger.warning("WebSocket %s overall timeout in event loop", conn_id)
#         raise WebSocketDisconnect


# async def _cleanup_connection(websocket: WebSocket, conn_id: int, sync_queue=None):
#     """Clean up WebSocket connection and resources."""
#     logger.info("WebSocket %s cleaning up", conn_id)
    
#     # Clean up broadcaster subscription
#     # support either a dict of queues or a single queue (legacy)
#     if sync_queue is not None:
#         try:
#             from ..services.event_broadcaster import broadcaster
#             if isinstance(sync_queue, dict):
#                 for name, q in sync_queue.items():
#                     try:
#                         await asyncio.wait_for(broadcaster.unsubscribe(name, q), timeout=3.0)
#                         logger.info("WebSocket %s unsubscribed from %s (queue=%s)", conn_id, name, id(q))
#                     except Exception:
#                         logger.debug("WebSocket %s unsubscribe error for %s", conn_id, name)
#             else:
#                 try:
#                     await asyncio.wait_for(broadcaster.unsubscribe("snapshot_updated", sync_queue), timeout=3.0)
#                 except Exception:
#                     logger.debug("WebSocket %s unsubscribe error (legacy)", conn_id)
#             logger.debug("WebSocket %s unsubscribed from broadcaster", conn_id)
#         except Exception as e:
#             logger.exception("WebSocket %s unsubscribe error: %s", conn_id, e)
    
#     # Remove from active connections
#     ws_active_connections.discard(conn_id)
    
#     # Close WebSocket connection
#     try:
#         if websocket.client_state != websocket.client_state.DISCONNECTED:
#             await asyncio.wait_for(websocket.close(), timeout=3.0)
#     except Exception as e:
#         logger.debug("WebSocket %s close error (expected): %s", conn_id, e)
    
#     logger.info("WebSocket %s cleanup completed", conn_id)