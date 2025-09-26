# from fastapi import APIRouter, WebSocket
# import asyncio
# import json
# from datetime import datetime
# from sqlalchemy import desc
# from sqlalchemy.orm import Session

# from ..services.stream import connected_clients
# from ..models import Token, get_db
# from .live import TokenSortService

# # Global state for WebSocket connections
# active_connections = set()

# router = APIRouter()


# @router.websocket("/ws/tokens")
# async def websocket_tokens_endpoint(websocket: WebSocket):
#     """
#     WebSocket endpoint for dynamic token streaming with real-time sorting
#     """
#     await websocket.accept()
#     active_connections.add(id(websocket))

#     try:
#         # Default sorting parameters
#         current_sort = {
#             "sort_by": "age",
#             "sort_order": "desc",
#             "time_period": None,
#             "filters": {},
#             "limit": 50,
#             "offset": 0
#         }

#         db = next(get_db())
#         # subscribe to snapshot updates so we can forward them immediately
#         from ..services.event_broadcaster import broadcaster
#         snapshot_queue = await broadcaster.subscribe("snapshot_updated")
#         last_update_time = None

#         # Send initial connection confirmation
#         await websocket.send_json({
#             "event": "connected",
#             "timestamp": datetime.now().isoformat(),
#             "message": "WebSocket connected for dynamic token streaming"
#         })

#         while True:
#             try:
#                 # Check for sorting updates from client (non-blocking)
#                 try:
#                     message = await asyncio.wait_for(websocket.receive_json(), timeout=0.1)
#                     if message.get("type") == "update_sorting":
#                         current_sort.update(message.get("params", {}))
#                         print(f"🔄 Updated sorting: {current_sort}")

#                         # Confirm sorting update
#                         await websocket.send_json({
#                             "event": "sorting_updated",
#                             "timestamp": datetime.now().isoformat(),
#                             "new_sorting": current_sort
#                         })

#                 except asyncio.TimeoutError:
#                     pass  # No message received, continue with current sorting

#                 # Build query with current sorting
#                 query = db.query(Token)

#                 # Apply current filters
#                 query = TokenSortService.apply_filters(query, current_sort["filters"])

#                 # Apply current sorting
#                 query = TokenSortService.apply_sorting(
#                     query,
#                     current_sort["sort_by"],
#                     current_sort["sort_order"],
#                     current_sort["time_period"]
#                 )

#                 # Check for updates
#                 latest_update = (
#                     db.query(Token.updated_at)
#                     .order_by(desc(Token.updated_at))
#                     .first()
#                 )
#                 current_update_time = latest_update[0] if latest_update else None

#                 # Send update if data changed or if a snapshot update was published
#                 forced_send = False
#                 try:
#                     _ = snapshot_queue.get_nowait()
#                     forced_send = True
#                     # when snapshot_updated arrives, prefer immediate send
#                 except asyncio.QueueEmpty:
#                     forced_send = False

#                 if current_update_time != last_update_time or forced_send:
#                     # Apply pagination and get tokens
#                     total_count = query.count()
#                     tokens = query.offset(current_sort["offset"]).limit(current_sort["limit"]).all()

#                     # Format tokens (compact format for WebSocket)
#                     token_data = []
#                     for t in tokens:
#                         token_dict = {
#                             "mint_address": t.mint_address,
#                             "name": t.name,
#                             "symbol": t.symbol,
#                             "mcap": t.mcap,
#                             "dev_activity": t.dev_activity,
#                             "created_coin_count": t.created_coin_count,
#                             "creator_balance_sol": t.creator_balance_sol,
#                             "creator_balance_usd": t.creator_balance_usd,
#                             "viewers": t.viewers,
#                             "is_live": t.is_live,
#                             "updated_at": t.updated_at.isoformat(),
#                         }
#                         token_data.append(token_dict)

#                     # Send WebSocket message
#                     event_data = {
#                         "event": "tokens_update",
#                         "timestamp": datetime.now().isoformat(),
#                         "sorting": current_sort,
#                         "data": {
#                             "tokens": token_data,
#                             "pagination": {
#                                 "total": total_count,
#                                 "limit": current_sort["limit"],
#                                 "offset": current_sort["offset"],
#                             },
#                         },
#                     }

#                     await websocket.send_json(event_data)
#                     last_update_time = current_update_time

#                 await asyncio.sleep(2)  # Update interval

#             except Exception as e:
#                 error_data = {
#                     "event": "error",
#                     "timestamp": datetime.now().isoformat(),
#                     "error": str(e),
#                 }
#                 await websocket.send_json(error_data)
#                 await asyncio.sleep(2)

#     finally:
#         active_connections.discard(id(websocket))
#         try:
#             await broadcaster.unsubscribe("snapshot_updated", snapshot_queue)
#         except Exception:
#             pass


# @router.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     connected_clients.add(websocket)
#     try:
#         while True:
#             # Keep the connection alive, messages are sent from the relay task
#             await websocket.receive_text()
#     except:
#         pass
#     finally:
#         connected_clients.remove(websocket)
