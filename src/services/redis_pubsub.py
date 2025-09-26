# import asyncio
# import logging
# import os
# import msgpack
# from typing import Any

# import redis.asyncio as redis

# from .event_broadcaster import broadcaster

# logger = logging.getLogger(__name__)

# REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")

# # Channels used for inter-process communication
# CHANNEL_TOKEN_UPDATES = "token_updates"
# CHANNEL_SNAPSHOT = "snapshot_updates"


# async def _handle_message(channel: str, data: bytes):
#     """Deserialize MessagePack data and publish into broadcaster."""
#     try:
#         obj = msgpack.unpackb(data, raw=False)
#     except Exception as e:
#         logger.exception("Failed to unpack msgpack from redis channel %s: %s", channel, e)
#         return

#     try:
#         logger.info("Redis pubsub received message on %s (type=%s)", channel, obj.get('type') if isinstance(obj, dict) else type(obj))
#         if channel == CHANNEL_TOKEN_UPDATES:
#             # Broadcast individual token update event
#             # If this is an mcap_update envelope, normalize and republish as mcap_update
#             if isinstance(obj, dict) and obj.get('type') == 'mcap_update':
#                 data = obj.get('data', {})
#                 logger.info("Redis pubsub republishing mcap_update for %s", data.get('mint_address') or data.get('mint'))
#                 await broadcaster.publish("mcap_update", data)
#             else:
#                 await broadcaster.publish("token_updated", obj)
#         elif channel == CHANNEL_SNAPSHOT:
#             # Broadcast snapshot update
#             await broadcaster.publish("snapshot_updated", obj)
#         else:
#             logger.debug("Redis pubsub received message on unknown channel %s", channel)
#     except Exception:
#         logger.exception("Failed to publish message from redis channel %s into broadcaster", channel)


# async def subscribe_loop(stop_event: asyncio.Event):
#     """Background task that subscribes to Redis channels and republishes messages to the in-process broadcaster.

#     Exits when stop_event is set.
#     """
#     redis_client = None
#     try:
#         redis_client = redis.from_url(REDIS_URL)
#         pubsub = redis_client.pubsub()

#         await pubsub.subscribe(CHANNEL_TOKEN_UPDATES, CHANNEL_SNAPSHOT)
#         logger.info("Subscribed to redis channels: %s, %s", CHANNEL_TOKEN_UPDATES, CHANNEL_SNAPSHOT)

#         while not stop_event.is_set():
#             message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
#             if not message:
#                 await asyncio.sleep(0.1)
#                 continue

#             # message dict contains 'channel' and 'data'
#             channel = message.get('channel')
#             data = message.get('data')
#             if isinstance(channel, bytes):
#                 channel = channel.decode('utf-8')

#             if data is None:
#                 continue

#             if isinstance(data, str):
#                 data = data.encode('utf-8')

#             # Handle message asynchronously so it doesn't block the loop
#             asyncio.create_task(_handle_message(channel, data))

#     except asyncio.CancelledError:
#         logger.info("Redis subscribe loop cancelled")
#     except Exception:
#         logger.exception("Redis subscribe loop failed")
#     finally:
#         try:
#             if redis_client is not None:
#                 await redis_client.close()
#         except Exception:
#             pass


# # Tiny helper to publish from this process to Redis (for producers)
# async def publish_token_update_to_redis(obj: Any):
#     try:
#         redis_client = redis.from_url(REDIS_URL)
#         packed = msgpack.packb(obj, use_bin_type=True)
#         await redis_client.publish(CHANNEL_TOKEN_UPDATES, packed)
#         await redis_client.close()
#     except Exception:
#         logger.exception("Failed to publish token update to redis")


# async def publish_snapshot_to_redis(obj: Any):
#     try:
#         redis_client = redis.from_url(REDIS_URL)
#         packed = msgpack.packb(obj, use_bin_type=True)
#         await redis_client.publish(CHANNEL_SNAPSHOT, packed)
#         await redis_client.close()
#     except Exception:
#         logger.exception("Failed to publish snapshot to redis")
