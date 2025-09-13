import asyncio
import logging
from typing import Any, Callable, Dict, List

"""
Lightweight in-process event broadcaster for notifying components (SSE, websockets) about events.
This avoids polling and lets DatabaseSyncService publish sync events that the SSE generator can await.

Usage:
    from src.services.event_broadcaster import broadcaster
    await broadcaster.publish('sync_completed', payload)
    await broadcaster.subscribe('sync_completed', callback)

The callback will be called with the payload (can be sync stats dict).

This broadcaster is intentionally simple and runs in-process; it does not persist events.
"""

logger = logging.getLogger(__name__)


class Broadcaster:
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, event_name: str) -> asyncio.Queue:
        """Return an asyncio.Queue that will receive payloads for event_name."""
        async with self._lock:
            q = asyncio.Queue()
            self._subs.setdefault(event_name, []).append(q)
            # log subscription
            try:
                logger.info("subscriber added for event '%s' (total=%d)", event_name, len(self._subs.get(event_name, [])))
            except Exception:
                pass
            return q

    async def unsubscribe(self, event_name: str, q: asyncio.Queue):
        async with self._lock:
            lst = self._subs.get(event_name)
            if not lst:
                return
            try:
                lst.remove(q)
            except ValueError:
                pass
            if not lst:
                self._subs.pop(event_name, None)
            try:
                logger.info("subscriber removed for event '%s' (remaining=%d)", event_name, len(self._subs.get(event_name, [])))
            except Exception:
                pass

    async def publish(self, event_name: str, payload: Any):
        """Publish payload to all subscribers (non-blocking)."""
        async with self._lock:
            queues = list(self._subs.get(event_name, []))
        try:
            logger.info("publishing event '%s' to %d subscribers", event_name, len(queues))
        except Exception:
            pass
        for q in queues:
            # Use put_nowait to avoid slow subscribers blocking publisher
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # If a queue is full, drop the event for that subscriber
                pass


# Singleton broadcaster
broadcaster = Broadcaster()
