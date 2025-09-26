import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp

from ..models import Token
from ..models.database import SessionLocal, DB_CONNECTION_SEMAPHORE

logger = logging.getLogger(__name__)


# In-memory cache for candle data shared across workers in the process
_CANDLE_CACHE: dict = {}
_CANDLE_CACHE_LOCK: asyncio.Lock | None = None


def _ensure_cache_lock():
    global _CANDLE_CACHE_LOCK
    if _CANDLE_CACHE_LOCK is None:
        _CANDLE_CACHE_LOCK = asyncio.Lock()


async def set_candle_cache(mint: str, data: Optional[List[Dict]]):
    _ensure_cache_lock()
    async with _CANDLE_CACHE_LOCK:
        if data is None:
            _CANDLE_CACHE.pop(mint, None)
        else:
            _CANDLE_CACHE[mint] = data


async def get_candle_cache(mint: str) -> Optional[List[Dict]]:
    _ensure_cache_lock()
    async with _CANDLE_CACHE_LOCK:
        return _CANDLE_CACHE.get(mint)


class CandleFetchService:
    """Service for fetching candle data from pump.fun API"""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session or aiohttp.ClientSession()
        self.base_url = "https://swap-api.pump.fun"
        # Rate limiting: allow X requests per second (default 5 rps)
        self.requests_per_second = 5
        self._last_request = 0.0
        self._rate_lock = asyncio.Lock()

    async def fetch_candle_data(self, mint_address: str, interval: str = "1h", limit: int = 120) -> Optional[List[Dict]]:
        """
        Fetch candle data for a specific token

        Args:
            mint_address: Token mint address
            interval: Candle interval (e.g., "1m", "5m", "1h")
            limit: Number of candles to fetch

        Returns:
            List of candle data or None if failed
        """
        # Rate limiting: simple sleep-based throttle to target requests_per_second
        # Also implement retry with exponential backoff on 429 and other 5xx errors
        max_retries = 4
        backoff_base = 1.5

        for attempt in range(1, max_retries + 1):
            # Throttle to requests_per_second
            async with self._rate_lock:
                now = asyncio.get_event_loop().time()
                min_interval = 1.0 / float(self.requests_per_second)
                elapsed = now - self._last_request
                if elapsed < min_interval:
                    await asyncio.sleep(min_interval - elapsed)
                self._last_request = asyncio.get_event_loop().time()

            try:
                created_ts = int(datetime.now().timestamp() * 1000)
                url = f"{self.base_url}/v2/coins/{mint_address}/candles"
                params = {"interval": interval, "limit": limit, "createdTs": created_ts}

                async with self.session.get(url, params=params) as response:
                    status = response.status
                    if status == 200:
                        data = await response.json()
                        logger.debug(f"Successfully fetched {len(data) if isinstance(data, list) else 0} candles for {mint_address}")
                        return data

                    # Handle rate limiting
                    if status == 429:
                        # Respect Retry-After header if present
                        retry_after = None
                        try:
                            ra = response.headers.get("Retry-After")
                            if ra:
                                retry_after = float(ra)
                        except Exception:
                            retry_after = None

                        if retry_after:
                            wait = retry_after
                        else:
                            wait = backoff_base ** (attempt - 1)

                        #logger.warning(f"Failed to fetch candles for {mint_address}: HTTP 429 (retry {attempt}/{max_retries}), sleeping {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    # Retry on server errors
                    if 500 <= status < 600:
                        wait = backoff_base ** (attempt - 1)
                        logger.warning(f"Server error fetching candles for {mint_address}: HTTP {status} (retry {attempt}/{max_retries}), sleeping {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    # Other non-success statuses: log and don't retry
                    #logger.warning(f"Failed to fetch candles for {mint_address}: HTTP {status}")
                    return None

            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Network or JSON errors: retry a few times
                wait = backoff_base ** (attempt - 1)
                logger.warning(f"Error fetching candle data for {mint_address} (attempt {attempt}/{max_retries}): {e} — sleeping {wait}s")
                await asyncio.sleep(wait)

        logger.error(f"Exhausted retries fetching candles for {mint_address}")
        return None

    async def fetch_candles_for_live_tokens(self, tokens: List[Token], interval: str = "1h", limit: int = 120) -> Dict[str, List[Dict]]:
        """
        Fetch candle data for all live tokens

        Args:
            tokens: List of Token objects
            interval: Candle interval
            limit: Number of candles to fetch per token

        Returns:
            Dictionary mapping mint_address to candle data
        """
        live_tokens = [token for token in tokens if token.is_live]
        logger.info(f"Fetching candle data for {len(live_tokens)} live tokens")

        if not live_tokens:
            return {}

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

        async def fetch_single_token_candles(token: Token):
            async with semaphore:
                try:
                    candle_data = await self.fetch_candle_data(token.mint_address, interval, limit)
                    return token.mint_address, candle_data
                except Exception as e:
                    logger.error(f"Exception fetching candles for {token.mint_address}: {e}")
                    return token.mint_address, None

        # Execute fetches in parallel
        tasks = [fetch_single_token_candles(token) for token in live_tokens]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        candle_data_dict: Dict[str, List[Dict]] = {}
        successful_fetches = 0

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception during candle fetch: {result}")
                continue

            mint_address, candle_data = result
            if candle_data:
                candle_data_dict[mint_address] = candle_data
                successful_fetches += 1
            else:
                logger.debug(f"No candle data for {mint_address}")

        logger.info(f"Successfully fetched candle data for {successful_fetches}/{len(live_tokens)} tokens")
        return candle_data_dict

    # --- Background runner -------------------------------------------------
    # Note: per-token DB writes removed. We collect fetched candle data into a
    # local cache and perform bulk writes in batches using a single DB session
    # per batch to avoid exhausting the DB connection pool.

    async def run_loop(self, interval_seconds: int = 60, limit: int = 120, concurrency: int = 10):
        """
        Periodically fetch candle data for live tokens and persist to DB.

        - Queries the DB each iteration so newly-added tokens are picked up.
        - Runs every `interval_seconds` (default 60s).
        - Uses an HTTP semaphore for concurrent requests and a DB semaphore
          to avoid exceeding the DB connection pool.
        """
        self.requests_per_second = max(self.requests_per_second, 1)
        http_semaphore = asyncio.Semaphore(concurrency)

        async def _fetch_for_token(token: Token):
            async with http_semaphore:
                try:
                    return token.mint_address, await self.fetch_candle_data(token.mint_address, "1h", limit)
                except Exception as e:
                    logger.exception("Error fetching candles for %s: %s", token.mint_address, e)
                    return token.mint_address, None

        logger.info("Starting CandleFetchService run loop (interval=%ss)", interval_seconds)
        while True:
            try:
                # Query DB for live tokens (fresh each loop to pick up new tokens)
                db = SessionLocal()
                try:
                    tokens = db.query(Token).filter(Token.is_live == True).all()
                finally:
                    try:
                        db.close()
                    except Exception:
                        pass

                if not tokens:
                    logger.debug("No live tokens found for candle fetch; sleeping")
                    await asyncio.sleep(interval_seconds)
                    continue

                # Throttle concurrent HTTP fetches via the local semaphore
                tasks = [_fetch_for_token(t) for t in tokens]
                results = []
                # Execute in reasonably-sized batches to avoid huge spikes
                batch_size = max(10, concurrency)
                for i in range(0, len(tasks), batch_size):
                    slice_tasks = tasks[i : i + batch_size]
                    res = await asyncio.gather(*slice_tasks, return_exceptions=True)
                    results.extend(res)

                # Build list of successful fetches and update local cache
                updates: list[tuple[str, List[Dict]]] = []
                for item in results:
                    if isinstance(item, Exception):
                        logger.debug("Exception in fetch results: %s", item)
                        continue
                    mint, candles = item
                    if not candles:
                        logger.debug("No candles for %s", mint)
                        continue
                    # Store in local in-memory cache for other workers
                    try:
                        await set_candle_cache(mint, candles)
                    except Exception:
                        logger.exception("Failed to set candle cache for %s", mint)
                    updates.append((mint, candles))

                # Bulk write updates in chunks using one DB session per chunk
                if updates:
                    # chunk to reasonable size to avoid huge SQL statements / mem usage
                    WRITE_CHUNK = 200

                    def _do_bulk_write(batch: list[tuple[str, List[Dict]]]) -> int:
                        db = SessionLocal()
                        try:
                            mint_list = [m for m, _ in batch]
                            rows = db.query(Token).filter(Token.mint_address.in_(mint_list)).all()
                            token_map = {r.mint_address: r for r in rows}
                            applied = 0
                            for m, candles in batch:
                                t = token_map.get(m)
                                if not t:
                                    continue
                                t.candle_data = candles
                                applied += 1
                            if applied:
                                db.commit()
                            return applied
                        except Exception:
                            try:
                                db.rollback()
                            except Exception:
                                pass
                            logger.exception("Exception during bulk candle write")
                            return 0
                        finally:
                            try:
                                db.close()
                            except Exception:
                                pass

                    # execute chunks, gating DB concurrency with semaphore
                    for i in range(0, len(updates), WRITE_CHUNK):
                        chunk = updates[i : i + WRITE_CHUNK]
                        try:
                            async with DB_CONNECTION_SEMAPHORE:
                                applied = await asyncio.to_thread(_do_bulk_write, chunk)
                                logger.info("Bulk wrote %d candles (chunk %d/%d)", applied, i // WRITE_CHUNK + 1, (len(updates) + WRITE_CHUNK - 1) // WRITE_CHUNK)
                        except Exception:
                            logger.exception("Error during bulk candle chunk write starting at %d", i)

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected error in CandleFetchService run loop")

            await asyncio.sleep(interval_seconds)

    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()