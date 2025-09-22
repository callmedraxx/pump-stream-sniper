import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp

from ..models import Token

logger = logging.getLogger(__name__)


class CandleFetchService:
    """Service for fetching candle data from pump.fun API"""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session or aiohttp.ClientSession()
        self.base_url = "https://swap-api.pump.fun"
        # Rate limiting: allow X requests per second (default 5 rps)
        self.requests_per_second = 5
        self._last_request = 0.0
        self._rate_lock = asyncio.Lock()

    async def fetch_candle_data(self, mint_address: str, interval: str = "1m", limit: int = 120) -> Optional[List[Dict]]:
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

    async def fetch_candles_for_live_tokens(self, tokens: List[Token], interval: str = "1m", limit: int = 120) -> Dict[str, List[Dict]]:
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

    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()