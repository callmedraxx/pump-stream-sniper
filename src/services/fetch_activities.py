import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from fastapi import HTTPException
import time

load_dotenv()

logger = logging.getLogger(__name__)

# The batch endpoint accepts at most 50 addresses per request (observed from API)
MAX_BATCH_SIZE = 50

# Throttling state for creator balance API calls (global per-process)
_balance_throttle_lock: asyncio.Lock | None = None
_last_balance_request_time: float = 0.0


def _ensure_balance_lock():
    global _balance_throttle_lock
    if _balance_throttle_lock is None:
        _balance_throttle_lock = asyncio.Lock()


async def _throttle_balance_request(min_interval: float = 1.0):
    """Ensure at least `min_interval` seconds between balance API requests per process."""
    global _last_balance_request_time
    _ensure_balance_lock()
    async with _balance_throttle_lock:
        now = asyncio.get_event_loop().time()
        elapsed = now - _last_balance_request_time
        to_wait = min_interval - elapsed
        if to_wait and to_wait > 0:
            await asyncio.sleep(to_wait)
        _last_balance_request_time = asyncio.get_event_loop().time()


async def fetch_market_activities(pool_address: str) -> Dict:
    """
    Fetch market activities for a specific token pool from pump.fun API

    Args:
        pool_address (str): The pool address (not mint address).
                          To get pool address from mint, you can use the token data
                          where pool_address = token.get('pump_swap_pool')

    Returns:
        Dict containing market activity data with time periods

    Raises:
        HTTPException: For various error conditions (400, 404, 429, 500, 502, 503)
    """

    if not pool_address:
        error_msg = "Pool address is required"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    if not isinstance(pool_address, str) or len(pool_address.strip()) == 0:
        error_msg = "Pool address must be a non-empty string"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    url = f"https://swap-api.pump.fun/v1/pools/{pool_address.strip()}/market-activity"

    headers = {
        "Host": "swap-api.pump.fun",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Sec-Ch-Ua-Mobile": "?0",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://pump.fun/",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200 or response.status == 201:
                    try:
                        data = await response.json()
                        if not data:
                            error_msg = (
                                f"Empty response received for pool: {pool_address}"
                            )
                            print(f"⚠️ Warning: {error_msg}")
                            raise HTTPException(status_code=404, detail=error_msg)
                        return data

                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON response from API: {str(e)}"
                        print(f"❌ Error: {error_msg}")
                        raise HTTPException(status_code=502, detail=error_msg)

                elif response.status == 404:
                    error_msg = f"Pool not found: {pool_address}"
                    print(f"❌ Error: {error_msg}")
                    raise HTTPException(status_code=404, detail=error_msg)

                elif response.status == 429:
                    error_msg = "Rate limit exceeded. Please try again later."
                    print(f"❌ Error: {error_msg}")
                    raise HTTPException(status_code=429, detail=error_msg)

                elif response.status >= 500:
                    error_msg = f"Server error: {response.status}"
                    print(f"❌ Error: {error_msg}")
                    raise HTTPException(status_code=502, detail=error_msg)

                else:
                    error_msg = f"API request failed with status: {response.status}"
                    print(f"❌ Error: {error_msg}")
                    try:
                        error_text = await response.text()
                        print(f"Response body: {error_text}")
                    except:
                        pass
                    raise HTTPException(status_code=response.status, detail=error_msg)

    except aiohttp.ClientError as e:
        error_msg = f"Network error while fetching market activities: {str(e)}"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=503, detail=error_msg)

    except Exception as e:
        error_msg = f"Unexpected error fetching market activities: {str(e)}"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=500, detail=error_msg)


async def get_market_activities_by_mint(
    mint_address: str, token_data: Dict = None
) -> Dict:
    """
    Convenience function to get market activities using mint address.
    Requires token data to find the corresponding pool address.

    Args:
        mint_address (str): The token mint address
        token_data (Dict): Token data containing pool information

    Returns:
        Market activity data

    Raises:
        HTTPException: For various error conditions
    """

    if not mint_address:
        error_msg = "Mint address is required"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    if not isinstance(mint_address, str) or len(mint_address.strip()) == 0:
        error_msg = "Mint address must be a non-empty string"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    if not token_data:
        error_msg = f"Token data required to find pool address for mint: {mint_address}"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    if not isinstance(token_data, dict):
        error_msg = "Token data must be a dictionary"
        print(f"❌ Error: {error_msg}")
        raise HTTPException(status_code=400, detail=error_msg)

    # Try to find pool address from token data
    pool_address = token_data.get("pump_swap_pool")

    if not pool_address:
        error_msg = f"No pool address found for mint: {mint_address}"
        print(f"❌ Error: {error_msg}")
        print(f"Available pool fields: pump_swap_pool, raydium_pool")
        raise HTTPException(status_code=404, detail=error_msg)

    print(f"🔍 Found pool address {pool_address} for mint {mint_address}")
    return await fetch_market_activities(pool_address)


async def fetch_market_activities_batch(addresses: List[str]) -> Dict[str, Dict]:
    """
    Fetch market activities for multiple token addresses using the batch endpoint.
    
    Args:
        addresses: List of mint addresses to fetch market activities for
        
    Returns:
        Dict mapping mint addresses to their market activity data
        
    Raises:
        HTTPException: For various error conditions
    """
    if not addresses:
        return {}
    
    if len(addresses) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400, 
            detail=f"Too many addresses. Maximum {MAX_BATCH_SIZE} addresses per batch."
        )
    
    url = "https://swap-api.pump.fun/v1/coins/market-activity/batch"
    
    headers = {
        "Host": "swap-api.pump.fun",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
        "Content-Type": "application/json",
        "Sec-Ch-Ua-Mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i"
    }
    
    payload = {
        "addresses": addresses,
        "intervals": ["5m", "1h", "6h", "24h"],
        "metrics": [
            "numTxs", "volumeUSD", "numUsers", "numBuys", "numSells",
            "buyVolumeUSD", "sellVolumeUSD", "numBuyers", "numSellers", 
            "priceChangePercent"
        ]
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, 
                headers=headers, 
                json=payload,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                
                if response.status == 200 or response.status == 201:
                    try:
                        data = await response.json()
                        #logger.info(f"Successfully fetched market activities for {len(addresses)} tokens")
                        return data
                    except json.JSONDecodeError as e:
                        error_msg = f"Invalid JSON response from batch API: {str(e)}"
                        logger.error(error_msg)
                        raise HTTPException(status_code=502, detail=error_msg)
                
                elif response.status == 400:
                    try:
                        error_data = await response.json()
                        error_msg = f"Bad request to batch API: {error_data}"
                        logger.error(error_msg)
                        raise HTTPException(status_code=400, detail=error_msg)
                    except:
                        error_msg = f"Bad request to batch API (status 400)"
                        logger.error(error_msg)
                        raise HTTPException(status_code=400, detail=error_msg)
                
                elif response.status == 429:
                    error_msg = "Rate limit exceeded on batch API. Please try again later."
                    logger.warning(error_msg)
                    raise HTTPException(status_code=429, detail=error_msg)
                
                elif response.status >= 500:
                    error_msg = f"Server error from batch API: {response.status}"
                    logger.error(error_msg)
                    raise HTTPException(status_code=502, detail=error_msg)
                
                else:
                    error_msg = f"Batch API request failed with status: {response.status}"
                    logger.error(error_msg)
                    try:
                        error_text = await response.text()
                        logger.error(f"Response body: {error_text}")
                    except:
                        pass
                    raise HTTPException(status_code=response.status, detail=error_msg)
    
    except asyncio.TimeoutError:
        error_msg = "Timeout while fetching batch market activities"
        logger.error(error_msg)
        raise HTTPException(status_code=504, detail=error_msg)
    except aiohttp.ClientError as e:
        error_msg = f"Network error while fetching batch market activities: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=503, detail=error_msg)
    except Exception as e:
        error_msg = f"Unexpected error fetching batch market activities: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


def convert_batch_response_to_legacy_format(batch_data: Dict) -> Dict:
    """
    Convert the new batch API response format to the format expected by database sync service.
    
    The batch API returns:
    {
      "address": {
        "5m": {"numTxs": 13, "volumeUSD": 3551.47, "numUsers": 11, ...},
        "1h": {...},
        "6h": {...}, 
        "24h": {...}
      }
    }
    
    The database sync service expects:
    {
      "5m": {"numTxs": 13, "volumeUSD": 3551.47, "numUsers": 11, ...},
      "1h": {...},
      "6h": {...},
      "24h": {...}
    }
    
    We keep the same field names to match what database sync service expects.
    """
    if not batch_data:
        return {}
    
    converted = {}
    
    for time_period in ["5m", "1h", "6h", "24h"]:
        if time_period in batch_data:
            # Keep the same field names as the batch API since that's what database sync expects
            converted[time_period] = batch_data[time_period]
        else:
            # Default empty data for missing time periods
            converted[time_period] = {
                "numTxs": 0,
                "volumeUSD": 0.0,
                "numUsers": 0,
                "numBuys": 0,
                "numSells": 0,
                "buyVolumeUSD": 0.0,
                "sellVolumeUSD": 0.0,
                "numBuyers": 0,
                "numSellers": 0,
                "priceChangePercent": 0.0
            }
    
    return converted


async def fetch_market_activities_for_addresses(addresses: List[str]) -> Dict[str, Dict]:
    """
    Fetch market activities for multiple addresses, automatically splitting into batches.
    Returns a mapping of address -> converted market activity data.
    
    Args:
        addresses: List of mint addresses
        
    Returns:
        Dict mapping addresses to market activity data in legacy format
    """
    if not addresses:
        return {}
    
    results = {}
    
    # Split addresses into batches
    for i in range(0, len(addresses), MAX_BATCH_SIZE):
        batch = addresses[i:i + MAX_BATCH_SIZE]
        #logger.info(f"Fetching market activities for batch {i//MAX_BATCH_SIZE + 1} ({len(batch)} addresses)")
        
        try:
            batch_response = await fetch_market_activities_batch(batch)
            
            # Convert each token's data to legacy format
            for address in batch:
                if address in batch_response:
                    results[address] = convert_batch_response_to_legacy_format(batch_response[address])
                else:
                    # Token not found in response, use empty data
                    results[address] = convert_batch_response_to_legacy_format({})
                    logger.warning(f"No market activity data found for address: {address}")
        
        except Exception as e:
            logger.error(f"Failed to fetch batch {i//MAX_BATCH_SIZE + 1}: {e}")
            # Set empty data for all addresses in failed batch
            for address in batch:
                results[address] = convert_batch_response_to_legacy_format({})
        
        # Small delay between batches to be respectful to the API
        if i + MAX_BATCH_SIZE < len(addresses):
            await asyncio.sleep(0.5)
    
    return results


async def get_market_activities_by_mint_batch(mint_address: str) -> Dict:
    """
    Enhanced version that uses the batch endpoint for a single address.
    This provides better reliability than the old pool-based endpoint.
    
    Args:
        mint_address: The token mint address
        
    Returns:
        Market activity data in legacy format
    """
    if not mint_address:
        error_msg = "Mint address is required"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    if not isinstance(mint_address, str) or len(mint_address.strip()) == 0:
        error_msg = "Mint address must be a non-empty string"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    mint_address = mint_address.strip()
    
    try:
        batch_results = await fetch_market_activities_for_addresses([mint_address])
        return batch_results.get(mint_address, convert_batch_response_to_legacy_format({}))
    except Exception as e:
        logger.error(f"Failed to fetch market activities for mint {mint_address}: {e}")
        return convert_batch_response_to_legacy_format({})


async def fetch_creator_trades_for_mint(mint_address: str, creator_address: str) -> dict:
    """
    Fetch recent trades for a specific mint filtered by creator/user addresses using the trades batch endpoint.
    Returns the latest trade info for that creator if present.
    """
    if not mint_address or not creator_address:
        return {}

    url = f"https://swap-api.pump.fun/v1/coins/{mint_address}/trades/batch"

    headers = {
        "Host": "swap-api.pump.fun",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
        "Content-Type": "application/json",
        "Sec-Ch-Ua-Mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
    }

    payload = {"userAddresses": [creator_address]}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status in (200, 201):
                    try:
                        data = await resp.json()
                        # data is expected to be {creator_address: [trades...]}
                        creator_trades = data.get(creator_address) or []
                        if not creator_trades:
                            return {}
                        # Return the most recent trade (assuming sorted newest first)
                        latest = creator_trades[0]
                        # Normalize fields we need
                        return {
                            "type": latest.get("type"),
                            "amountUSD": float(latest.get("amountUSD", 0)) if latest.get("amountUSD") else None,
                            "amountSOL": float(latest.get("amountSOL", 0)) if latest.get("amountSOL") else None,
                            "timestamp": latest.get("timestamp"),
                            "userAddress": latest.get("userAddress"),
                            "tx": latest.get("tx"),
                        }
                    except Exception as e:
                        logger.warning(f"Failed parsing creator trades response for {mint_address}: {e}")
                        return {}
                else:
                    logger.warning(f"Creator trades API returned {resp.status} for {mint_address}")
                    return {}
    except Exception as e:
        logger.error(f"Network error fetching creator trades for {mint_address}: {e}")
        return {}


async def fetch_creator_created_count(creator_address: str, offset: int = 0, limit: int = 1000, include_nsfw: bool = False) -> int:
    """
    Fetch the number of coins created by a given creator address using frontend-api-v3.

    Returns the `count` integer from the API response, or 0 on error.
    """
    if not creator_address:
        return 0

    url = f"https://frontend-api-v3.pump.fun/coins/user-created-coins/{creator_address}?offset={offset}&limit=10&includeNsfw={'true' if include_nsfw else 'false'}"

    headers = {
        "Host": "frontend-api-v3.pump.fun",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Sec-Ch-Ua-Mobile": "?0",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status in (200, 201):
                    try:
                        data = await resp.json()
                        # Expect response like {"count": 42, ...}
                        count = int(data.get("count", 0) or 0)
                        return count
                    except Exception as e:
                        logger.warning(f"Failed parsing created-count response for {creator_address}: {e}")
                        return 0
                else:
                    logger.warning(f"Created-count API returned {resp.status} for {creator_address}")
                    return 0
    except Exception as e:
        logger.error(f"Network error fetching created-count for {creator_address}: {e}")
        return 0


async def fetch_creator_created_counts(
    creator_addresses,
    concurrency: int = 3,
    max_retries: int = 3,
    base_backoff: float = 0.5,
):
    """
    Fetch created_coin_count for multiple creators with limited concurrency and retries.

    Returns a dict mapping creator_address -> count (int). On error, the value will be 0.
    This reduces rate-limit errors by reusing an aiohttp session, limiting concurrency,
    and retrying with exponential backoff when HTTP 429 is encountered.
    """
    if not creator_addresses:
        return {}

    results = {}
    sem = asyncio.Semaphore(concurrency)
    # Shared timestamp (epoch seconds) until which we should back off globally
    rate_limit_until = 0.0

    async def _fetch_with_retries(session, creator):
        url = (
            f"https://frontend-api-v3.pump.fun/coins/user-created-coins/{creator}?offset=0&limit=10&includeNsfw=false"
        )
        headers = {
            "Host": "frontend-api-v3.pump.fun",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
            "User-Agent": "pump-stream-sniper/1.0",
            "Sec-Ch-Ua-Mobile": "?0",
            "Accept": "*/*",
            "Origin": "https://pump.fun",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Accept-Encoding": "gzip, deflate, br",
            "Priority": "u=1, i",
        }

        nonlocal rate_limit_until
        attempt = 0
        while attempt <= max_retries:
            # If a recent global rate-limit was triggered, wait until it expires
            now = time.time()
            if now < rate_limit_until:
                to_wait = rate_limit_until - now
                logger.debug(f"Global rate-limit active, waiting {to_wait:.1f}s before requesting {creator}")
                await asyncio.sleep(to_wait)
            try:
                async with sem:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status in (200, 201):
                            try:
                                data = await resp.json()
                                count = int(data.get("count", 0) or 0)
                                return count
                            except Exception as e:
                                logger.warning(f"Failed parsing created-count for {creator}: {e}")
                                return 0
                        elif resp.status == 429:
                            # Rate limited — set a global backoff window so other tasks pause too
                            attempt += 1
                            # set global rate limit to 50 seconds from now
                            rate_limit_until = time.time() + 50
                            logger.warning(f"429 for {creator}: entering global backoff for 50s (attempt {attempt})")
                            await asyncio.sleep(50)
                            continue
                        else:
                            logger.warning(f"Created-count API returned {resp.status} for {creator}")
                            return 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"Network error fetching created-count for {creator}: {e}")
                attempt += 1
                backoff = base_backoff * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)

        # If we exhausted retries, return 0
        logger.warning(f"Exhausted retries fetching created count for {creator}")
        return 0

    async with aiohttp.ClientSession() as session:
        tasks = [
            asyncio.create_task(_fetch_with_retries(session, creator)) for creator in creator_addresses
        ]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)

    for creator, val in zip(creator_addresses, results_list):
        if isinstance(val, Exception):
            logger.warning(f"Error fetching created-count for {creator}: {val}")
            results[creator] = 0
        else:
            results[creator] = int(val or 0)

    return results

async def fetch_creator_balance_for_mint(creator_address: str, target_mint: str, page: int = 1, limit: int = 1000, session: aiohttp.ClientSession = None) -> dict:
    """
    Fetch wallet balances for creator and return the balance and balanceUSD for target_mint.

    Returns dict {"balance": float, "balanceUSD": float} or {} if not found.
    """
    if not creator_address or not target_mint:
        return {}

    url = f"https://swap-api.pump.fun/v1/wallet/{creator_address}/balances?includePnl=true&page={page}&limit=10"

    headers = {
        "Host": "swap-api.pump.fun",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Ch-Ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Sec-Ch-Ua-Mobile": "?0",
        "Accept": "*/*",
        "Origin": "https://pump.fun",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=1, i",
    }

    try:
        # Respect per-request throttling (1s between calls)
        try:
            await _throttle_balance_request(min_interval=1.0)
        except Exception:
            pass

        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status in (200, 201):
                    try:
                        data = await resp.json()
                        items = data.get("items", []) or []
                        for it in items:
                            if it.get("mint") == target_mint:
                                return {
                                    "balance": float(it.get("balance")) if it.get("balance") is not None else None,
                                    "balanceUSD": float(it.get("balanceUSD")) if it.get("balanceUSD") is not None else None,
                                }
                        return {}
                    except Exception as e:
                        logger.warning(f"Failed parsing wallet balances for {creator_address}: {e}")
                        return {}
                else:
                    logger.warning(f"Wallet balances API returned {resp.status} for {creator_address}")
                    return {}
    except Exception as e:
        logger.error(f"Network error fetching wallet balances for {creator_address}: {e}")
        try:
            if close_session:
                await session.close()
        except Exception:
            pass
        return {}
    finally:
        try:
            if 'close_session' in locals() and close_session:
                await session.close()
        except Exception:
            pass


async def fetch_creator_balances_for_mints(creator_addresses: List[str], target_mint: str, *, batch_size: int = 10, batch_interval: float = 1.5) -> Dict[str, dict]:
    """
    Fetch balances for multiple creator addresses. Splits the list into batches (up to batch_size creators per batch),
    ensures at least 1 second between individual API calls and waits `batch_interval` seconds between batches.

    Returns a mapping creator_address -> balance dict (or {} if not found).
    """
    if not creator_addresses:
        return {}

    results: Dict[str, dict] = {}

    # Limit batch size to at most 10 (or provided), but never exceed a safe upper bound
    batch_size = max(1, min(batch_size, 10))

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(creator_addresses), batch_size):
            batch = creator_addresses[i : i + batch_size]
            # Within the batch, perform sequential requests to avoid parallel hammering
            for creator in batch:
                try:
                    res = await fetch_creator_balance_for_mint(creator, target_mint, session=session)
                    results[creator] = res or {}
                except Exception as e:
                    logger.exception(f"Error fetching balance for creator {creator}: {e}")
                    results[creator] = {}

            # If there are more batches to process, wait between batches to reduce rate-limit risk
            if i + batch_size < len(creator_addresses):
                # Sleep at least batch_interval seconds (1-2s recommended)
                try:
                    await asyncio.sleep(batch_interval)
                except Exception:
                    pass

    return results


# Backward compatibility: make the old function use the new batch method for better reliability
async def get_market_activities_by_mint_enhanced(mint_address: str, token_data: Dict = None) -> Dict:
    """
    Enhanced backward-compatible version that uses the new batch endpoint.
    Falls back to the old pool-based method if batch fails.
    
    Args:
        mint_address: The token mint address
        token_data: Token data (kept for backward compatibility but not needed anymore)
        
    Returns:
        Market activity data in the expected format
    """
    try:
        # Try the new batch method first (more reliable)
        return await get_market_activities_by_mint_batch(mint_address)
    except Exception as e:
        logger.warning(f"Batch method failed for {mint_address}, trying legacy method: {e}")
        
        # Fall back to old method if token_data is provided
        if token_data:
            try:
                return await get_market_activities_by_mint(mint_address, token_data)
            except Exception as e2:
                logger.error(f"Legacy method also failed for {mint_address}: {e2}")
                return convert_batch_response_to_legacy_format({})
        else:
            logger.error(f"No token_data provided for fallback method for {mint_address}")
            return convert_batch_response_to_legacy_format({})
