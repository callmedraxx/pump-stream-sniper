import asyncio
import json
import logging
import os
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

logger = logging.getLogger(__name__)

# The batch endpoint accepts at most 50 addresses per request
MAX_BATCH_SIZE = 50


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
                if response.status == 200:
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
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                if response.status == 200 or response.status == 201:
                    try:
                        data = await response.json()
                        logger.info(f"Successfully fetched market activities for {len(addresses)} tokens")
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
        logger.info(f"Fetching market activities for batch {i//MAX_BATCH_SIZE + 1} ({len(batch)} addresses)")
        
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
