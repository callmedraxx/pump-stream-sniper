import json
import os
from typing import Dict, Optional

import aiohttp
from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()


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
