import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from ..models import Token, get_db
from .event_broadcaster import broadcaster

load_dotenv()

logger = logging.getLogger(__name__)


class TopHoldersService:
    """Service for fetching and managing top holders data"""

    def __init__(self, db: Session, max_concurrent: int = 10, rate_limit: int = 10):
        self.db = db
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limit = rate_limit
        self._last_request_time = asyncio.Queue(maxsize=rate_limit)

    async def _rate_limit(self):
        if self._last_request_time.full():
            oldest_request = await self._last_request_time.get()
            elapsed = asyncio.get_event_loop().time() - oldest_request
            if elapsed < 1:
                await asyncio.sleep(1 - elapsed)
        await self._last_request_time.put(asyncio.get_event_loop().time())

    async def fetch_top_holders(self, mint_address: str) -> Optional[Dict]:
        """
        Fetch top holders for a specific token from pump.fun API
        """
        cookie = os.getenv("COOKIE")
        if not cookie:
            print("❌ Error: Cookie not found in .env file")
            return None

        url = f"https://frontend-api-v3.pump.fun/coins/top-holders/{mint_address}"
        headers = {
            "Host": "frontend-api-v3.pump.fun",
            "Cookie": cookie,
            "Sec-Ch-Ua-Platform": "macOS",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
            "Content-Type": "application/json",
            "Sec-Ch-Ua-Mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Origin": "https://pump.fun",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://pump.fun/",
            "Accept-Encoding": "gzip, deflate, br",
            "Priority": "u=1, i",
        }

        async with self.semaphore:
            await self._rate_limit()
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            data = await response.json()
                            print(f"📊 Fetched top holders for {mint_address[:8]}...")
                            return data
                        else:
                            print(
                                f"❌ Failed to fetch holders for {mint_address[:8]}: {response.status}"
                            )
                            return None
            except Exception as e:
                print(f"❌ Error fetching holders for {mint_address[:8]}: {str(e)}")
                return None

    def analyze_creator_holding(
        self, top_holders: List[Dict], creator_address: str, total_supply: float
    ) -> Tuple[float, float, bool]:
        """
        Analyze if creator is in top holders and calculate their holding percentage

        Returns:
            Tuple[float, float, bool]: (holding_amount, holding_percentage, is_top_holder)
        """
        if not top_holders or not creator_address or not total_supply:
            return 0.0, 0.0, False

        creator_holding = 0.0
        is_top_holder = False

        # Check if creator is in top holders
        for holder in top_holders:
            if holder.get("address") == creator_address:
                creator_holding = holder.get("uiAmount", 0.0)
                is_top_holder = True
                break

        # Calculate percentage
        holding_percentage = (
            (creator_holding / total_supply * 100) if total_supply > 0 else 0.0
        )

        return creator_holding, holding_percentage, is_top_holder

    async def update_token_holders(self, mint_address: str) -> bool:
        """
        Update holders data for a specific token
        """
        try:
            # Fetch holders data
            holders_data = await self.fetch_top_holders(mint_address)
            if not holders_data or not isinstance(holders_data, dict):
                print(f"⚠️ Invalid holders data for {mint_address[:8]}")
                return False

            # Get token from database
            token = (
                self.db.query(Token).filter(Token.mint_address == mint_address).first()
            )
            if not token:
                print(f"⚠️ Token {mint_address[:8]} not found in database")
                return False

            # Extract holders list
            top_holders = holders_data.get("topHolders", {}).get("value", [])

            # Analyze creator holding
            (
                creator_holding_amount,
                creator_holding_percentage,
                creator_is_top_holder,
            ) = self.analyze_creator_holding(
                top_holders, token.creator, token.total_supply
            )

            # Update token with holders data
            token.top_holders = top_holders
            token.creator_holding_amount = creator_holding_amount
            token.creator_holding_percentage = creator_holding_percentage
            token.creator_is_top_holder = creator_is_top_holder

            self.db.commit()
            
            # Publish token_updated event for real-time updates
            try:
                payload = {
                    "type": "token_updated",
                    "data": {
                        "mint_address": token.mint_address,
                        "creator_holding_percentage": token.creator_holding_percentage,
                        "creator_is_top_holder": token.creator_is_top_holder,
                        "creator_holding_amount": token.creator_holding_amount,
                        "is_live": token.is_live,
                        "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                    },
                }
                ok = broadcaster.schedule_publish("token_updated", payload)
                if not ok:
                    logger.warning("broadcaster.schedule_publish returned False for holders update %s", mint_address)
            except Exception:
                logger.exception("Failed to publish token_updated from fetch_top_holders for %s", mint_address)

            print(
                f"✅ Updated holders for {token.symbol} ({mint_address[:8]}): "
                f"Creator holds {creator_holding_percentage:.2f}% ({creator_is_top_holder})"
            )

            return True

        except Exception as e:
            print(f"❌ Error updating holders for {mint_address[:8]}: {str(e)}")
            self.db.rollback()
            return False

    async def update_multiple_tokens_holders(
        self, mint_addresses: List[str]
    ) -> Dict[str, bool]:
        """
        Update holders data for multiple tokens concurrently with rate limit handling
        """
        print(f"🔄 Updating holders for {len(mint_addresses)} tokens...")

        results = {}
        batch_size = 60  # Maximum requests per minute
        delay_between_batches = 60  # Seconds to wait between batches

        for i in range(0, len(mint_addresses), batch_size):
            batch = mint_addresses[i : i + batch_size]
            print(
                f"🔄 Processing batch {i // batch_size + 1} with {len(batch)} tokens..."
            )

            tasks = []
            for mint_address in batch:
                task = asyncio.create_task(self.update_token_holders(mint_address))
                tasks.append((mint_address, task))

            for mint_address, task in tasks:
                try:
                    success = await task
                    results[mint_address] = success
                except Exception as e:
                    print(f"❌ Task failed for {mint_address[:8]}: {str(e)}")
                    results[mint_address] = False

            successful = sum(1 for success in results.values() if success)
            print(f"📊 Batch complete: {successful}/{len(batch)} successful")

            # Delay before processing the next batch if there are more tokens
            if i + batch_size < len(mint_addresses):
                print(f"⏳ Waiting {delay_between_batches} seconds before next batch...")
                await asyncio.sleep(delay_between_batches)

        print(
            f"📊 All batches complete: {sum(1 for success in results.values() if success)}/{len(mint_addresses)} successful"
        )

        return results


async def fetch_top_holders_for_token(mint_address: str) -> Optional[Dict]:
    """
    Standalone function to fetch top holders for a single token
    """
    db = next(get_db())
    service = TopHoldersService(db)
    return await service.fetch_top_holders(mint_address)


async def update_token_holders_data(mint_address: str) -> bool:
    """
    Standalone function to update holders data for a token
    """
    db = next(get_db())
    service = TopHoldersService(db)
    return await service.update_token_holders(mint_address)
