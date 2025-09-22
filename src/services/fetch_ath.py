import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Set, Optional, Any

import aiohttp
from sqlalchemy.orm import Session

from ..models import Token
from ..models.database import get_db

logger = logging.getLogger(__name__)

# The external pump.fun batch ATH endpoint accepts at most 100 addresses per request
MAX_API_BATCH_SIZE = 100


class ATHService:
    """
    Service to fetch All-Time High (ATH) data for tokens from pump.fun API in batches
    """

    def __init__(self, db: Session = None, batch_size: int = 150, delay_between_batches: float = 0.5):
        """
        Initialize ATH service
        
        Args:
            db: Database session (if None, will create new sessions as needed)
            batch_size: Number of tokens to fetch ATH for in each batch
            delay_between_batches: Delay in seconds between batch requests to avoid rate limits
        """
        self.db = db
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches
        self.api_url = "https://swap-api.pump.fun/v1/coins/ath/batch"
        self.headers = {
            "Host": "swap-api.pump.fun",
            "Sec-Ch-Ua-Platform": "macOS",
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

    async def fetch_ath_batch(self, addresses: List[str], session: aiohttp.ClientSession) -> Dict[str, Optional[float]]:
        """
        Fetch ATH data for a batch of token addresses
        
        Args:
            addresses: List of mint addresses to fetch ATH for
            session: aiohttp session for making requests
            
        Returns:
            Dictionary mapping mint addresses to their ATH market cap values
        """
        if not addresses:
            return {}

        payload = {
            "addresses": addresses,
            "currency": "USD"
        }

        try:
            logger.info(f"Fetching ATH for batch of {len(addresses)} tokens")
            
            async with session.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                if response.status == 200 or response.status == 201:
                    data = await response.json()
                    result = {}
                    
                    # Process the response - ensure all addresses get a value
                    for address in addresses:
                        if address in data:
                            ath_data = data[address]
                            if ath_data and isinstance(ath_data, dict) and "athMarketCap" in ath_data:
                                ath_value = ath_data["athMarketCap"]
                                # Ensure the value is a valid number
                                if isinstance(ath_value, (int, float)) and ath_value > 0:
                                    result[address] = ath_value
                                else:
                                    result[address] = None
                                    logger.warning(f"Invalid ATH value for token {address}: {ath_value}")
                            else:
                                # Token has no ATH data (null response)
                                result[address] = None
                                logger.warning(f"No ATH data available for token {address}")
                        else:
                            result[address] = None
                            logger.warning(f"Token {address} not found in ATH response")
                    
                    logger.info(f"Successfully fetched ATH for {len([v for v in result.values() if v is not None])} out of {len(addresses)} tokens")
                    return result
                
                elif response.status == 429:
                    logger.warning("Rate limited by ATH API, waiting longer before retry")
                    await asyncio.sleep(5)
                    raise aiohttp.ClientError("Rate limited")
                
                else:
                    logger.error(f"ATH API returned status {response.status}")
                    response_text = await response.text()
                    logger.error(f"Response: {response_text}")
                    raise aiohttp.ClientError(f"HTTP {response.status}")

        except asyncio.TimeoutError:
            logger.error("Timeout while fetching ATH batch")
            raise
        except Exception as e:
            logger.error(f"Error fetching ATH batch: {e}")
            raise

    def split_into_batches(self, addresses: List[str]) -> List[List[str]]:
        """Split list of addresses into batches of specified size"""
        batches = []
        # Ensure we never request more than the API supports
        effective_batch_size = min(self.batch_size, MAX_API_BATCH_SIZE)
        if self.batch_size > MAX_API_BATCH_SIZE:
            logger.warning(f"Configured batch_size={self.batch_size} exceeds API limit {MAX_API_BATCH_SIZE}; clamping to {effective_batch_size}")

        for i in range(0, len(addresses), effective_batch_size):
            batch = addresses[i:i + effective_batch_size]
            batches.append(batch)
        return batches

    async def fetch_all_ath_data(self, addresses: List[str]) -> Dict[str, Optional[float]]:
        """
        Fetch ATH data for all provided addresses in batches
        
        Args:
            addresses: List of mint addresses to fetch ATH for
            
        Returns:
            Dictionary mapping mint addresses to their ATH market cap values
        """
        if not addresses:
            logger.info("No addresses provided for ATH fetching")
            return {}

        logger.info(f"Starting ATH fetch for {len(addresses)} tokens in batches of {self.batch_size}")

        batches = self.split_into_batches(addresses)

        # Trackers for overall results
        all_ath_data: Dict[str, Optional[float]] = {}
        total_updated = 0
        total_failed_updates = 0
        tokens_with_ath = 0
        tokens_without_ath = 0
        successful_batches = 0
        failed_batches = 0

        async with aiohttp.ClientSession() as session:
            for i, batch in enumerate(batches):
                try:
                    logger.info(f"Processing batch {i + 1}/{len(batches)} ({len(batch)} tokens)")

                    # Fetch ATH data for this batch
                    batch_ath_data = await self.fetch_ath_batch(batch, session)

                    # Immediately update DB for this batch as results arrive
                    for addr in batch:
                        ath_value = batch_ath_data.get(addr)
                        all_ath_data[addr] = ath_value

                        if ath_value is not None:
                            tokens_with_ath += 1
                        else:
                            tokens_without_ath += 1

                        try:
                            updated = self.update_token_ath(addr, ath_value)
                            if updated:
                                total_updated += 1
                            else:
                                total_failed_updates += 1
                        except Exception as e:
                            logger.error(f"Error updating token {addr} after ATH fetch: {e}")
                            total_failed_updates += 1

                    successful_batches += 1

                    # Delay between batches to avoid rate limits (except for last batch)
                    if i < len(batches) - 1:
                        logger.debug(f"Waiting {self.delay_between_batches}s before next batch")
                        await asyncio.sleep(self.delay_between_batches)

                except Exception as e:
                    logger.error(f"Failed to fetch/process ATH for batch {i + 1}: {e}")
                    failed_batches += 1

                    # Add None values for failed batch and attempt to update DB with 0
                    for address in batch:
                        all_ath_data[address] = None
                        tokens_without_ath += 1
                        logger.warning(f"Setting ATH to None for failed token: {address}")
                        try:
                            updated = self.update_token_ath(address, None)
                            if updated:
                                total_updated += 1
                            else:
                                total_failed_updates += 1
                        except Exception as ue:
                            logger.error(f"Error updating token {address} after failed batch: {ue}")
                            total_failed_updates += 1

                    # Wait longer after a failed batch
                    if i < len(batches) - 1:
                        await asyncio.sleep(self.delay_between_batches * 2)

        logger.info(
            f"ATH fetch completed: {successful_batches} successful, {failed_batches} failed batches; "
            f"updated={total_updated}, failed_updates={total_failed_updates}, with_ath={tokens_with_ath}, without_ath={tokens_without_ath}"
        )

        # Return the raw ath map for compatibility plus some aggregated stats
        return {
            "ath_map": all_ath_data,
            "stats": {
                "successful_batches": successful_batches,
                "failed_batches": failed_batches,
                "updated_tokens": total_updated,
                "failed_updates": total_failed_updates,
                "tokens_with_ath": tokens_with_ath,
                "tokens_without_ath": tokens_without_ath,
            },
        }

    def update_token_ath(self, mint_address: str, ath_value: Optional[float]) -> bool:
        """
        Update ATH value for a single token in the database
        
        Args:
            mint_address: Token mint address
            ath_value: New ATH market cap value (can be None)
            
        Returns:
            True if update was successful, False otherwise
        """
        try:
            # Use provided db session or create new one
            db = self.db
            should_close_db = False
            
            if db is None:
                db = next(get_db())
                should_close_db = True

            token = db.query(Token).filter(Token.mint_address == mint_address).first()
            
            if token:
                old_ath = token.ath
                
                # Default to 0 if ath_value is None
                if ath_value is None:
                    return False
                new_ath_value = ath_value
                token.ath = new_ath_value
                
                # Recalculate progress percentage
                if token.mcap and new_ath_value > 0:
                    token.progress = (token.mcap / new_ath_value) * 100
                else:
                    token.progress = 0.0
                    
                # Update timestamp
                token.updated_at = datetime.utcnow()
                
                db.commit()
                
                if ath_value is not None:
                    logger.debug(f"Updated ATH for {mint_address}: {old_ath} -> {new_ath_value}")
                else:
                    logger.debug(f"No ATH data for {mint_address}, set to 0: {old_ath} -> 0.0")
                return True
            else:
                logger.warning(f"Token {mint_address} not found in database")
                return False
                
        except Exception as e:
            logger.error(f"Error updating ATH for {mint_address}: {e}")
            if db:
                db.rollback()
            return False
        finally:
            if should_close_db and db:
                db.close()

    async def update_all_ath_from_live_tokens(self) -> Dict[str, Any]:
        """
        Fetch and update ATH data for all live tokens in the database
        
        Returns:
            Statistics about the update process
        """
        start_time = time.time()
        
        # Use provided db session or create new one
        db = self.db
        should_close_db = False
        
        if db is None:
            db = next(get_db())
            should_close_db = True

        try:
            # Get all live tokens
            live_tokens = db.query(Token).filter(Token.is_live == True).all()
            
            if not live_tokens:
                logger.info("No live tokens found in database")
                return {
                    "total_tokens": 0,
                    "updated_tokens": 0,
                    "failed_tokens": 0,
                    "duration_seconds": time.time() - start_time
                }

            logger.info(f"Found {len(live_tokens)} live tokens to update ATH data")
            
            # Extract mint addresses
            addresses = [token.mint_address for token in live_tokens]

            # Fetch ATH data in batches and update DB as each batch returns
            fetch_result = await self.fetch_all_ath_data(addresses)

            # fetch_result contains an "ath_map" and "stats"
            ath_map = fetch_result.get("ath_map", {}) if isinstance(fetch_result, dict) else {}
            batch_stats = fetch_result.get("stats", {}) if isinstance(fetch_result, dict) else {}

            duration = time.time() - start_time

            stats = {
                "total_tokens": len(live_tokens),
                "updated_tokens": batch_stats.get("updated_tokens", 0),
                "failed_tokens": batch_stats.get("failed_updates", 0),
                "tokens_with_ath": batch_stats.get("tokens_with_ath", 0),
                "tokens_without_ath": batch_stats.get("tokens_without_ath", 0),
                "successful_batches": batch_stats.get("successful_batches", 0),
                "failed_batches": batch_stats.get("failed_batches", 0),
                "duration_seconds": round(duration, 2),
            }

            logger.info(f"ATH update completed: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error in update_all_ath_from_live_tokens: {e}")
            raise
        finally:
            if should_close_db and db:
                db.close()

    async def update_ath_for_addresses(self, addresses: List[str]) -> Dict[str, Any]:
        """
        Fetch and update ATH data for specific token addresses
        
        Args:
            addresses: List of mint addresses to update
            
        Returns:
            Statistics about the update process
        """
        if not addresses:
            return {
                "total_tokens": 0,
                "updated_tokens": 0,
                "failed_tokens": 0,
                "duration_seconds": 0
            }

        start_time = time.time()
        logger.info(f"Updating ATH data for {len(addresses)} specific tokens")
        
        # Fetch ATH data for provided addresses
        ath_data = await self.fetch_all_ath_data(addresses)
        
        # Update tokens in database
        updated_count = 0
        failed_count = 0
        
        for mint_address, ath_value in ath_data.items():
            try:
                if self.update_token_ath(mint_address, ath_value):
                    updated_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                logger.error(f"Failed to update ATH for {mint_address}: {e}")
                failed_count += 1

        duration = time.time() - start_time
        
        stats = {
            "total_tokens": len(addresses),
            "updated_tokens": updated_count,
            "failed_tokens": failed_count,
            "duration_seconds": round(duration, 2)
        }
        
        logger.info(f"ATH update completed: {stats}")
        return stats

    def cleanup(self):
        """Cleanup any resources if needed"""
        # Currently no cleanup needed for this service
        pass


# Convenience functions for easy usage

async def fetch_and_update_all_live_ath(batch_size: int = 50, delay: float = 1.0) -> Dict[str, Any]:
    """
    Convenience function to fetch and update ATH for all live tokens
    
    Args:
        batch_size: Number of tokens per batch request
        delay: Delay between batches in seconds
        
    Returns:
        Update statistics
    """
    service = ATHService(batch_size=batch_size, delay_between_batches=delay)
    return await service.update_all_ath_from_live_tokens()


async def fetch_and_update_specific_ath(addresses: List[str], batch_size: int = 50, delay: float = 1.0) -> Dict[str, Any]:
    """
    Convenience function to fetch and update ATH for specific addresses
    
    Args:
        addresses: List of mint addresses to update
        batch_size: Number of tokens per batch request
        delay: Delay between batches in seconds
        
    Returns:
        Update statistics
    """
    service = ATHService(batch_size=batch_size, delay_between_batches=delay)
    return await service.update_ath_for_addresses(addresses)


# Background looping runner to periodically fetch ATH for tokens with complete==True
# Provides start/stop helpers so main.py or other code can run it as a background task
_ath_loop_task: Optional[asyncio.Task] = None


async def run_loop(interval_seconds: int = 60, batch_size: int = 100, delay_between_batches: float = 0.5, stop_event: Optional[asyncio.Event] = None):
    """
    Continuously poll the database for tokens where `complete == True` and update their ATH.

    Args:
        interval_seconds: Seconds to wait between full poll cycles.
        batch_size: Number of tokens to request per batch to the ATH API.
        delay_between_batches: Delay between internal batch requests to avoid rate limits.
        stop_event: Optional asyncio.Event that can be set to stop the loop externally.
    """
    service = ATHService(batch_size=batch_size, delay_between_batches=delay_between_batches)

    logger.info(f"Starting ATH fetch loop: interval={interval_seconds}s, batch_size={batch_size}")

    try:
        while True:
            # allow external stop
            if stop_event and stop_event.is_set():
                logger.info("Stop event set for ATH loop; exiting")
                break

            # Query DB for tokens with complete == True
            db = next(get_db())
            try:
                # Only fetch ATH for tokens that are currently live
                tokens = db.query(Token).filter(Token.is_live == True).all()
                if not tokens:
                    logger.debug("No live tokens found for ATH update")
                else:
                    addresses = [t.mint_address for t in tokens]
                    logger.info(f"Found {len(addresses)} live tokens to update ATH")

                    # Reuse DB session during this fetch so update_token_ath can reuse it
                    service.db = db

                    try:
                        await service.fetch_all_ath_data(addresses)
                    finally:
                        # detach db so update_token_ath will open its own session if called later
                        service.db = None

            except Exception as e:
                logger.error(f"Error while fetching tokens for ATH loop: {e}")
            finally:
                # ensure db closed after each cycle
                try:
                    db.close()
                except Exception:
                    pass

            # Sleep until next cycle or exit early if stop_event is set
            for _ in range(int(interval_seconds)):
                if stop_event and stop_event.is_set():
                    break
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        logger.info("ATH loop task was cancelled")
        raise
    except Exception as e:
        logger.error(f"Unhandled exception in ATH loop: {e}")
        raise
    finally:
        logger.info("ATH fetch loop stopped")


def start_background_loop(*, interval_seconds: int = 60, batch_size: int = 100, delay_between_batches: float = 0.5) -> asyncio.Task:
    """
    Start the ATH loop as a background asyncio task. Returns the created Task.
    If a task is already running, it will be returned instead of creating a new one.
    """
    global _ath_loop_task

    if _ath_loop_task and not _ath_loop_task.done():
        logger.info("ATH loop is already running; returning existing task")
        return _ath_loop_task

    loop = asyncio.get_event_loop()
    _ath_loop_task = loop.create_task(run_loop(interval_seconds=interval_seconds, batch_size=batch_size, delay_between_batches=delay_between_batches))
    logger.info("Started ATH background loop task")
    return _ath_loop_task


async def stop_background_loop():
    """
    Stop the running ATH background loop task (if any) and wait for it to finish.
    """
    global _ath_loop_task
    if not _ath_loop_task:
        return

    task = _ath_loop_task
    _ath_loop_task = None

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("ATH background loop task cancelled successfully")
    except Exception as e:
        logger.error(f"Error while stopping ATH background loop task: {e}")


if __name__ == "__main__":
    # Example usage - update ATH for all live tokens
    async def main():
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        
        stats = await fetch_and_update_all_live_ath(batch_size=50, delay=1.0)
        print(f"ATH Update Results: {json.dumps(stats, indent=2)}")

    asyncio.run(main())
