import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Set

from sqlalchemy import and_, desc, or_
from sqlalchemy.orm import Session

from ..models import Token
from ..services.fetch_candles import CandleFetchService
from ..services.fetch_activities import fetch_market_activities, fetch_market_activities_for_addresses
from ..services.fetch_activities import fetch_creator_trades_for_mint, fetch_creator_created_count, fetch_creator_balance_for_mint
from ..services.event_broadcaster import broadcaster
# TEMPORARILY DISABLED: from ..services.fetch_top_holders import TopHoldersService
from ..services.token_service import TokenService

logger = logging.getLogger(__name__)


class DatabaseSyncService:
    """
    High-performance database synchronization service for pump.fun tokens
    Handles parallel processing, real-time updates, and data consistency
    """

    def __init__(self, db: Session, max_workers: int = 20):
        self.db = db
        self.token_service = TokenService(db)
        # TEMPORARILY DISABLED: self.holders_service = TopHoldersService(
        #     db, max_concurrent=max_workers // 2
        # )  # Use half the workers for holders
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="token_sync"
        )

    def _normalize_price_change(self, val) -> float:
        """Normalize incoming priceChangePercent to decimal fraction.

        Converts values like 85.22 -> 0.8522. Keeps small fractional values as-is.
        Returns 0.0 for invalid input.
        """
        try:
            v = float(val)
        except Exception:
            return 0.0

        # If API returns a percent like 85.22 (meaning 85.22%), convert to fraction 0.8522
        # If API returns a small number like 0.85 or -0.17, assume it's already fraction
        if abs(v) > 1 and abs(v) < 10000:
            return v / 100.0
        return v

    async def sync_live_tokens(
        self, live_tokens_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Synchronize live tokens data with database in parallel

        Args:
            live_tokens_data: List of token data from fetch_live

        Returns:
            Sync statistics
        """
        start_time = time.time()

        logger.info(f"Starting sync with {len(live_tokens_data)} tokens using {self.max_workers} workers")

        # Extract mint addresses from live data
        live_mint_addresses = {
            token.get("mint") for token in live_tokens_data if token.get("mint")
        }

        # Get existing tokens from database
        existing_tokens = self.token_service.get_all_tokens(
            limit=10000
        )  # Get all for comparison
        existing_mint_addresses = {token.mint_address for token in existing_tokens}

        # Calculate differences
        new_mints = live_mint_addresses - existing_mint_addresses
        removed_mints = existing_mint_addresses - live_mint_addresses
        updated_mints = live_mint_addresses & existing_mint_addresses

        logger.info(
            f"Sync stats: {len(new_mints)} new, {len(removed_mints)} marked inactive, {len(updated_mints)} updated"
        )

        # Process in parallel
        tasks = []

        # Add new tokens
        if new_mints:
            new_tokens_data = [
                token for token in live_tokens_data if token.get("mint") in new_mints
            ]
            tasks.append(self._add_new_tokens_parallel(new_tokens_data))

        # Update existing tokens
        if updated_mints:
            existing_tokens_dict = {
                token.mint_address: token for token in existing_tokens
            }
            updated_tokens_data = [
                token
                for token in live_tokens_data
                if token.get("mint") in updated_mints
            ]
            tasks.append(
                self._update_existing_tokens_parallel(
                    updated_tokens_data, existing_tokens_dict
                )
            )

        # Mark old tokens as inactive
        if removed_mints:
            tasks.append(self._remove_old_tokens_parallel(list(removed_mints)))

        # Execute all tasks concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

            # TEMPORARILY DISABLED: Update holders data for all live tokens (run in background)
            # if live_mint_addresses:
            #     # Don't await this - let it run in background for better performance
            #     asyncio.create_task(
            #         self.update_holders_for_tokens(list(live_mint_addresses))
            #     )

            sync_time = time.time() - start_time
            stats = {
                "sync_time_seconds": round(sync_time, 2),
                "new_tokens": len(new_mints),
                "marked_inactive_tokens": len(removed_mints),
                "updated_tokens": len(updated_mints),
                "total_live_tokens": len(live_tokens_data),
                "tokens_per_second": (
                    round(len(live_tokens_data) / sync_time, 2) if sync_time > 0 else 0
                ),
            }

            # logger.info(
            #     f"Sync completed in {stats['sync_time_seconds']}s - {stats['tokens_per_second']} tokens/sec"
            # )

            # Fetch and update candle data for live tokens
            try:
                await self._update_candle_data_for_live_tokens()
            except Exception as e:
                logger.error(f"Error updating candle data: {e}")

            # Publish sync completed event so subscribers (SSE/websocket) can react immediately
            try:
                logger.info("DatabaseSyncService publishing sync_completed: %s", stats)
                # publish asynchronously but don't await if no loop; safest is to schedule
                asyncio.create_task(broadcaster.publish("sync_completed", stats))
            except Exception:
                # If create_task fails (no running loop), try direct publish (sync)
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(broadcaster.publish("sync_completed", stats))
                    else:
                        # fallback: run until complete
                        loop.run_until_complete(broadcaster.publish("sync_completed", stats))
                except Exception:
                    pass

            return stats

    async def _add_new_tokens_parallel(self, new_tokens_data: List[Dict[str, Any]]):
        """Add new tokens using batch market activities fetching"""
        if not new_tokens_data:
            return

        logger.info(f"Adding {len(new_tokens_data)} new tokens using batch market activities")

        # Extract mint addresses for batch fetching
        mint_addresses = [token.get("mint") for token in new_tokens_data if token.get("mint")]
        
        # Fetch market activities for all new tokens in batches
        batch_activities = {}
        if mint_addresses:
            try:
                batch_activities = await fetch_market_activities_for_addresses(mint_addresses)
                #logger.info(f"Successfully fetched market activities for {len(batch_activities)} tokens")
            except Exception as e:
                logger.error(f"Failed to fetch batch market activities for new tokens: {e}")
                batch_activities = {}

        # Create tokens with their market activities
        semaphore = asyncio.Semaphore(self.max_workers)

        async def create_single_token(token_data):
            async with semaphore:
                try:
                    mint_address = token_data.get("mint")
                    market_activities = batch_activities.get(mint_address, {})

                    # Fetch dev/creator last activity once at creation
                    dev_activity = {}
                    creator_addr = token_data.get("creator")
                    if creator_addr and mint_address:
                        try:
                            dev_activity = await fetch_creator_trades_for_mint(mint_address, creator_addr)
                        except Exception as e:
                            logger.warning(f"Failed to fetch creator trades for {mint_address}: {e}")
                    
                    # Create token data with activities
                    token_dict = self._prepare_token_data(token_data, market_activities)
                    if dev_activity:
                        token_dict["dev_activity"] = dev_activity
                    # Fetch creator created coin count once at creation
                    if creator_addr:
                        try:
                            created_count = await fetch_creator_created_count(creator_addr)
                            # API returns integer count; enforce business rule: cannot be None or 0
                            try:
                                created_count_int = int(created_count or 0)
                            except Exception:
                                created_count_int = 0

                            if created_count_int <= 0:
                                # If API reports 0, set to 1 per business rule
                                created_count_int = 1

                            token_dict["created_coin_count"] = created_count_int
                            logger.info(f"Initial created_coin_count for new token {mint_address} set to {created_count_int} (creator={creator_addr})")
                        except Exception as e:
                            logger.warning(f"Failed to fetch created_coin_count for creator {creator_addr}: {e}")
                    # Fetch creator balances for this mint at creation
                    if creator_addr and mint_address:
                        try:
                            balance_info = await fetch_creator_balance_for_mint(creator_addr, mint_address)
                            if balance_info:
                                token_dict["creator_balance_sol"] = balance_info.get("balance")
                                token_dict["creator_balance_usd"] = balance_info.get("balanceUSD")
                                logger.info(
                                    f"Initial creator balance for new token {mint_address} (creator={creator_addr}): "
                                    f"sol={token_dict.get('creator_balance_sol')} usd={token_dict.get('creator_balance_usd')}"
                                )
                        except Exception as e:
                            logger.warning(f"Failed to fetch creator balance for {creator_addr}/{mint_address}: {e}")
                    
                    # Create token in database
                    self.token_service.create_token(token_dict)
                    logger.info(f"Created token: {token_data.get('symbol')} mint={mint_address} created_coin_count={token_dict.get('created_coin_count')} creator_balance_sol={token_dict.get('creator_balance_sol')} creator_balance_usd={token_dict.get('creator_balance_usd')}")
                    
                except Exception as e:
                    logger.error(f"Error creating token {token_data.get('mint') if isinstance(token_data, dict) else 'unknown'}: {e}")

        # Execute token creation in parallel
        tasks = [create_single_token(token_data) for token_data in new_tokens_data]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _update_existing_tokens_parallel(
        self,
        updated_tokens_data: List[Dict[str, Any]],
        existing_tokens_dict: Dict[str, Token],
    ):
        """Update existing tokens using batch market activities fetching"""
        if not updated_tokens_data:
            return

        logger.info(f"Updating {len(updated_tokens_data)} existing tokens using batch market activities")

        # Extract mint addresses for batch fetching
        mint_addresses = [token.get("mint") for token in updated_tokens_data if token.get("mint")]
        
        # Fetch market activities for all updated tokens in batches
        batch_activities = {}
        if mint_addresses:
            try:
                batch_activities = await fetch_market_activities_for_addresses(mint_addresses)
                #logger.info(f"Successfully fetched market activities for {len(batch_activities)} existing tokens")
            except Exception as e:
                logger.error(f"Failed to fetch batch market activities for existing tokens: {e}")
                batch_activities = {}

        # Update tokens with their market activities
        semaphore = asyncio.Semaphore(self.max_workers)

        async def update_single_token(token_data):
            async with semaphore:
                try:
                    if token_data is None:
                        logger.error("Received None for token_data, skipping this task.")
                        return

                    mint_address = token_data.get("mint")
                    existing_token = existing_tokens_dict.get(mint_address)
                    
                    if not existing_token:
                        logger.warning(f"No existing token found for mint {mint_address}")
                        return

                    market_activities = batch_activities.get(mint_address, {})
                    
                    # Prepare update data
                    update_data = self._prepare_update_data(token_data, market_activities)
                    
                    # Handle live_since timestamp logic
                    new_is_live = update_data.get("is_live", False)
                    if new_is_live and not existing_token.is_live:
                        # Token is becoming live, set live_since
                        update_data["live_since"] = datetime.now()
                    elif not new_is_live and existing_token.is_live:
                        # Token is becoming inactive, reset live_since
                        update_data["live_since"] = None
                    
                    # Check if any fields actually changed
                    if self._has_changes(existing_token, update_data):
                        self.token_service.update_token(mint_address, update_data)
                        logger.debug(f"Updated token: {token_data.get('symbol')}")
                    else:
                        logger.debug(f"No changes for token: {token_data.get('symbol')}")

                except Exception as e:
                    logger.error(f"Error updating token {token_data.get('mint') if isinstance(token_data, dict) else 'unknown'}: {e}")

        # Execute token updates in parallel  
        tasks = [update_single_token(token_data) for token_data in updated_tokens_data]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _remove_old_tokens_parallel(self, removed_mints: List[str]):
        """Mark tokens that are no longer live as inactive"""
        #logger.info("Marking %d old tokens as inactive", len(removed_mints))

        if not removed_mints:
            return

        try:
            # Use a single bulk UPDATE to set is_live=False for efficiency
            updated = (
                self.db.query(Token)
                .filter(Token.mint_address.in_(removed_mints))
                .update({"is_live": False, "live_since": None}, synchronize_session=False)
            )
            self.db.commit()
            #logger.info("Bulk marked %d tokens as inactive", updated)
        except Exception as e:
            logger.exception("Bulk update failed, falling back to per-mint updates: %s", e)
            # Fallback: update individually (slower) using token_service to preserve existing behavior
            for mint in removed_mints:
                try:
                    self.token_service.update_token(mint, {"is_live": False})
                    logger.debug("Marked token as inactive: %s", mint)
                except Exception as ex:
                    logger.error("Error marking token %s as inactive: %s", mint, ex)

    async def _update_candle_data_for_live_tokens(self):
        """Fetch and update candle data for all live tokens"""
        try:
            # Get all live tokens
            live_tokens = self.token_service.get_live_tokens()
            if not live_tokens:
                logger.debug("No live tokens found for candle data update")
                return

            logger.info(f"Updating candle data for {len(live_tokens)} live tokens")

            # Create candle fetch service
            candle_service = CandleFetchService()

            try:
                # Fetch candle data for all live tokens
                candle_data_dict = await candle_service.fetch_candles_for_live_tokens(live_tokens)

                # Ensure we have a dict (fetcher may return None on catastrophic failure)
                if not candle_data_dict:
                    logger.warning("Candle fetcher returned no data for any tokens")
                    candle_data_dict = {}

                # Update tokens with candle data
                updated_count = 0
                for token in live_tokens:
                    mint_address = token.mint_address
                    candle_data = candle_data_dict.get(mint_address)
                    if candle_data is not None:
                        # Persist even empty lists (if service returned []) or actual data
                        self.token_service.update_token(mint_address, {"candle_data": candle_data})
                        updated_count += 1

                logger.info(f"Updated candle data for {updated_count}/{len(live_tokens)} live tokens")

            finally:
                await candle_service.close()

        except Exception as e:
            logger.error(f"Error in candle data update: {e}")
            raise

    def _prepare_token_data(
        self, token_data: Dict[str, Any], market_activities: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Prepare token data for database insertion"""
        # Calculate progress
        mcap = token_data.get("usd_market_cap", token_data.get("market_cap", 0)) or 0
        ath = token_data.get("ath_market_cap", mcap) or mcap
        progress = (mcap / ath * 100) if ath > 0 else 0

        # Calculate age
        created_ts = token_data.get("created_timestamp", 0) or 0
        age = datetime.fromtimestamp(created_ts / 1000) if created_ts > 0 else datetime.now()

        # Extract market activity data
        activities_5m = (market_activities.get("5m") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_1h = (market_activities.get("1h") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_6h = (market_activities.get("6h") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_24h = (market_activities.get("24h") or {}) if market_activities and isinstance(market_activities, dict) else {}

    # Use the class-level normalizer to convert incoming percent values to decimal fractions

        return {
            # Core information (static fields) - handle nulls
            "mint_address": token_data.get("mint", ""),
            "name": token_data.get("name", ""),
            "symbol": token_data.get("symbol", ""),
            "image_url": token_data.get("image_uri", ""),
            "stream_url": f"https://pump.fun/coin/{token_data.get('mint', '')}",
            "creator": token_data.get("creator", ""),
            "total_supply": token_data.get("total_supply", 0) or 0,
            "pump_swap_pool": token_data.get("pump_swap_pool", ""),
            # Social links (static)
            "twitter": token_data.get("twitter"),
            "telegram": token_data.get("telegram"),
            "website": token_data.get("website"),
            # Dynamic fields (updated frequently) - handle nulls
            "age": age,
            "mcap": mcap,
            "ath": ath,
            "progress": progress,
            "viewers": token_data.get("num_participants", 0) or 0,
            "liquidity": token_data.get("virtual_sol_reserves", 0) or 0,
            # Price changes - normalize and default to 0. Stored as decimal fraction (e.g., 0.85 for 85%)
            "price_change_5m": self._normalize_price_change(activities_5m.get("priceChangePercent", 0) or 0),
            "price_change_1h": self._normalize_price_change(activities_1h.get("priceChangePercent", 0) or 0),
            "price_change_6h": self._normalize_price_change(activities_6h.get("priceChangePercent", 0) or 0),
            "price_change_24h": self._normalize_price_change(activities_24h.get("priceChangePercent", 0) or 0),
            # Trader counts - defaults to 0
            "traders_5m": activities_5m.get("numUsers", 0) or 0,
            "traders_1h": activities_1h.get("numUsers", 0) or 0,
            "traders_6h": activities_6h.get("numUsers", 0) or 0,
            "traders_24h": activities_24h.get("numUsers", 0) or 0,
            # Volume data - defaults to 0
            "volume_5m": activities_5m.get("volumeUSD", 0) or 0,
            "volume_1h": activities_1h.get("volumeUSD", 0) or 0,
            "volume_6h": activities_6h.get("volumeUSD", 0) or 0,
            "volume_24h": activities_24h.get("volumeUSD", 0) or 0,
            # Transaction counts - defaults to 0
            "txns_5m": activities_5m.get("numTxs", 0) or 0,
            "txns_1h": activities_1h.get("numTxs", 0) or 0,
            "txns_6h": activities_6h.get("numTxs", 0) or 0,
            "txns_24h": activities_24h.get("numTxs", 0) or 0,
            # Status flags - handle nulls
            "is_live": token_data.get("is_currently_live", False) or False,
            "nsfw": token_data.get("nsfw", False) or False,
            # Set live_since timestamp when token becomes live
            "live_since": datetime.now() if (token_data.get("is_currently_live", False) or False) else None,
            # Optional fields
            "raydium_pool": token_data.get("raydium_pool"),
            "description": token_data.get("description"),
            "metadata_uri": token_data.get("metadata_uri"),
            "video_uri": token_data.get("video_uri"),
            "banner_uri": token_data.get("banner_uri"),
            # Bonding curve data - handle nulls
            "virtual_sol_reserves": token_data.get("virtual_sol_reserves"),
            "real_sol_reserves": token_data.get("real_sol_reserves"),
            "virtual_token_reserves": token_data.get("virtual_token_reserves"),
            "real_token_reserves": token_data.get("real_token_reserves"),
            "complete": token_data.get("complete"),
            # Activity metrics - handle nulls
            "reply_count": token_data.get("reply_count", 0) or 0,
            "last_reply": (
                datetime.fromtimestamp(token_data.get("last_reply", 0) / 1000)
                if token_data.get("last_reply")
                else None
            ),
            "last_trade_timestamp": (
                datetime.fromtimestamp(token_data.get("last_trade_timestamp", 0) / 1000)
                if token_data.get("last_trade_timestamp")
                else None
            ),
            # Raw data backup
            "raw_data": token_data,
        }

    def _prepare_update_data(
        self, token_data: Dict[str, Any], market_activities: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Prepare update data for existing tokens (only dynamic fields)"""
        # Calculate progress
        mcap = token_data.get("usd_market_cap", token_data.get("market_cap", 0)) or 0

        # Extract market activity data
        activities_5m = (market_activities.get("5m") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_1h = (market_activities.get("1h") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_6h = (market_activities.get("6h") or {}) if market_activities and isinstance(market_activities, dict) else {}
        activities_24h = (market_activities.get("24h") or {}) if market_activities and isinstance(market_activities, dict) else {}

    # Use class-level normalizer for incoming percent values

        return {
            # Dynamic fields only (static fields don't change)
            # Note: mcap is now updated only from websocket trades, not from fetch_live
            #"mcap": mcap,
            "viewers": token_data.get("num_participants", 0) or 0,
            "liquidity": token_data.get("virtual_sol_reserves", 0) or 0,
            # Price changes - normalize and default to 0. Stored as decimal fraction (e.g., 0.85 for 85%)
            "price_change_5m": self._normalize_price_change(activities_5m.get("priceChangePercent", 0) or 0),
            "price_change_1h": self._normalize_price_change(activities_1h.get("priceChangePercent", 0) or 0),
            "price_change_6h": self._normalize_price_change(activities_6h.get("priceChangePercent", 0) or 0),
            "price_change_24h": self._normalize_price_change(activities_24h.get("priceChangePercent", 0) or 0),
            # Trader counts - defaults to 0
            "traders_5m": activities_5m.get("numUsers", 0) or 0,
            "traders_1h": activities_1h.get("numUsers", 0) or 0,
            "traders_6h": activities_6h.get("numUsers", 0) or 0,
            "traders_24h": activities_24h.get("numUsers", 0) or 0,
            # Volume data - defaults to 0
            "volume_5m": activities_5m.get("volumeUSD", 0) or 0,
            "volume_1h": activities_1h.get("volumeUSD", 0) or 0,
            "volume_6h": activities_6h.get("volumeUSD", 0) or 0,
            "volume_24h": activities_24h.get("volumeUSD", 0) or 0,
            # Transaction counts - defaults to 0
            "txns_5m": activities_5m.get("numTxs", 0) or 0,
            "txns_1h": activities_1h.get("numTxs", 0) or 0,
            "txns_6h": activities_6h.get("numTxs", 0) or 0,
            "txns_24h": activities_24h.get("numTxs", 0) or 0,
            # Status flags - handle nulls
            "is_live": token_data.get("is_currently_live", False) or False,
            # Activity metrics - handle nulls
            "reply_count": token_data.get("reply_count", 0) or 0,
            "last_reply": (
                datetime.fromtimestamp(token_data.get("last_reply", 0) / 1000)
                if token_data.get("last_reply")
                else None
            ),
            "last_trade_timestamp": (
                datetime.fromtimestamp(token_data.get("last_trade_timestamp", 0) / 1000)
                if token_data.get("last_trade_timestamp")
                else None
            ),
            # Bonding curve data - handle nulls
            "virtual_sol_reserves": token_data.get("virtual_sol_reserves"),
            "real_sol_reserves": token_data.get("real_sol_reserves"),
            "virtual_token_reserves": token_data.get("virtual_token_reserves"),
            "real_token_reserves": token_data.get("real_token_reserves"),
            "complete": token_data.get("complete"),
            # Raw data backup
            "raw_data": token_data,
        }

    def _has_changes(self, existing_token: Token, update_data: Dict[str, Any]) -> bool:
        """Check if any fields have actually changed"""
        # Check dynamic fields for changes
        dynamic_fields = [
            "mcap",
            "viewers",
            "liquidity",
            "price_change_5m",
            "price_change_1h",
            "price_change_6h",
            "price_change_24h",
            "traders_5m",
            "traders_1h",
            "traders_6h",
            "traders_24h",
            "volume_5m",
            "volume_1h",
            "volume_6h",
            "volume_24h",
            "txns_5m",
            "txns_1h",
            "txns_6h",
            "txns_24h",
            "is_live",
            "live_since",
            "reply_count",
            "last_reply",
            "last_trade_timestamp",
            "virtual_sol_reserves",
            "real_sol_reserves",
            "complete",
        ]

        for field in dynamic_fields:
            if field in update_data:
                current_value = getattr(existing_token, field)
                new_value = update_data[field]

                # Handle None comparisons
                if current_value != new_value:
                    if not (current_value is None and new_value == 0):
                        if not (current_value == 0 and new_value is None):
                            return True

        return False

    # TEMPORARILY DISABLED: async def update_holders_for_tokens(
    #     self, mint_addresses: List[str]
    # ) -> Dict[str, bool]:
    #     """
    #     Update holders data for specified tokens
    #     """
    #     if not mint_addresses:
    #         return {}

    #     logger.info(f"Updating holders for {len(mint_addresses)} tokens")
    #     return await self.holders_service.update_multiple_tokens_holders(mint_addresses)

    def cleanup(self):
        """Cleanup resources"""
        if self.executor:
            self.executor.shutdown(wait=True)
