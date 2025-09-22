import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import and_, desc, func, or_
from sqlalchemy.orm import Session

from ..models import Token
from ..services.event_broadcaster import broadcaster

logger = logging.getLogger(__name__)


class TokenService:
    """Service class for Token database operations"""

    def __init__(self, db: Session):
        self.db = db

    def create_token(self, token_data: Dict[str, Any]) -> Token:
        """
        Create a new token in the database

        Args:
            token_data: Dictionary containing token data

        Returns:
            Created Token instance
        """
        try:
            # Check if token already exists
            existing_token = self.get_token_by_mint(token_data.get("mint_address"))
            if existing_token:
                logger.warning(
                    f"Token with mint {token_data.get('mint_address')} already exists"
                )
                return existing_token

            token = Token(**token_data)
            
            # Set live_since if token is live at creation
            if token.is_live:
                token.live_since = datetime.now()
            
            self.db.add(token)
            self.db.commit()
            self.db.refresh(token)
            # Publish token created/updated event for subscribers (SSE/WebSocket)
            try:
                payload = {
                    "type": "token_created",
                    "data": {
                        "mint_address": token.mint_address,
                        "symbol": token.symbol,
                        "name": token.name,
                        "mcap": token.mcap,
                        "ath": token.ath,
                        "progress": token.progress,
                        "dev_activity": token.dev_activity,
                        "created_coin_count": token.created_coin_count,
                        "creator_balance_sol": token.creator_balance_sol,
                        "creator_balance_usd": token.creator_balance_usd,
                        "is_live": token.is_live,
                        "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                    },
                }
                try:
                    asyncio.create_task(broadcaster.publish("token_updated", payload))
                except Exception:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(broadcaster.publish("token_updated", payload))
                    else:
                        loop.run_until_complete(broadcaster.publish("token_updated", payload))
            except Exception:
                logger.debug("Failed to publish token_created event for %s", token.mint_address)
            #logger.info(f"Created token: {token.symbol} ({token.mint_address[:8]}...)")
            return token

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error creating token: {e}")
            raise

    def update_token(
        self, mint_address: str, update_data: Dict[str, Any]
    ) -> Optional[Token]:
        """
        Update an existing token

        Args:
            mint_address: Token mint address
            update_data: Dictionary of fields to update

        Returns:
            Updated Token instance or None if not found
        """
        try:
            token = self.get_token_by_mint(mint_address)
            if not token:
                logger.warning(f"Token with mint {mint_address} not found for update")
                return None

            # Handle live_since timestamp based on is_live changes
            if "is_live" in update_data:
                current_is_live = token.is_live
                new_is_live = update_data["is_live"]
                if not current_is_live and new_is_live:
                    # Token becoming live, set live_since to current time
                    update_data["live_since"] = datetime.now()
                elif current_is_live and not new_is_live:
                    # Token becoming not live, reset live_since
                    update_data["live_since"] = None

            # Update fields
            for key, value in update_data.items():
                if hasattr(token, key):
                    setattr(token, key, value)

            token.updated_at = datetime.now()
            self.db.commit()
            self.db.refresh(token)
            # Publish token update event
            try:
                payload = {
                    "type": "token_updated",
                    "data": {
                        "mint_address": token.mint_address,
                        "symbol": token.symbol,
                        "mcap": token.mcap,
                        "ath": token.ath,
                        "progress": token.progress,
                        "dev_activity": token.dev_activity,
                        "created_coin_count": token.created_coin_count,
                        "creator_balance_sol": token.creator_balance_sol,
                        "creator_balance_usd": token.creator_balance_usd,
                        "is_live": token.is_live,
                        "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                    },
                }
                try:
                    asyncio.create_task(broadcaster.publish("token_updated", payload))
                except Exception:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        loop.create_task(broadcaster.publish("token_updated", payload))
                    else:
                        loop.run_until_complete(broadcaster.publish("token_updated", payload))
            except Exception:
                logger.debug("Failed to publish token_updated event for %s", token.mint_address)
            #logger.info(f"Updated token: {token.symbol}")
            return token

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error updating token {mint_address}: {e}")
            raise

    def get_token_by_mint(self, mint_address: str) -> Optional[Token]:
        """
        Get token by mint address

        Args:
            mint_address: Token mint address

        Returns:
            Token instance or None if not found
        """
        return self.db.query(Token).filter(Token.mint_address == mint_address).first()

    def get_token_by_id(self, token_id: int) -> Optional[Token]:
        """
        Get token by ID

        Args:
            token_id: Token database ID

        Returns:
            Token instance or None if not found
        """
        return self.db.query(Token).filter(Token.id == token_id).first()

    def get_all_tokens(self, limit: int = 100, offset: int = 0) -> List[Token]:
        """
        Get all tokens with pagination

        Args:
            limit: Maximum number of tokens to return
            offset: Number of tokens to skip

        Returns:
            List of Token instances
        """
        return self.db.query(Token).offset(offset).limit(limit).all()

    def get_live_tokens(self) -> List[Token]:
        """
        Get all currently live tokens

        Returns:
            List of live Token instances
        """
        return self.db.query(Token).filter(Token.is_live == True).all()

    def get_tokens_by_creator(self, creator_address: str) -> List[Token]:
        """
        Get all tokens created by a specific address

        Args:
            creator_address: Creator wallet address

        Returns:
            List of Token instances
        """
        return self.db.query(Token).filter(Token.creator == creator_address).all()

    def search_tokens(self, query: str, limit: int = 50) -> List[Token]:
        """
        Search tokens by name or symbol

        Args:
            query: Search query
            limit: Maximum number of results

        Returns:
            List of matching Token instances
        """
        search_filter = f"%{query}%"
        return (
            self.db.query(Token)
            .filter(
                or_(Token.name.ilike(search_filter), Token.symbol.ilike(search_filter))
            )
            .limit(limit)
            .all()
        )

    def get_top_tokens_by_mcap(self, limit: int = 10) -> List[Token]:
        """
        Get top tokens by market cap

        Args:
            limit: Number of tokens to return

        Returns:
            List of Token instances ordered by market cap descending
        """
        return self.db.query(Token).order_by(desc(Token.mcap)).limit(limit).all()

    def get_recent_tokens(self, hours: int = 24) -> List[Token]:
        """
        Get tokens created within the last N hours

        Args:
            hours: Number of hours to look back

        Returns:
            List of recent Token instances
        """
        since = datetime.now() - timedelta(hours=hours)
        return (
            self.db.query(Token)
            .filter(Token.age >= since)
            .order_by(desc(Token.age))
            .all()
        )

    def get_tokens_with_price_change(
        self, timeframe: str = "24h", min_change: float = 0
    ) -> List[Token]:
        """
        Get tokens with significant price changes

        Args:
            timeframe: Timeframe ('5m', '1h', '6h', '24h')
            min_change: Minimum price change percentage

        Returns:
            List of Token instances with significant price changes
        """
        if timeframe == "5m":
            return (
                self.db.query(Token)
                .filter(Token.price_change_5m >= min_change)
                .order_by(desc(Token.price_change_5m))
                .all()
            )
        elif timeframe == "1h":
            return (
                self.db.query(Token)
                .filter(Token.price_change_1h >= min_change)
                .order_by(desc(Token.price_change_1h))
                .all()
            )
        elif timeframe == "6h":
            return (
                self.db.query(Token)
                .filter(Token.price_change_6h >= min_change)
                .order_by(desc(Token.price_change_6h))
                .all()
            )
        elif timeframe == "24h":
            return (
                self.db.query(Token)
                .filter(Token.price_change_24h >= min_change)
                .order_by(desc(Token.price_change_24h))
                .all()
            )
        else:
            return []

    def get_tokens_by_volume(
        self, timeframe: str = "24h", min_volume: float = 0
    ) -> List[Token]:
        """
        Get tokens by trading volume

        Args:
            timeframe: Timeframe ('5m', '1h', '6h', '24h')
            min_volume: Minimum volume in USD

        Returns:
            List of Token instances ordered by volume descending
        """
        if timeframe == "5m":
            return (
                self.db.query(Token)
                .filter(Token.volume_5m >= min_volume)
                .order_by(desc(Token.volume_5m))
                .all()
            )
        elif timeframe == "1h":
            return (
                self.db.query(Token)
                .filter(Token.volume_1h >= min_volume)
                .order_by(desc(Token.volume_1h))
                .all()
            )
        elif timeframe == "6h":
            return (
                self.db.query(Token)
                .filter(Token.volume_6h >= min_volume)
                .order_by(desc(Token.volume_6h))
                .all()
            )
        elif timeframe == "24h":
            return (
                self.db.query(Token)
                .filter(Token.volume_24h >= min_volume)
                .order_by(desc(Token.volume_24h))
                .all()
            )
        else:
            return []

    def delete_token(self, mint_address: str) -> bool:
        """
        Delete a token by mint address

        Args:
            mint_address: Token mint address

        Returns:
            True if deleted, False if not found
        """
        try:
            token = self.get_token_by_mint(mint_address)
            if not token:
                return False

            # Mark token as inactive instead of deleting to preserve history
            token.is_live = False
            token.live_since = None
            token.updated_at = datetime.now()
            self.db.commit()
            self.db.refresh(token)
            return True

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error marking token {mint_address} as inactive: {e}")
            raise

    def bulk_create_tokens(self, tokens_data: List[Dict[str, Any]]) -> List[Token]:
        """
        Bulk create multiple tokens

        Args:
            tokens_data: List of token data dictionaries

        Returns:
            List of created Token instances
        """
        try:
            tokens = []
            for token_data in tokens_data:
                # Skip if token already exists
                if not self.get_token_by_mint(token_data.get("mint_address")):
                    token = Token(**token_data)
                    self.db.add(token)
                    tokens.append(token)

            self.db.commit()

            # Refresh all tokens
            for token in tokens:
                self.db.refresh(token)

            # Publish created events for all newly bulk-created tokens
            for token in tokens:
                try:
                    payload = {
                        "type": "token_created",
                        "data": {
                            "mint_address": token.mint_address,
                            "symbol": token.symbol,
                            "name": token.name,
                            "mcap": token.mcap,
                            "ath": token.ath,
                            "progress": token.progress,
                            "dev_activity": token.dev_activity,
                            "created_coin_count": token.created_coin_count,
                            "creator_balance_sol": token.creator_balance_sol,
                            "creator_balance_usd": token.creator_balance_usd,
                            "is_live": token.is_live,
                            "updated_at": token.updated_at.isoformat() if token.updated_at else None,
                        },
                    }
                    try:
                        asyncio.create_task(broadcaster.publish("token_updated", payload))
                    except Exception:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(broadcaster.publish("token_updated", payload))
                        else:
                            loop.run_until_complete(broadcaster.publish("token_updated", payload))
                except Exception:
                    logger.debug("Failed to publish token_created event for %s", getattr(token, 'mint_address', None))

            logger.info(f"Bulk created {len(tokens)} tokens")
            return tokens

        except Exception as e:
            self.db.rollback()
            logger.error(f"Error in bulk token creation: {e}")
            raise

    def get_token_stats(self) -> Dict[str, Any]:
        """
        Get overall token statistics

        Returns:
            Dictionary with various statistics
        """
        try:
            total_tokens = self.db.query(func.count(Token.id)).scalar()
            live_tokens = (
                self.db.query(func.count(Token.id))
                .filter(Token.is_live == True)
                .scalar()
            )
            total_mcap = self.db.query(func.sum(Token.mcap)).scalar() or 0
            avg_mcap = self.db.query(func.avg(Token.mcap)).scalar() or 0

            return {
                "total_tokens": total_tokens,
                "live_tokens": live_tokens,
                "total_market_cap": total_mcap,
                "average_market_cap": avg_mcap,
                "last_updated": datetime.now(),
            }

        except Exception as e:
            logger.error(f"Error getting token stats: {e}")
            return {}
