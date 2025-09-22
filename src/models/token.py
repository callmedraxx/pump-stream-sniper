from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Token(Base):
    """
    Database model for pump.fun tokens with comprehensive market data
    """

    __tablename__ = "tokens"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core token information (required)
    mint_address = Column(
        String(44), unique=True, nullable=False, index=True
    )  # Solana address format
    name = Column(String(100), nullable=False)
    symbol = Column(String(20), nullable=False)
    image_url = Column(Text, nullable=False)
    stream_url = Column(Text, nullable=False)  # Pump.fun stream URL: https://pump.fun/coin/{mint_address}
    age = Column(DateTime, nullable=False)  # Token creation timestamp
    mcap = Column(Float, nullable=False)  # Current market cap
    ath = Column(Float, nullable=False)  # All-time high market cap
    creator = Column(String(44), nullable=False)  # Creator wallet address
    total_supply = Column(Float, nullable=False)
    pump_swap_pool = Column(String(44), nullable=True)  # Pump.fun pool address
    viewers = Column(Integer, nullable=False, default=0)  # Must be >= 0

    # Optional pool information
    raydium_pool = Column(String(44), nullable=True)  # Raydium pool address (optional)

    # Social links (optional)
    twitter = Column(Text, nullable=True)
    telegram = Column(Text, nullable=True)
    website = Column(Text, nullable=True)

    # Calculated fields (required)
    progress = Column(Float, nullable=False)  # (mcap/ath) * 100 percentage
    liquidity = Column(Float, nullable=False, default=0.0)  # Must be >= 0

    # Price changes (required, can be 0)
    price_change_5m = Column(Float, nullable=False, default=0.0)
    price_change_1h = Column(Float, nullable=False, default=0.0)
    price_change_6h = Column(Float, nullable=False, default=0.0)
    price_change_24h = Column(Float, nullable=False, default=0.0)

    # Trader counts (required, can be 0)
    traders_5m = Column(Integer, nullable=False, default=0)
    traders_1h = Column(Integer, nullable=False, default=0)
    traders_6h = Column(Integer, nullable=False, default=0)
    traders_24h = Column(Integer, nullable=False, default=0)

    # Volume data (required)
    volume_5m = Column(Float, nullable=False, default=0.0)
    volume_1h = Column(Float, nullable=False, default=0.0)
    volume_6h = Column(Float, nullable=False, default=0.0)
    volume_24h = Column(Float, nullable=False, default=0.0)

    # Transaction counts (required)
    txns_5m = Column(Integer, nullable=False, default=0)
    txns_1h = Column(Integer, nullable=False, default=0)
    txns_6h = Column(Integer, nullable=False, default=0)
    txns_24h = Column(Integer, nullable=False, default=0)

    # Status flags
    is_active = Column(Boolean, nullable=False, default=True)
    is_live = Column(Boolean, nullable=False, default=False)
    nsfw = Column(Boolean, nullable=False, default=False)

    # Additional metadata
    description = Column(Text, nullable=True)
    metadata_uri = Column(Text, nullable=True)
    video_uri = Column(Text, nullable=True)
    banner_uri = Column(Text, nullable=True)

    # Bonding curve data
    virtual_sol_reserves = Column(Float, nullable=True)
    real_sol_reserves = Column(Float, nullable=True)
    virtual_token_reserves = Column(Float, nullable=True)
    real_token_reserves = Column(Float, nullable=True)
    complete = Column(Boolean, nullable=True)  # Whether bonding curve is complete

    # Activity metrics
    reply_count = Column(Integer, nullable=False, default=0)
    last_reply = Column(DateTime, nullable=True)
    last_trade_timestamp = Column(DateTime, nullable=True)

    # Top holders data
    top_holders = Column(JSON, nullable=True)  # Store top holders as JSON array
    creator_holding_amount = Column(
        Float, nullable=True, default=0.0
    )  # Creator's token holding amount
    creator_holding_percentage = Column(
        Float, nullable=True, default=0.0
    )  # Creator's percentage of total supply
    creator_is_top_holder = Column(
        Boolean, nullable=False, default=False
    )  # Whether creator is in top holders

    # Raw data backup (for debugging and future compatibility)
    raw_data = Column(JSON, nullable=True)

    # Candle data from pump.fun API
    candle_data = Column(JSON, nullable=True)

    # Timestamps
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now()
    )
    live_since = Column(DateTime, nullable=True)

    # Database indexes for fast sorting and querying
    __table_args__ = (
        # Age sorting (newest to oldest by default)
        Index("idx_tokens_age_desc", age.desc()),
        # Market cap sorting (both directions)
        Index("idx_tokens_mcap_desc", mcap.desc()),
        Index("idx_tokens_mcap_asc", mcap.asc()),
        # Viewers sorting (highest to lowest by default)
        Index("idx_tokens_viewers_desc", viewers.desc()),
        Index("idx_tokens_viewers_asc", viewers.asc()),
        # Creator grouping and sorting
        Index("idx_tokens_creator", creator),
        # Transaction count indexes (composite for time periods)
        Index("idx_tokens_txns_24h_desc", txns_24h.desc()),
        Index("idx_tokens_txns_6h_desc", txns_6h.desc()),
        Index("idx_tokens_txns_1h_desc", txns_1h.desc()),
        Index("idx_tokens_txns_5m_desc", txns_5m.desc()),
        # Volume indexes (composite for time periods)
        Index("idx_tokens_volume_24h_desc", volume_24h.desc()),
        Index("idx_tokens_volume_6h_desc", volume_6h.desc()),
        Index("idx_tokens_volume_1h_desc", volume_1h.desc()),
        Index("idx_tokens_volume_5m_desc", volume_5m.desc()),
        # Trader count indexes (composite for time periods)
        Index("idx_tokens_traders_24h_desc", traders_24h.desc()),
        Index("idx_tokens_traders_6h_desc", traders_6h.desc()),
        Index("idx_tokens_traders_1h_desc", traders_1h.desc()),
        Index("idx_tokens_traders_5m_desc", traders_5m.desc()),
        # Combined indexes for common queries
        Index(
            "idx_tokens_age_mcap", age.desc(), mcap.desc()
        ),  # Newest with highest mcap
        Index(
            "idx_tokens_mcap_volume_24h", mcap.desc(), volume_24h.desc()
        ),  # High mcap, high volume
        Index(
            "idx_tokens_viewers_traders_24h", viewers.desc(), traders_24h.desc()
        ),  # Popular tokens
        # Creator holding indexes for fast filtering
        Index("idx_tokens_creator_holding_pct_desc", creator_holding_percentage.desc()),
        Index("idx_tokens_creator_is_top_holder", creator_is_top_holder),
        # Combined indexes for holders analysis
        Index(
            "idx_tokens_creator_holding_analysis",
            creator,
            creator_holding_percentage.desc(),
            creator_is_top_holder,
        ),
    )

    def __repr__(self):
        return f"<Token(mint={self.mint_address[:8]}..., symbol={self.symbol}, mcap={self.mcap})>"

    @property
    def progress_percentage(self) -> float:
        """Calculate progress as percentage of ATH"""
        if self.ath and self.ath > 0:
            return (self.mcap / self.ath) * 100
        return 0.0

    @property
    def social_links(self) -> dict:
        """Return social links as a dictionary"""
        links = {}
        if self.twitter:
            links["twitter"] = self.twitter
        if self.telegram:
            links["telegram"] = self.telegram
        if self.website:
            links["website"] = self.website
        return links
