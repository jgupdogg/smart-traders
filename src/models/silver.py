"""
Silver layer SQLModel schemas for curated and processed data.
These models represent the refined data after initial filtering and processing.
"""

from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, String, DateTime, Float, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB


class SilverToken(SQLModel, table=True):
    """Silver tokens table - selected tokens for tracking."""
    
    __tablename__ = "silver_tokens"
    __table_args__ = (
        Index("idx_silver_tokens_selected_at", "selected_at"),
        Index("idx_silver_tokens_whales_processed", "whales_processed"),
        Index("idx_silver_tokens_selection_score", "selection_score"),
        {"schema": "silver"}
    )
    
    # Primary key
    token_address: str = Field(primary_key=True, max_length=44, description="Solana token mint address")
    
    # Selection metadata
    selection_score: Optional[float] = Field(default=None, description="Score used for token selection")
    selection_reason: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON metadata about why token was selected")
    selected_at: datetime = Field(default_factory=datetime.utcnow, description="When token was selected")
    
    # Processing state for downstream tasks
    whales_processed: bool = Field(default=False, description="Whether whale data has been fetched")
    whales_processed_at: Optional[datetime] = Field(default=None, description="When whale processing completed")
    
    # Denormalized token data for convenience (copied from bronze)
    symbol: Optional[str] = Field(default=None, max_length=50, description="Token symbol")
    name: Optional[str] = Field(default=None, max_length=255, description="Token name")
    market_cap: Optional[float] = Field(default=None, description="Market cap at selection time")
    price_change_24h_percent: Optional[float] = Field(default=None, description="24h price change at selection")
    volume_24h_usd: Optional[float] = Field(default=None, description="24h volume at selection time")
    liquidity: Optional[float] = Field(default=None, description="Liquidity at selection time")
    holder_count: Optional[int] = Field(default=None, description="Number of holders at selection time")
    price: Optional[float] = Field(default=None, description="Price at selection time")


class SilverWhale(SQLModel, table=True):
    """Silver whales table - unique whale addresses for tracking."""
    
    __tablename__ = "silver_whales"
    __table_args__ = (
        Index("idx_silver_whales_first_seen", "first_seen_at"),
        Index("idx_silver_whales_transactions_processed", "transactions_processed"),
        Index("idx_silver_whales_pnl_processed", "pnl_processed"),
        Index("idx_silver_whales_tokens_held", "tokens_held_count"),
        {"schema": "silver"}
    )
    
    # Primary key
    wallet_address: str = Field(primary_key=True, max_length=44, description="Unique wallet address")
    
    # Whale metadata
    tokens_held_count: Optional[int] = Field(default=None, description="Number of different tokens held")
    total_value_held: Optional[float] = Field(default=None, description="Total USD value of holdings")
    first_seen_at: datetime = Field(default_factory=datetime.utcnow, description="When whale was first discovered")
    last_updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last time whale data was updated")
    
    # Processing state tracking
    transactions_processed: bool = Field(default=False, description="Whether transactions have been fetched")
    transactions_processed_at: Optional[datetime] = Field(default=None, description="When transaction processing completed")
    
    pnl_processed: bool = Field(default=False, description="Whether PnL has been calculated")
    pnl_processed_at: Optional[datetime] = Field(default=None, description="When PnL processing completed")
    
    # Additional metadata
    source_tokens: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON list of tokens where this whale was found")


class SilverWalletPnL(SQLModel, table=True):
    """Silver wallet PnL table - calculated trading performance metrics."""
    
    __tablename__ = "silver_wallet_pnl"
    __table_args__ = (
        Index("idx_silver_wallet_pnl_realized_pnl", "total_realized_pnl_usd"),
        Index("idx_silver_wallet_pnl_win_rate", "win_rate_percent"),
        Index("idx_silver_wallet_pnl_trade_count", "total_trades"),
        Index("idx_silver_wallet_pnl_last_calculated", "last_calculated_at"),
        Index("idx_silver_wallet_pnl_smart_trader_eligible", "smart_trader_eligible"),
        {"schema": "silver"}
    )
    
    # Primary key
    wallet_address: str = Field(primary_key=True, max_length=44, description="Wallet address")
    
    # Core PnL metrics
    total_realized_pnl_usd: Optional[float] = Field(default=None, description="Total realized profit/loss in USD")
    total_unrealized_pnl_usd: Optional[float] = Field(default=None, description="Total unrealized profit/loss in USD")
    
    # Trading performance metrics
    win_rate_percent: Optional[float] = Field(default=None, description="Percentage of profitable trades")
    avg_win_usd: Optional[float] = Field(default=None, description="Average profit per winning trade")
    avg_loss_usd: Optional[float] = Field(default=None, description="Average loss per losing trade")
    trade_frequency_per_day: Optional[float] = Field(default=None, description="Average trades per day")
    
    # Trade statistics
    total_trades: Optional[int] = Field(default=None, description="Total number of trades")
    winning_trades: Optional[int] = Field(default=None, description="Number of profitable trades")
    losing_trades: Optional[int] = Field(default=None, description="Number of losing trades")
    
    # Portfolio metrics
    tokens_traded_count: Optional[int] = Field(default=None, description="Number of different tokens traded")
    total_volume_usd: Optional[float] = Field(default=None, description="Total trading volume in USD")
    avg_trade_size_usd: Optional[float] = Field(default=None, description="Average trade size in USD")
    
    # Time-based metrics
    first_trade_at: Optional[datetime] = Field(default=None, description="Timestamp of first trade")
    last_trade_at: Optional[datetime] = Field(default=None, description="Timestamp of last trade")
    trading_days: Optional[int] = Field(default=None, description="Number of days with trading activity")
    
    # Risk metrics
    max_drawdown_percent: Optional[float] = Field(default=None, description="Maximum drawdown percentage")
    sharpe_ratio: Optional[float] = Field(default=None, description="Risk-adjusted return ratio")
    
    # Smart trader qualification
    smart_trader_eligible: bool = Field(default=False, description="Whether wallet qualifies as smart trader")
    smart_trader_score: Optional[float] = Field(default=None, description="Composite score for smart trader ranking")
    
    # Metadata
    last_calculated_at: datetime = Field(default_factory=datetime.utcnow, description="When PnL was last calculated")
    calculation_method: str = Field(default="fifo", max_length=20, description="Method used for PnL calculation")
    tokens_analyzed: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON metadata about tokens included in analysis")