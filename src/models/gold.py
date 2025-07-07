"""
Gold layer SQLModel schemas for final analyzed and aggregated data.
These models represent the final output layer with smart trader rankings.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, String, DateTime, Float, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB


class SmartTrader(SQLModel, table=True):
    """Smart traders table - final ranked traders based on performance."""
    
    __tablename__ = "smart_traders"
    __table_args__ = (
        Index("idx_smart_traders_performance_tier", "performance_tier"),
        Index("idx_smart_traders_composite_score", "composite_score"),
        Index("idx_smart_traders_total_pnl", "total_realized_pnl_usd"),
        Index("idx_smart_traders_win_rate", "win_rate_percent"),
        Index("idx_smart_traders_last_updated", "last_updated_at"),
        {"schema": "gold"}
    )
    
    # Primary key
    wallet_address: str = Field(primary_key=True, max_length=44, description="Wallet address")
    
    # Performance tier and ranking
    performance_tier: str = Field(max_length=20, description="Performance tier (elite, strong, promising, qualified)")
    rank_in_tier: Optional[int] = Field(default=None, description="Rank within performance tier")
    overall_rank: Optional[int] = Field(default=None, description="Overall rank across all traders")
    composite_score: float = Field(description="Weighted composite performance score")
    
    # Core performance metrics (from silver_wallet_pnl)
    total_realized_pnl_usd: float = Field(description="Total realized profit/loss in USD")
    total_unrealized_pnl_usd: Optional[float] = Field(default=None, description="Total unrealized profit/loss in USD")
    win_rate_percent: float = Field(description="Percentage of profitable trades")
    total_trades: int = Field(description="Total number of trades")
    
    # Advanced metrics
    roi_percentage: Optional[float] = Field(default=None, description="Return on investment percentage")
    avg_trade_size_usd: Optional[float] = Field(default=None, description="Average trade size in USD")
    max_drawdown_percent: Optional[float] = Field(default=None, description="Maximum drawdown percentage")
    sharpe_ratio: Optional[float] = Field(default=None, description="Risk-adjusted return ratio")
    trade_frequency_per_day: Optional[float] = Field(default=None, description="Average trades per day")
    
    # Portfolio characteristics
    tokens_traded_count: int = Field(description="Number of different tokens traded")
    favorite_tokens: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Most frequently traded tokens")
    trading_patterns: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Trading pattern analysis")
    
    # Time-based metrics
    first_trade_at: datetime = Field(description="Timestamp of first trade")
    last_trade_at: datetime = Field(description="Timestamp of last trade")
    trading_days: int = Field(description="Number of days with trading activity")
    days_since_last_trade: Optional[int] = Field(default=None, description="Days since last trading activity")
    
    # Selection criteria met
    qualification_criteria: Dict[str, Any] = Field(sa_column=Column(JSONB), description="Criteria used for smart trader selection")
    score_breakdown: Dict[str, float] = Field(sa_column=Column(JSONB), description="Breakdown of composite score components")
    
    # Metadata
    selected_at: datetime = Field(default_factory=datetime.utcnow, description="When trader was selected/ranked")
    last_updated_at: datetime = Field(default_factory=datetime.utcnow, description="When record was last updated")
    data_freshness_hours: Optional[float] = Field(default=None, description="Hours since underlying data was updated")
    
    # Monitoring flags
    is_active: bool = Field(default=True, description="Whether trader is currently active")
    performance_declining: bool = Field(default=False, description="Flag for declining performance trend")
    risk_score: Optional[float] = Field(default=None, description="Risk assessment score (0-1, higher = riskier)")