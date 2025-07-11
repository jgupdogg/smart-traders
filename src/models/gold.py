"""
Gold layer SQLModel schemas for final analyzed and aggregated data.
These models represent the final output layer with smart trader rankings.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, String, DateTime, Float, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB


class SmartTrader(SQLModel, table=True):
    """Gold smart traders table - ranked and categorized traders for following."""
    
    __tablename__ = "smart_traders"
    __table_args__ = (
        Index("idx_smart_traders_tier", "performance_tier"),
        Index("idx_smart_traders_tier_score", "tier_score"),
        Index("idx_smart_traders_coverage", "coverage_confidence"),
        Index("idx_smart_traders_roi", "total_roi_percent"),
        Index("idx_smart_traders_last_updated", "last_updated_at"),
        Index("idx_smart_traders_tier_score_composite", "performance_tier", "tier_score"),
        Index("idx_smart_traders_auto_follow", "auto_follow_eligible"),
        {"schema": "gold"}
    )
    
    # Primary key
    wallet_address: str = Field(primary_key=True, max_length=44, description="Wallet address")
    
    # Performance tier classification
    performance_tier: str = Field(max_length=20, description="ELITE/STRONG/PROMISING/QUALIFIED/EXPERIMENTAL")
    tier_score: float = Field(description="Weighted score within tier (0-100)")
    tier_rank: Optional[int] = Field(default=None, description="Rank within performance tier")
    overall_rank: Optional[int] = Field(default=None, description="Overall rank across all tiers")
    
    # Confidence metrics
    coverage_confidence: str = Field(max_length=20, description="High/Good/Acceptable/Low based on coverage ratio")
    coverage_ratio_percent: float = Field(description="Percentage of trades with known cost basis")
    
    # Core performance metrics (from silver layer)
    total_roi_percent: float = Field(description="Overall ROI percentage for all matched trades")
    avg_roi_percent: float = Field(description="Average ROI percentage per matched trade")
    median_roi_percent: float = Field(description="Median ROI percentage for matched trades")
    total_realized_pnl_usd: float = Field(description="Total realized profit/loss in USD")
    win_rate_percent: float = Field(description="Percentage of profitable trades")
    sharpe_ratio: float = Field(description="Risk-adjusted return ratio")
    max_drawdown_percent: float = Field(description="Maximum drawdown percentage")
    
    # Trading activity metrics
    meaningful_trades: int = Field(description="Number of trades with >= $50 profit/loss")
    matched_trades: int = Field(description="Number of trades with known cost basis")
    total_trades: int = Field(description="Total number of trades")
    trade_frequency_per_day: float = Field(description="Average trades per day")
    trading_days: int = Field(description="Number of days with trading activity")
    days_since_first_trade: int = Field(description="Total days since first trade")
    
    # Volume and size metrics
    total_volume_usd: float = Field(description="Total trading volume in USD")
    matched_volume_usd: float = Field(description="Trading volume for matched trades")
    matched_cost_basis_usd: float = Field(description="Total cost basis for matched trades")
    avg_trade_size_usd: float = Field(description="Average trade size in USD")
    
    # Recent performance (last 30 days)
    last_30d_roi_percent: Optional[float] = Field(default=None, description="ROI in last 30 days")
    last_30d_trades: Optional[int] = Field(default=None, description="Number of trades in last 30 days")
    last_30d_win_rate: Optional[float] = Field(default=None, description="Win rate in last 30 days")
    last_30d_pnl_usd: Optional[float] = Field(default=None, description="PnL in last 30 days")
    
    # Time-based metrics
    first_trade_at: datetime = Field(description="Timestamp of first trade")
    last_trade_at: datetime = Field(description="Timestamp of last trade")
    last_updated_at: datetime = Field(default_factory=datetime.utcnow, description="When record was last updated")
    
    # Portfolio composition
    tokens_traded_count: int = Field(description="Number of different tokens traded")
    top_tokens: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="Top performing tokens with metrics")
    
    # Risk and consistency metrics
    consistency_score: Optional[float] = Field(default=None, description="Consistency of returns over time")
    volatility_score: Optional[float] = Field(default=None, description="Volatility of trading performance")
    
    # Qualification flags
    auto_follow_eligible: bool = Field(default=False, description="Safe for automated following")
    verified_performance: bool = Field(default=False, description="Performance verified through multiple time periods")
    
    # Scoring breakdown
    score_breakdown: dict = Field(sa_column=Column(JSONB), description="Breakdown of tier score components")
    tier_criteria_met: dict = Field(sa_column=Column(JSONB), description="Which tier criteria were satisfied")
    
    # Source tracking
    source_silver_calculated_at: datetime = Field(description="When source silver PnL was calculated")
    calculation_notes: Optional[str] = Field(default=None, max_length=500, description="Notes about calculation or data quality")


class TraderPerformanceHistory(SQLModel, table=True):
    """Historical performance tracking for smart traders."""
    
    __tablename__ = "trader_performance_history"
    __table_args__ = (
        Index("idx_trader_history_address_date", "wallet_address", "performance_date"),
        Index("idx_trader_history_date", "performance_date"),
        Index("idx_trader_history_tier", "performance_tier"),
        {"schema": "gold"}
    )
    
    # Composite primary key
    wallet_address: str = Field(primary_key=True, max_length=44, description="Wallet address")
    performance_date: datetime = Field(primary_key=True, description="Date of performance snapshot")
    
    # Performance snapshot
    performance_tier: str = Field(max_length=20, description="Tier at this time")
    tier_score: float = Field(description="Score at this time")
    total_roi_percent: float = Field(description="ROI at this time")
    total_realized_pnl_usd: float = Field(description="PnL at this time")
    win_rate_percent: float = Field(description="Win rate at this time")
    meaningful_trades: int = Field(description="Trade count at this time")
    coverage_ratio_percent: float = Field(description="Coverage ratio at this time")
    
    # Change metrics
    roi_change_7d: Optional[float] = Field(default=None, description="ROI change over 7 days")
    pnl_change_7d: Optional[float] = Field(default=None, description="PnL change over 7 days")
    tier_change: Optional[str] = Field(default=None, max_length=50, description="Tier change description")
    
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When snapshot was created")