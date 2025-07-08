"""
Bronze layer SQLModel schemas for raw data from BirdEye API.
These models represent the initial data ingestion layer.
"""

from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, String, DateTime, Float, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB


class BronzeToken(SQLModel, table=True):
    """Bronze tokens table - raw token data from BirdEye API."""
    
    __tablename__ = "bronze_tokens"
    __table_args__ = (
        Index("idx_bronze_tokens_market_cap", "market_cap"),
        Index("idx_bronze_tokens_price_change", "price_change_24h_percent"),
        Index("idx_bronze_tokens_ingested_at", "ingested_at"),
        Index("idx_bronze_tokens_silver_processed", "silver_processed"),
        {"schema": "bronze"}
    )
    
    # Primary key and identifiers
    token_address: str = Field(primary_key=True, max_length=44, description="Solana token mint address")
    
    # Token metadata
    logo_uri: Optional[str] = Field(default=None, max_length=500, description="Token logo URL")
    name: Optional[str] = Field(default=None, max_length=255, description="Token name")
    symbol: Optional[str] = Field(default=None, max_length=50, description="Token symbol")
    decimals: Optional[int] = Field(default=None, description="Token decimal places")
    
    # Market data
    market_cap: Optional[float] = Field(default=None, description="Market capitalization in USD")
    fdv: Optional[float] = Field(default=None, description="Fully diluted valuation in USD")
    liquidity: Optional[float] = Field(default=None, description="Total liquidity in USD")
    last_trade_unix_time: Optional[int] = Field(default=None, description="Unix timestamp of last trade")
    
    # Volume metrics
    volume_1h_usd: Optional[float] = Field(default=None, description="1-hour volume in USD")
    volume_1h_change_percent: Optional[float] = Field(default=None, description="1-hour volume change %")
    volume_2h_usd: Optional[float] = Field(default=None, description="2-hour volume in USD")
    volume_2h_change_percent: Optional[float] = Field(default=None, description="2-hour volume change %")
    volume_4h_usd: Optional[float] = Field(default=None, description="4-hour volume in USD")
    volume_4h_change_percent: Optional[float] = Field(default=None, description="4-hour volume change %")
    volume_8h_usd: Optional[float] = Field(default=None, description="8-hour volume in USD")
    volume_8h_change_percent: Optional[float] = Field(default=None, description="8-hour volume change %")
    volume_24h_usd: Optional[float] = Field(default=None, description="24-hour volume in USD")
    volume_24h_change_percent: Optional[float] = Field(default=None, description="24-hour volume change %")
    
    # Trade count metrics
    trade_1h_count: Optional[int] = Field(default=None, description="1-hour trade count")
    trade_2h_count: Optional[int] = Field(default=None, description="2-hour trade count")
    trade_4h_count: Optional[int] = Field(default=None, description="4-hour trade count")
    trade_8h_count: Optional[int] = Field(default=None, description="8-hour trade count")
    trade_24h_count: Optional[int] = Field(default=None, description="24-hour trade count")
    
    # Price metrics
    price: Optional[float] = Field(default=None, description="Current price in USD")
    price_change_1h_percent: Optional[float] = Field(default=None, description="1-hour price change %")
    price_change_2h_percent: Optional[float] = Field(default=None, description="2-hour price change %")
    price_change_4h_percent: Optional[float] = Field(default=None, description="4-hour price change %")
    price_change_8h_percent: Optional[float] = Field(default=None, description="8-hour price change %")
    price_change_24h_percent: Optional[float] = Field(default=None, description="24-hour price change %")
    
    # Holder data
    holder: Optional[int] = Field(default=None, description="Number of token holders")
    recent_listing_time: Optional[int] = Field(default=None, description="Unix timestamp of recent listing")
    
    # Extensions data (flattened from API response)
    coingecko_id: Optional[str] = Field(default=None, max_length=100, description="CoinGecko ID")
    serum_v3_usdc: Optional[str] = Field(default=None, max_length=44, description="Serum V3 USDC market address")
    serum_v3_usdt: Optional[str] = Field(default=None, max_length=44, description="Serum V3 USDT market address")
    website: Optional[str] = Field(default=None, max_length=500, description="Token website URL")
    telegram: Optional[str] = Field(default=None, max_length=500, description="Telegram link")
    twitter: Optional[str] = Field(default=None, max_length=500, description="Twitter link")
    description: Optional[str] = Field(default=None, max_length=1000, description="Token description")
    discord: Optional[str] = Field(default=None, max_length=500, description="Discord link")
    medium: Optional[str] = Field(default=None, max_length=500, description="Medium link")
    
    # Metadata and processing state
    ingested_at: datetime = Field(default_factory=datetime.utcnow, description="When record was ingested")
    silver_processed: bool = Field(default=False, description="Whether processed for silver layer")
    silver_processed_at: Optional[datetime] = Field(default=None, description="When silver processing completed")
    
    # Batch tracking
    batch_id: Optional[str] = Field(default=None, max_length=100, description="Batch identifier")
    processing_date: Optional[datetime] = Field(default=None, description="Processing date for partitioning")


class BronzeWhale(SQLModel, table=True):
    """Bronze whales table - raw whale/holder data from BirdEye API."""
    
    __tablename__ = "bronze_whales"
    __table_args__ = (
        Index("idx_bronze_whales_token_address", "token_address"),
        Index("idx_bronze_whales_wallet_address", "wallet_address"),
        Index("idx_bronze_whales_ui_amount", "ui_amount"),
        Index("idx_bronze_whales_fetched_at", "fetched_at"),
        {"schema": "bronze"}
    )
    
    # Composite primary key: token + wallet
    token_address: str = Field(primary_key=True, max_length=44, description="Token mint address")
    wallet_address: str = Field(primary_key=True, max_length=44, description="Wallet address")
    
    # Token metadata (denormalized for convenience)
    token_symbol: Optional[str] = Field(default=None, max_length=50, description="Token symbol")
    token_name: Optional[str] = Field(default=None, max_length=255, description="Token name")
    
    # Whale data
    rank: Optional[int] = Field(default=None, description="Ranking among token holders")
    amount: Optional[str] = Field(default=None, max_length=100, description="Raw token amount as string")
    ui_amount: Optional[float] = Field(default=None, description="Human-readable token amount")
    decimals: Optional[int] = Field(default=None, description="Token decimal places")
    mint: Optional[str] = Field(default=None, max_length=44, description="Token mint address (verification)")
    token_account: Optional[str] = Field(default=None, max_length=44, description="Holder's token account")
    
    # Metadata
    fetched_at: datetime = Field(default_factory=datetime.utcnow, description="When data was fetched")
    batch_id: Optional[str] = Field(default=None, max_length=100, description="Batch identifier")
    data_source: str = Field(default="birdeye_v3", max_length=50, description="Data source identifier")


class BronzeTransaction(SQLModel, table=True):
    """Bronze transactions table - raw transaction data from BirdEye API."""
    
    __tablename__ = "bronze_transactions"
    __table_args__ = (
        Index("idx_bronze_transactions_wallet", "wallet_address"),
        Index("idx_bronze_transactions_timestamp", "timestamp"),
        Index("idx_bronze_transactions_base_address", "base_address"),
        Index("idx_bronze_transactions_tx_type", "tx_type"),
        Index("idx_bronze_transactions_whale_id", "whale_id"),
        {"schema": "bronze"}
    )
    
    # Primary key
    transaction_hash: str = Field(primary_key=True, max_length=88, description="Transaction signature")
    
    # Whale and wallet identification
    whale_id: Optional[str] = Field(default=None, max_length=100, description="Whale identifier")
    wallet_address: str = Field(max_length=44, description="Wallet address that made the transaction")
    
    # Transaction metadata
    timestamp: datetime = Field(description="Transaction timestamp")
    tx_type: Optional[str] = Field(default=None, max_length=20, description="Transaction type (swap, transfer, etc.)")
    source: Optional[str] = Field(default=None, max_length=50, description="Transaction source/DEX")
    
    # Base token (token being traded)
    base_symbol: Optional[str] = Field(default=None, max_length=50, description="Base token symbol")
    base_address: Optional[str] = Field(default=None, max_length=44, description="Base token address")
    base_type_swap: Optional[str] = Field(default=None, max_length=10, description="Base token swap type (to/from)")
    base_ui_change_amount: Optional[float] = Field(default=None, description="Base token amount change")
    base_nearest_price: Optional[float] = Field(default=None, description="Base token price at transaction time")
    
    # Quote token (usually SOL or USDC)
    quote_symbol: Optional[str] = Field(default=None, max_length=50, description="Quote token symbol")
    quote_address: Optional[str] = Field(default=None, max_length=44, description="Quote token address")
    quote_type_swap: Optional[str] = Field(default=None, max_length=10, description="Quote token swap type (to/from)")
    quote_ui_change_amount: Optional[float] = Field(default=None, description="Quote token amount change")
    quote_nearest_price: Optional[float] = Field(default=None, description="Quote token price at transaction time")
    
    # Metadata
    fetched_at: datetime = Field(default_factory=datetime.utcnow, description="When data was fetched")
    batch_id: Optional[str] = Field(default=None, max_length=100, description="Batch identifier")
    data_source: str = Field(default="birdeye_v3", max_length=50, description="Data source identifier")