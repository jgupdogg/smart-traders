"""
Bronze layer model for webhook transactions.
Stores raw webhook payloads from Helius.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, Index
from sqlalchemy.dialects.postgresql import JSONB


class BronzeWebhookTransaction(SQLModel, table=True):
    """Bronze webhook transactions table - raw webhook data from Helius."""
    
    __tablename__ = "bronze_webhook_transactions"
    __table_args__ = (
        Index("idx_bronze_webhook_tx_signature", "transaction_signature"),
        Index("idx_bronze_webhook_timestamp", "webhook_timestamp"),
        Index("idx_bronze_webhook_account", "account_address"),
        Index("idx_bronze_webhook_token", "token_address"),
        Index("idx_bronze_webhook_type", "webhook_type"),
        Index("idx_bronze_webhook_status", "processing_status"),
        Index("idx_bronze_webhook_ingested", "ingested_at"),
        {"schema": "bronze"}
    )
    
    # Primary key - webhook_id ensures deduplication
    webhook_id: str = Field(primary_key=True, max_length=100, description="Unique webhook identifier")
    
    # Transaction identification
    transaction_signature: Optional[str] = Field(default=None, max_length=100, description="Solana transaction signature")
    
    # Webhook metadata
    webhook_type: Optional[str] = Field(default=None, max_length=50, description="Type of webhook (TRANSFER, SWAP, etc.)")
    webhook_timestamp: datetime = Field(description="Timestamp when webhook was generated")
    
    # Transaction data extracted from payload
    account_address: Optional[str] = Field(default=None, max_length=44, description="Primary account address involved")
    token_address: Optional[str] = Field(default=None, max_length=44, description="Token mint address if applicable")
    
    # Raw webhook payload - stored as JSONB for efficient querying
    raw_payload: Dict[str, Any] = Field(
        default={},
        sa_column=Column(JSONB),
        description="Complete webhook payload from Helius"
    )
    
    # Processing metadata
    ingested_at: datetime = Field(default_factory=datetime.utcnow, description="When record was ingested")
    processing_status: str = Field(default="pending", max_length=20, description="Processing status: pending/processed/failed")
    processed_at: Optional[datetime] = Field(default=None, description="When record was processed")
    
    # Additional metadata
    source_ip: Optional[str] = Field(default=None, max_length=45, description="IP address of webhook sender")
    user_agent: Optional[str] = Field(default=None, max_length=500, description="User agent of webhook sender")