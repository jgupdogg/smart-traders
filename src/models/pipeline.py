"""
Pipeline schema for tracking task execution state and metadata.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from sqlmodel import SQLModel, Field, Column, String, DateTime, Boolean, Integer, Index
from sqlalchemy.dialects.postgresql import JSONB


class PipelineState(SQLModel, table=True):
    """Pipeline state tracking table."""
    
    __tablename__ = "pipeline_state"
    __table_args__ = (
        Index("idx_pipeline_state_task_name", "task_name"),
        Index("idx_pipeline_state_run_id", "run_id"),
        Index("idx_pipeline_state_execution_date", "execution_date"),
        Index("idx_pipeline_state_status", "status"),
        {"schema": "pipeline"}
    )
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Task identification
    task_name: str = Field(max_length=100, description="Name of the task")
    run_id: str = Field(max_length=200, description="Airflow run ID")
    execution_date: datetime = Field(description="Task execution date")
    
    # Execution tracking
    status: str = Field(max_length=20, description="Task status (started, success, failed)")
    start_time: datetime = Field(default_factory=datetime.utcnow, description="Task start time")
    end_time: Optional[datetime] = Field(default=None, description="Task end time")
    
    # Metrics and metadata
    records_processed: int = Field(default=0, description="Number of records processed")
    records_failed: int = Field(default=0, description="Number of records that failed")
    records_skipped: int = Field(default=0, description="Number of records skipped")
    
    # Task-specific metrics and metadata
    task_metrics: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Task-specific metrics")
    error_summary: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Error details if task failed")
    task_metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Additional task metadata")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When record was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="When record was last updated")


class EntityState(SQLModel, table=True):
    """Entity processing state tracking table."""
    
    __tablename__ = "entity_state" 
    __table_args__ = (
        Index("idx_entity_state_task_entity", "task_name", "entity_type", "entity_id"),
        Index("idx_entity_state_status", "status"),
        Index("idx_entity_state_last_processed", "last_processed_at"),
        {"schema": "pipeline"}
    )
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Entity identification
    task_name: str = Field(max_length=100, description="Name of the task")
    entity_type: str = Field(max_length=50, description="Type of entity (token, wallet, etc.)")
    entity_id: str = Field(max_length=100, description="Unique identifier for the entity")
    
    # Processing state
    status: str = Field(max_length=20, description="Processing status (pending, completed, failed)")
    first_seen_at: datetime = Field(default_factory=datetime.utcnow, description="When entity was first seen")
    last_processed_at: Optional[datetime] = Field(default=None, description="When entity was last processed")
    
    # Error tracking
    retry_count: int = Field(default=0, description="Number of retry attempts")
    last_error: Optional[str] = Field(default=None, max_length=1000, description="Last error message")
    
    # Metadata
    entity_metadata: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSONB), description="Entity-specific metadata")
    
    # Timing
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When record was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="When record was last updated")