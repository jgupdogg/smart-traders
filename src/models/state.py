"""
State tracking SQLModel schemas for precise pipeline state management.
These models enable precise tracking of processing state for every entity.
"""

from datetime import datetime
from typing import Optional
from sqlmodel import SQLModel, Field, Column, String, DateTime, Float, Integer, Boolean, Index
from sqlalchemy.dialects.postgresql import JSONB
from enum import Enum


class ProcessingState(str, Enum):
    """Enumeration of possible processing states."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class EntityProcessingState(SQLModel, table=True):
    """Entity processing state table - tracks processing state for every entity."""
    
    __tablename__ = "entity_processing_state"
    __table_args__ = (
        Index("idx_pipeline_state_lookup", "task_name", "state"),
        Index("idx_pipeline_state_entity", "entity_type", "entity_id"),
        Index("idx_pipeline_state_last_attempt", "last_attempt_at"),
        Index("idx_pipeline_state_completed", "completed_at"),
        {"schema": "pipeline"}
    )
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True, description="Auto-increment ID")
    
    # Entity identification
    task_name: str = Field(max_length=50, description="Name of the task/pipeline step")
    entity_type: str = Field(max_length=50, description="Type of entity (token, whale, transaction)")
    entity_id: str = Field(max_length=100, description="Unique identifier for the entity")
    
    # Processing state
    state: ProcessingState = Field(description="Current processing state")
    attempt_count: int = Field(default=0, description="Number of processing attempts")
    max_attempts: int = Field(default=3, description="Maximum allowed attempts before marking as failed")
    
    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow, description="When state record was created")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="When state was last updated")
    last_attempt_at: Optional[datetime] = Field(default=None, description="When last processing attempt was made")
    completed_at: Optional[datetime] = Field(default=None, description="When processing was completed")
    
    # Error tracking
    error_message: Optional[str] = Field(default=None, description="Last error message if processing failed")
    error_details: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON details about the error")
    
    # Processing metadata
    processing_metadata: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON metadata about processing")
    processing_duration_seconds: Optional[float] = Field(default=None, description="Time taken for last processing attempt")
    
    # Retry configuration
    next_retry_at: Optional[datetime] = Field(default=None, description="When next retry should be attempted")
    backoff_multiplier: float = Field(default=2.0, description="Exponential backoff multiplier for retries")


class TaskExecutionLog(SQLModel, table=True):
    """Task execution log - tracks overall task execution statistics."""
    
    __tablename__ = "task_execution_log"
    __table_args__ = (
        Index("idx_task_execution_task_name", "task_name"),
        Index("idx_task_execution_started_at", "started_at"),
        Index("idx_task_execution_status", "status"),
        {"schema": "pipeline"}
    )
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True, description="Auto-increment ID")
    
    # Task identification
    task_name: str = Field(max_length=50, description="Name of the executed task")
    run_id: str = Field(max_length=100, description="Unique run identifier (from Airflow)")
    execution_date: datetime = Field(description="Scheduled execution date")
    
    # Execution metadata
    started_at: datetime = Field(default_factory=datetime.utcnow, description="When task execution started")
    completed_at: Optional[datetime] = Field(default=None, description="When task execution completed")
    duration_seconds: Optional[float] = Field(default=None, description="Total execution duration")
    
    # Results
    status: str = Field(max_length=20, description="Execution status (success, failed, partial)")
    entities_processed: int = Field(default=0, description="Number of entities successfully processed")
    entities_failed: int = Field(default=0, description="Number of entities that failed processing")
    entities_skipped: int = Field(default=0, description="Number of entities skipped")
    
    # Resource usage
    memory_usage_mb: Optional[float] = Field(default=None, description="Peak memory usage in MB")
    cpu_usage_percent: Optional[float] = Field(default=None, description="Average CPU usage percentage")
    
    # Task-specific metrics
    task_metrics: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON task-specific metrics")
    error_summary: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON summary of errors encountered")
    
    # Airflow integration
    airflow_task_id: Optional[str] = Field(default=None, max_length=100, description="Airflow task identifier")
    airflow_dag_id: Optional[str] = Field(default=None, max_length=100, description="Airflow DAG identifier")


class DataQualityCheck(SQLModel, table=True):
    """Data quality check results for pipeline validation."""
    
    __tablename__ = "data_quality_checks"
    __table_args__ = (
        Index("idx_data_quality_table_name", "table_name"),
        Index("idx_data_quality_check_type", "check_type"),
        Index("idx_data_quality_executed_at", "executed_at"),
        Index("idx_data_quality_passed", "passed"),
        {"schema": "pipeline"}
    )
    
    # Primary key
    id: Optional[int] = Field(default=None, primary_key=True, description="Auto-increment ID")
    
    # Check identification
    table_name: str = Field(max_length=100, description="Name of table being checked")
    check_type: str = Field(max_length=50, description="Type of quality check (completeness, uniqueness, etc.)")
    check_name: str = Field(max_length=100, description="Descriptive name of the check")
    
    # Check execution
    executed_at: datetime = Field(default_factory=datetime.utcnow, description="When check was executed")
    execution_duration_seconds: Optional[float] = Field(default=None, description="Time taken to execute check")
    
    # Check results
    passed: bool = Field(description="Whether the check passed")
    expected_value: Optional[str] = Field(default=None, description="Expected value or condition")
    actual_value: Optional[str] = Field(default=None, description="Actual value observed")
    threshold: Optional[float] = Field(default=None, description="Threshold for pass/fail")
    
    # Check details
    records_checked: Optional[int] = Field(default=None, description="Number of records checked")
    violations_found: Optional[int] = Field(default=None, description="Number of violations found")
    
    # Additional context
    check_query: Optional[str] = Field(default=None, description="SQL query used for the check")
    quality_check_metadata: Optional[dict] = Field(default=None, sa_column=Column(JSONB), description="JSON metadata about the check")
    remediation_suggestions: Optional[str] = Field(default=None, description="Suggestions for fixing quality issues")