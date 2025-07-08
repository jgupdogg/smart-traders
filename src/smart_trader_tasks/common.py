"""
Common utilities and base classes for smart trader tasks.
Provides consistent patterns for API calls, state management, and database operations.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, List, Optional, Type
from abc import ABC, abstractmethod
from src.services.birdeye_client import BirdEyeAPIClient
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


def clean_dataframe_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame to handle NaN values for database insertion.
    
    Args:
        df: DataFrame to clean
        
    Returns:
        Cleaned DataFrame with NaN values replaced by None
    """
    # Use multiple approaches to ensure all NaN values are handled
    # First handle scalar NaN values
    df = df.replace([np.nan], [None])
    
    # For each column, handle NaN values more carefully
    for col in df.columns:
        # Check if column contains arrays/lists that shouldn't be processed with isna
        sample_val = df[col].iloc[0] if len(df) > 0 else None
        if isinstance(sample_val, (list, dict)):
            # Skip NaN processing for array/dict columns
            continue
        else:
            # Safe NaN replacement for scalar columns
            df[col] = df[col].where(pd.notnull(df[col]), None)
    
    return df


def safe_value(val):
    """
    Convert pandas values to database-safe values, handling NaN.
    
    Args:
        val: Value to convert (can be pandas value, numpy value, etc.)
        
    Returns:
        None if value is NaN/None, otherwise the original value
    """
    if val is None:
        return None
    if pd.isna(val):
        return None
    # Handle the case where val might be a numpy NaN or float('nan')
    if isinstance(val, float) and (val != val):  # NaN check
        return None
    return val


def get_birdeye_client() -> BirdEyeAPIClient:
    """
    Get initialized BirdEye API client with consistent configuration.
    
    Returns:
        Configured BirdEyeAPIClient instance
    """
    settings = get_settings()
    api_key = settings.get_birdeye_api_key()
    return BirdEyeAPIClient(api_key)


class TaskBase(ABC):
    """Base class for all smart trader tasks with common functionality."""
    
    def __init__(self, task_name: str, context: Dict[str, Any]):
        """
        Initialize base task.
        
        Args:
            task_name: Name of the task for logging and state tracking
            context: Airflow context dictionary
        """
        self.task_name = task_name
        self.context = context
        self.run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.execution_date = datetime.utcnow()
        self.state_manager = StateManager()
        self.logger = logging.getLogger(f"{__name__}.{task_name}")
        
        # Initialize metrics
        self.processed_entities = 0
        self.new_records = 0
        self.updated_records = 0
        self.failed_entities = []
        
    def start_task_logging(self, metadata: Optional[Dict[str, Any]] = None) -> Any:
        """Start task execution logging."""
        return log_task_start(
            task_name=self.task_name,
            run_id=self.run_id,
            execution_date=self.execution_date,
            metadata=metadata or {}
        )
        
    def complete_task_logging(
        self, 
        task_log: Any, 
        status: str, 
        task_metrics: Optional[Dict[str, Any]] = None,
        error_summary: Optional[Dict[str, Any]] = None
    ):
        """Complete task execution logging."""
        log_task_completion(
            log_id=task_log.id,
            status=status,
            entities_processed=self.processed_entities,
            entities_failed=len(self.failed_entities),
            task_metrics=task_metrics,
            error_summary=error_summary
        )
        
    def upsert_records(
        self,
        session: Any,
        df: pd.DataFrame,
        model_class: Type[SQLModel],
        conflict_columns: List[str],
        batch_size: int = 50
    ) -> Dict[str, Any]:
        """
        Standard upsert operation with consistent error handling.
        
        Args:
            session: Database session
            df: DataFrame to upsert
            model_class: SQLModel class for target table
            conflict_columns: Columns for conflict detection
            batch_size: Batch size for processing
            
        Returns:
            Upsert operation results
        """
        # Clean DataFrame before upsert
        clean_df = clean_dataframe_for_db(df)
        
        return upsert_dataframe(
            session=session,
            df=clean_df,
            model_class=model_class,
            conflict_columns=conflict_columns,
            batch_size=batch_size
        )
        
    @abstractmethod
    def execute(self) -> Dict[str, Any]:
        """Execute the task. Must be implemented by subclasses."""
        pass


class BronzeTaskBase(TaskBase):
    """Base class for bronze layer tasks (API data fetching)."""
    
    def __init__(self, task_name: str, context: Dict[str, Any]):
        super().__init__(task_name, context)
        self.birdeye_client = get_birdeye_client()
        
    def handle_api_error(self, entity_id: str, error: Exception, metadata: Optional[Dict[str, Any]] = None):
        """Handle API errors with consistent state tracking."""
        self.logger.error(f"API error for {entity_id}: {error}")
        self.failed_entities.append(entity_id)
        
        self.state_manager.create_or_update_state(
            task_name=self.task_name,
            entity_type="entity",
            entity_id=entity_id,
            state="failed",
            error_message=str(error),
            metadata=metadata or {}
        )


class SilverTaskBase(TaskBase):
    """Base class for silver layer tasks (data processing)."""
    
    def __init__(self, task_name: str, context: Dict[str, Any]):
        super().__init__(task_name, context)
        
    def mark_upstream_processed(
        self,
        session: Any,
        model_class: Type[SQLModel],
        entity_ids: List[str],
        processed_field: str = "processed",
        processed_at_field: str = "processed_at"
    ):
        """Mark upstream entities as processed."""
        try:
            # Update using SQLModel
            for entity_id in entity_ids:
                entity = session.get(model_class, entity_id)
                if entity:
                    setattr(entity, processed_field, True)
                    setattr(entity, processed_at_field, datetime.utcnow())
                    session.add(entity)
            
            session.commit()
            self.logger.info(f"Marked {len(entity_ids)} entities as processed")
            
        except Exception as e:
            self.logger.error(f"Failed to mark entities as processed: {e}")
            session.rollback()
            raise


class GoldTaskBase(TaskBase):
    """Base class for gold layer tasks (aggregation and scoring)."""
    
    def __init__(self, task_name: str, context: Dict[str, Any]):
        super().__init__(task_name, context)
        
    def calculate_percentiles(self, values: List[float], percentiles: List[float] = [0.95, 0.80, 0.60]) -> Dict[str, float]:
        """Calculate percentiles for ranking."""
        if not values:
            return {}
            
        series = pd.Series(values)
        return {
            f"p{int(p*100)}": series.quantile(p) 
            for p in percentiles
        }