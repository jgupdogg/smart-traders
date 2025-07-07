"""
State manager for precise pipeline state tracking.
Manages processing state for every entity across all pipeline tasks.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from sqlalchemy import and_, or_, text
from sqlmodel import Session, select, func
from src.models.state import EntityProcessingState, ProcessingState, TaskExecutionLog
from src.database.connection import get_db_session

logger = logging.getLogger(__name__)


class StateManager:
    """Manages pipeline state tracking for precise processing control."""
    
    def __init__(self, session: Optional[Session] = None):
        """Initialize state manager.
        
        Args:
            session: Optional database session. If None, will create sessions as needed.
        """
        self.session = session
        self._use_context_manager = session is None
    
    def _get_session(self):
        """Get database session, either provided or create new one."""
        if self.session:
            return self.session
        else:
            return get_db_session()
    
    def create_or_update_state(
        self,
        task_name: str,
        entity_type: str,
        entity_id: str,
        state: ProcessingState,
        metadata: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> EntityProcessingState:
        """Create or update pipeline state for an entity.
        
        Args:
            task_name: Name of the task/pipeline step
            entity_type: Type of entity (token, whale, transaction)
            entity_id: Unique identifier for the entity
            state: Processing state
            metadata: Optional metadata about the processing
            error_message: Optional error message if processing failed
            
        Returns:
            EntityProcessingState record
        """
        if self._use_context_manager:
            with self._get_session() as session:
                return self._create_or_update_state_impl(
                    session, task_name, entity_type, entity_id, state, metadata, error_message
                )
        else:
            return self._create_or_update_state_impl(
                self.session, task_name, entity_type, entity_id, state, metadata, error_message
            )
    
    def _create_or_update_state_impl(
        self,
        session: Session,
        task_name: str,
        entity_type: str,
        entity_id: str,
        state: ProcessingState,
        metadata: Optional[Dict[str, Any]],
        error_message: Optional[str]
    ) -> EntityProcessingState:
        """Implementation of create_or_update_state with session."""
        # Check if state record already exists
        existing_state = session.exec(
            select(EntityProcessingState).where(
                and_(
                    EntityProcessingState.task_name == task_name,
                    EntityProcessingState.entity_type == entity_type,
                    EntityProcessingState.entity_id == entity_id
                )
            )
        ).first()
        
        now = datetime.utcnow()
        
        if existing_state:
            # Update existing state
            existing_state.state = state
            existing_state.updated_at = now
            existing_state.processing_metadata = metadata
            
            if state == ProcessingState.PROCESSING:
                existing_state.attempt_count += 1
                existing_state.last_attempt_at = now
            elif state == ProcessingState.COMPLETED:
                existing_state.completed_at = now
            elif state == ProcessingState.FAILED:
                existing_state.error_message = error_message
            
            session.add(existing_state)
            session.commit()
            session.refresh(existing_state)
            return existing_state
        else:
            # Create new state record
            new_state = EntityProcessingState(
                task_name=task_name,
                entity_type=entity_type,
                entity_id=entity_id,
                state=state,
                attempt_count=1 if state == ProcessingState.PROCESSING else 0,
                created_at=now,
                updated_at=now,
                last_attempt_at=now if state == ProcessingState.PROCESSING else None,
                completed_at=now if state == ProcessingState.COMPLETED else None,
                error_message=error_message,
                processing_metadata=metadata
            )
            
            session.add(new_state)
            session.commit()
            session.refresh(new_state)
            return new_state
    
    def get_pending_entities(
        self,
        task_name: str,
        entity_type: str,
        limit: Optional[int] = None,
        exclude_failed: bool = True
    ) -> List[str]:
        """Get list of entity IDs that are pending processing.
        
        Args:
            task_name: Name of the task
            entity_type: Type of entity
            limit: Maximum number of entities to return
            exclude_failed: Whether to exclude entities that have failed processing
            
        Returns:
            List of entity IDs that need processing
        """
        if self._use_context_manager:
            with self._get_session() as session:
                return self._get_pending_entities_impl(
                    session, task_name, entity_type, limit, exclude_failed
                )
        else:
            return self._get_pending_entities_impl(
                self.session, task_name, entity_type, limit, exclude_failed
            )
    
    def _get_pending_entities_impl(
        self,
        session: Session,
        task_name: str,
        entity_type: str,
        limit: Optional[int],
        exclude_failed: bool
    ) -> List[str]:
        """Implementation of get_pending_entities with session."""
        query = select(EntityProcessingState.entity_id).where(
            and_(
                EntityProcessingState.task_name == task_name,
                EntityProcessingState.entity_type == entity_type,
                EntityProcessingState.state == ProcessingState.PENDING
            )
        )
        
        if exclude_failed:
            # Also exclude entities that have failed max attempts
            query = query.where(
                or_(
                    EntityProcessingState.attempt_count < EntityProcessingState.max_attempts,
                    EntityProcessingState.state != ProcessingState.FAILED
                )
            )
        
        if limit:
            query = query.limit(limit)
        
        results = session.exec(query).all()
        return list(results)
    
    def mark_entities_processing(
        self,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Mark multiple entities as processing.
        
        Args:
            task_name: Name of the task
            entity_type: Type of entity
            entity_ids: List of entity IDs to mark as processing
            metadata: Optional metadata
            
        Returns:
            Number of entities marked as processing
        """
        if not entity_ids:
            return 0
        
        if self._use_context_manager:
            with self._get_session() as session:
                return self._mark_entities_processing_impl(
                    session, task_name, entity_type, entity_ids, metadata
                )
        else:
            return self._mark_entities_processing_impl(
                self.session, task_name, entity_type, entity_ids, metadata
            )
    
    def _mark_entities_processing_impl(
        self,
        session: Session,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        metadata: Optional[Dict[str, Any]]
    ) -> int:
        """Implementation of mark_entities_processing with session."""
        count = 0
        for entity_id in entity_ids:
            self._create_or_update_state_impl(
                session, task_name, entity_type, entity_id, 
                ProcessingState.PROCESSING, metadata, None
            )
            count += 1
        return count
    
    def mark_entities_completed(
        self,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Mark multiple entities as completed.
        
        Args:
            task_name: Name of the task
            entity_type: Type of entity
            entity_ids: List of entity IDs to mark as completed
            metadata: Optional metadata
            
        Returns:
            Number of entities marked as completed
        """
        if not entity_ids:
            return 0
        
        if self._use_context_manager:
            with self._get_session() as session:
                return self._mark_entities_completed_impl(
                    session, task_name, entity_type, entity_ids, metadata
                )
        else:
            return self._mark_entities_completed_impl(
                self.session, task_name, entity_type, entity_ids, metadata
            )
    
    def _mark_entities_completed_impl(
        self,
        session: Session,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        metadata: Optional[Dict[str, Any]]
    ) -> int:
        """Implementation of mark_entities_completed with session."""
        count = 0
        for entity_id in entity_ids:
            self._create_or_update_state_impl(
                session, task_name, entity_type, entity_id, 
                ProcessingState.COMPLETED, metadata, None
            )
            count += 1
        return count
    
    def mark_entities_failed(
        self,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        error_message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Mark multiple entities as failed.
        
        Args:
            task_name: Name of the task
            entity_type: Type of entity
            entity_ids: List of entity IDs to mark as failed
            error_message: Error message describing the failure
            metadata: Optional metadata
            
        Returns:
            Number of entities marked as failed
        """
        if not entity_ids:
            return 0
        
        if self._use_context_manager:
            with self._get_session() as session:
                return self._mark_entities_failed_impl(
                    session, task_name, entity_type, entity_ids, error_message, metadata
                )
        else:
            return self._mark_entities_failed_impl(
                self.session, task_name, entity_type, entity_ids, error_message, metadata
            )
    
    def _mark_entities_failed_impl(
        self,
        session: Session,
        task_name: str,
        entity_type: str,
        entity_ids: List[str],
        error_message: str,
        metadata: Optional[Dict[str, Any]]
    ) -> int:
        """Implementation of mark_entities_failed with session."""
        count = 0
        for entity_id in entity_ids:
            self._create_or_update_state_impl(
                session, task_name, entity_type, entity_id, 
                ProcessingState.FAILED, metadata, error_message
            )
            count += 1
        return count
    
    def get_task_statistics(self, task_name: str) -> Dict[str, Any]:
        """Get statistics for a specific task.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Dictionary with task statistics
        """
        if self._use_context_manager:
            with self._get_session() as session:
                return self._get_task_statistics_impl(session, task_name)
        else:
            return self._get_task_statistics_impl(self.session, task_name)
    
    def _get_task_statistics_impl(self, session: Session, task_name: str) -> Dict[str, Any]:
        """Implementation of get_task_statistics with session."""
        # Get counts by state
        stats_query = select(
            EntityProcessingState.state,
            EntityProcessingState.entity_type,
            func.count(EntityProcessingState.id).label('count')
        ).where(
            EntityProcessingState.task_name == task_name
        ).group_by(
            EntityProcessingState.state, EntityProcessingState.entity_type
        )
        
        results = session.exec(stats_query).all()
        
        # Organize results
        stats = {
            "task_name": task_name,
            "total_entities": 0,
            "by_state": {},
            "by_entity_type": {}
        }
        
        for state, entity_type, count in results:
            stats["total_entities"] += count
            
            if state not in stats["by_state"]:
                stats["by_state"][state] = 0
            stats["by_state"][state] += count
            
            if entity_type not in stats["by_entity_type"]:
                stats["by_entity_type"][entity_type] = {}
            if state not in stats["by_entity_type"][entity_type]:
                stats["by_entity_type"][entity_type][state] = 0
            stats["by_entity_type"][entity_type][state] += count
        
        return stats
    
    def cleanup_old_states(self, days_old: int = 30) -> int:
        """Clean up old completed state records.
        
        Args:
            days_old: Number of days old for records to be considered for cleanup
            
        Returns:
            Number of records cleaned up
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        if self._use_context_manager:
            with self._get_session() as session:
                return self._cleanup_old_states_impl(session, cutoff_date)
        else:
            return self._cleanup_old_states_impl(self.session, cutoff_date)
    
    def _cleanup_old_states_impl(self, session: Session, cutoff_date: datetime) -> int:
        """Implementation of cleanup_old_states with session."""
        # Only clean up completed states that are old
        old_states = session.exec(
            select(EntityProcessingState).where(
                and_(
                    EntityProcessingState.state == ProcessingState.COMPLETED,
                    EntityProcessingState.completed_at < cutoff_date
                )
            )
        ).all()
        
        count = len(old_states)
        for state in old_states:
            session.delete(state)
        
        session.commit()
        logger.info(f"Cleaned up {count} old state records")
        return count


# Task execution logging functions

def log_task_start(
    task_name: str,
    run_id: str,
    execution_date: datetime,
    metadata: Optional[Dict[str, Any]] = None
) -> TaskExecutionLog:
    """Log the start of a task execution.
    
    Args:
        task_name: Name of the task
        run_id: Unique run identifier
        execution_date: Scheduled execution date
        metadata: Optional task metadata
        
    Returns:
        TaskExecutionLog record
    """
    with get_db_session() as session:
        log_entry = TaskExecutionLog(
            task_name=task_name,
            run_id=run_id,
            execution_date=execution_date,
            started_at=datetime.utcnow(),
            status="running",
            task_metrics=metadata
        )
        
        session.add(log_entry)
        session.commit()
        session.refresh(log_entry)
        return log_entry


def log_task_completion(
    log_id: int,
    status: str,
    entities_processed: int = 0,
    entities_failed: int = 0,
    entities_skipped: int = 0,
    task_metrics: Optional[Dict[str, Any]] = None,
    error_summary: Optional[Dict[str, Any]] = None
) -> TaskExecutionLog:
    """Log the completion of a task execution.
    
    Args:
        log_id: ID of the TaskExecutionLog record
        status: Final status of the task
        entities_processed: Number of entities successfully processed
        entities_failed: Number of entities that failed
        entities_skipped: Number of entities skipped
        task_metrics: Task-specific metrics
        error_summary: Summary of errors if any
        
    Returns:
        Updated TaskExecutionLog record
    """
    with get_db_session() as session:
        log_entry = session.get(TaskExecutionLog, log_id)
        if log_entry:
            now = datetime.utcnow()
            log_entry.completed_at = now
            log_entry.duration_seconds = (now - log_entry.started_at).total_seconds()
            log_entry.status = status
            log_entry.entities_processed = entities_processed
            log_entry.entities_failed = entities_failed
            log_entry.entities_skipped = entities_skipped
            log_entry.task_metrics = task_metrics
            log_entry.error_summary = error_summary
            
            session.add(log_entry)
            session.commit()
            session.refresh(log_entry)
        
        return log_entry