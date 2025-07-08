"""
Database operations module for batch UPSERT and other database operations.
Provides efficient bulk operations using PostgreSQL's ON CONFLICT functionality.
"""

import logging
import pandas as pd
from typing import List, Dict, Any, Type, Optional, Union
from sqlalchemy import text, inspect
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, SQLModel
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class DatabaseOperations:
    """Database operations handler for efficient batch operations."""
    
    def __init__(self, session: Session):
        """Initialize with database session.
        
        Args:
            session: SQLModel Session instance
        """
        self.session = session
    
    def upsert_dataframe(
        self,
        df: pd.DataFrame,
        model_class: Type[SQLModel],
        conflict_columns: Union[str, List[str]],
        update_columns: Optional[List[str]] = None,
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """Perform batch UPSERT operation from pandas DataFrame.
        
        Args:
            df: DataFrame containing data to upsert
            model_class: SQLModel class representing the target table
            conflict_columns: Column(s) to use for conflict detection
            update_columns: Specific columns to update on conflict (if None, updates all non-conflict columns)
            batch_size: Number of records to process in each batch
            
        Returns:
            Dictionary with operation statistics
        """
        if df.empty:
            return {"status": "success", "records_processed": 0, "batches": 0}
        
        # Ensure conflict_columns is a list
        if isinstance(conflict_columns, str):
            conflict_columns = [conflict_columns]
        
        # Get table metadata
        table = model_class.__table__
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        
        # Determine columns to update
        all_columns = [col.name for col in table.columns]
        if update_columns is None:
            update_columns = [col for col in all_columns if col not in conflict_columns]
        
        total_records = len(df)
        records_processed = 0
        batch_count = 0
        
        logger.info(f"Starting UPSERT operation for {total_records} records to {table_name}")
        
        try:
            # Process in batches
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Convert DataFrame to list of dictionaries, handling NaN values
                # Use multiple approaches to clean NaN values
                import numpy as np
                
                # First replace NaN values with None
                clean_batch_df = batch_df.where(pd.notnull(batch_df), None)
                records = clean_batch_df.to_dict('records')
                
                # Additional cleaning of records to handle any remaining NaN values
                cleaned_records = []
                for record in records:
                    cleaned_record = {}
                    for key, value in record.items():
                        # Handle array/list values (don't try to check isna on them)
                        if isinstance(value, (list, dict)):
                            cleaned_record[key] = value
                        elif value is None:
                            cleaned_record[key] = None
                        elif isinstance(value, float) and np.isnan(value):
                            cleaned_record[key] = None
                        else:
                            # For scalar values, safely check if it's NaN
                            try:
                                if pd.isna(value):
                                    cleaned_record[key] = None
                                else:
                                    cleaned_record[key] = value
                            except (ValueError, TypeError):
                                # If pd.isna fails (e.g., for complex objects), just keep the value
                                cleaned_record[key] = value
                    cleaned_records.append(cleaned_record)
                
                records = cleaned_records
                
                # Create INSERT statement
                stmt = insert(table).values(records)
                
                # Add ON CONFLICT DO UPDATE clause
                if update_columns:
                    update_dict = {col: stmt.excluded[col] for col in update_columns}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=conflict_columns,
                        set_=update_dict
                    )
                else:
                    # ON CONFLICT DO NOTHING if no update columns specified
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
                
                # Execute the statement
                result = self.session.execute(stmt)
                batch_count += 1
                records_processed += len(records)
                
                logger.debug(f"Processed batch {batch_count}: {len(records)} records")
            
            # Commit the transaction
            self.session.commit()
            
            logger.info(f"UPSERT completed: {records_processed} records in {batch_count} batches")
            
            return {
                "status": "success",
                "records_processed": records_processed,
                "batches": batch_count,
                "table_name": table_name
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"UPSERT operation failed for {table_name}: {e}")
            raise
    
    def bulk_insert(
        self,
        records: List[Dict[str, Any]],
        model_class: Type[SQLModel],
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """Perform bulk insert operation.
        
        Args:
            records: List of dictionaries containing record data
            model_class: SQLModel class representing the target table
            batch_size: Number of records to process in each batch
            
        Returns:
            Dictionary with operation statistics
        """
        if not records:
            return {"status": "success", "records_inserted": 0, "batches": 0}
        
        table = model_class.__table__
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        
        total_records = len(records)
        records_inserted = 0
        batch_count = 0
        
        logger.info(f"Starting bulk insert for {total_records} records to {table_name}")
        
        try:
            # Process in batches
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_records = records[start_idx:end_idx]
                
                # Create and execute INSERT statement
                stmt = insert(table).values(batch_records)
                self.session.execute(stmt)
                
                batch_count += 1
                records_inserted += len(batch_records)
                
                logger.debug(f"Inserted batch {batch_count}: {len(batch_records)} records")
            
            # Commit the transaction
            self.session.commit()
            
            logger.info(f"Bulk insert completed: {records_inserted} records in {batch_count} batches")
            
            return {
                "status": "success",
                "records_inserted": records_inserted,
                "batches": batch_count,
                "table_name": table_name
            }
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Bulk insert failed for {table_name}: {e}")
            raise
    
    def execute_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute raw SQL query.
        
        Args:
            sql: SQL query string
            params: Optional parameters for the query
            
        Returns:
            Query result
        """
        try:
            result = self.session.execute(text(sql), params or {})
            return result
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            self.session.rollback()
            raise
    
    def get_table_stats(self, model_class: Type[SQLModel]) -> Dict[str, Any]:
        """Get statistics for a table.
        
        Args:
            model_class: SQLModel class representing the table
            
        Returns:
            Dictionary with table statistics
        """
        table = model_class.__table__
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        
        try:
            # Get row count
            count_result = self.session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = count_result.scalar()
            
            # Get table size
            size_result = self.session.execute(text(f"""
                SELECT pg_size_pretty(pg_total_relation_size('{table_name}')) as size,
                       pg_total_relation_size('{table_name}') as size_bytes
            """))
            size_info = size_result.fetchone()
            
            return {
                "table_name": table_name,
                "row_count": row_count,
                "size_pretty": size_info[0] if size_info else "Unknown",
                "size_bytes": size_info[1] if size_info else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get table stats for {table_name}: {e}")
            return {
                "table_name": table_name,
                "error": str(e)
            }
    
    def vacuum_analyze_table(self, model_class: Type[SQLModel]) -> bool:
        """Run VACUUM ANALYZE on a table to update statistics.
        
        Args:
            model_class: SQLModel class representing the table
            
        Returns:
            True if successful, False otherwise
        """
        table = model_class.__table__
        table_name = f"{table.schema}.{table.name}" if table.schema else table.name
        
        try:
            # Note: VACUUM cannot run inside a transaction, so we use autocommit
            self.session.execute(text(f"VACUUM ANALYZE {table_name}"))
            logger.info(f"VACUUM ANALYZE completed for {table_name}")
            return True
        except Exception as e:
            logger.error(f"VACUUM ANALYZE failed for {table_name}: {e}")
            return False


# Convenience functions for common operations

def upsert_dataframe(
    session: Session,
    df: pd.DataFrame,
    model_class: Type[SQLModel],
    conflict_columns: Union[str, List[str]],
    update_columns: Optional[List[str]] = None,
    batch_size: int = 1000
) -> Dict[str, Any]:
    """Convenience function for DataFrame UPSERT operation.
    
    Args:
        session: Database session
        df: DataFrame to upsert
        model_class: SQLModel class for target table
        conflict_columns: Column(s) for conflict detection
        update_columns: Columns to update on conflict
        batch_size: Batch size for processing
        
    Returns:
        Operation statistics
    """
    ops = DatabaseOperations(session)
    return ops.upsert_dataframe(df, model_class, conflict_columns, update_columns, batch_size)


def bulk_insert_records(
    session: Session,
    records: List[Dict[str, Any]],
    model_class: Type[SQLModel],
    batch_size: int = 1000
) -> Dict[str, Any]:
    """Convenience function for bulk insert operation.
    
    Args:
        session: Database session
        records: List of record dictionaries
        model_class: SQLModel class for target table
        batch_size: Batch size for processing
        
    Returns:
        Operation statistics
    """
    ops = DatabaseOperations(session)
    return ops.bulk_insert(records, model_class, batch_size)


def get_table_statistics(session: Session, model_class: Type[SQLModel]) -> Dict[str, Any]:
    """Convenience function to get table statistics.
    
    Args:
        session: Database session
        model_class: SQLModel class for target table
        
    Returns:
        Table statistics
    """
    ops = DatabaseOperations(session)
    return ops.get_table_stats(model_class)