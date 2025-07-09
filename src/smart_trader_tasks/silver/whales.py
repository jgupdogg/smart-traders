"""
Silver whales task - Process unique whale addresses from bronze_whales table.
Standardized implementation with minimal filtering (all unique whales).
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import select, func, text
from src.models.bronze import BronzeWhale
from src.models.silver import SilverWhale
from src.database.connection import get_db_session
from src.config.settings import get_silver_whales_config
from smart_trader_tasks.common import SilverTaskBase

logger = logging.getLogger(__name__)


class SilverWhalesTask(SilverTaskBase):
    """Silver whales task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("silver_whales", context)
        self.config = get_silver_whales_config()
        
    def get_unique_whales(self, session: Any) -> List[Any]:
        """Get unique whale addresses from unprocessed bronze_whales records."""
        unique_whales_query = select(
            BronzeWhale.wallet_address,
            func.count(BronzeWhale.token_address).label('tokens_held_count'),
            func.array_agg(BronzeWhale.token_address).label('token_addresses'),
            func.array_agg(BronzeWhale.token_symbol).label('token_symbols'),
            func.sum(BronzeWhale.ui_amount).label('total_tokens_held')
        ).where(
            BronzeWhale.silver_processed == False
        ).group_by(
            BronzeWhale.wallet_address
        )
        
        return list(session.exec(unique_whales_query))
        
    def get_whale_details(self, session: Any, wallet_address: str) -> List[Any]:
        """Get detailed whale data from bronze_whales for this address."""
        whale_details_query = select(BronzeWhale).where(
            BronzeWhale.wallet_address == wallet_address
        )
        return list(session.exec(whale_details_query))
        
    def create_whale_record(
        self, 
        wallet_address: str, 
        tokens_held_count: int, 
        whale_details: List[Any]
    ) -> Dict[str, Any]:
        """Create a whale record from the aggregated data."""
        # Calculate total value held (approximate using ui_amount)
        total_value_held = 0
        source_tokens = []
        
        for detail in whale_details:
            # For now, use ui_amount as proxy for value
            # In production, would need to fetch current token prices
            ui_amount = getattr(detail, 'ui_amount', 0) or 0
            total_value_held += ui_amount
            source_tokens.append({
                "token_address": getattr(detail, 'token_address', ''),
                "token_symbol": getattr(detail, 'token_symbol', ''),
                "ui_amount": ui_amount,
                "rank": getattr(detail, 'rank', 0)
            })
        
        # No filtering - process all whales
        whale_record = {
            'wallet_address': wallet_address,
            'tokens_held_count': tokens_held_count,
            'total_value_held': total_value_held,
            'first_seen_at': datetime.utcnow(),
            'last_updated_at': datetime.utcnow(),
            'transactions_processed': False,
            'transactions_processed_at': None,
            'pnl_processed': False,
            'pnl_processed_at': None,
            'source_tokens': source_tokens
        }
        
        return whale_record
    
    def mark_bronze_whales_processed(self, session: Any, wallet_address: str):
        """Mark bronze whale records as processed for this wallet."""
        try:
            # Mark all bronze_whales for this wallet as silver_processed
            update_query = text("""
                UPDATE bronze.bronze_whales 
                SET silver_processed = true, silver_processed_at = :timestamp 
                WHERE wallet_address = :wallet_address
            """)
            session.execute(update_query, {
                "timestamp": datetime.utcnow(), 
                "wallet_address": wallet_address
            })
            session.commit()
            
            self.logger.debug(f"Marked bronze_whales for wallet {wallet_address} as silver_processed=true")
            
        except Exception as e:
            self.logger.error(f"Failed to mark bronze_whales as processed for wallet {wallet_address}: {e}")
            session.rollback()
            raise
        
    def process_whale(self, session: Any, whale_data: Any) -> Dict[str, Any]:
        """Process a single whale."""
        wallet_address = whale_data.wallet_address
        tokens_held_count = whale_data.tokens_held_count
        
        try:
            # Create state for this whale
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="whale",
                entity_id=wallet_address,
                state="processing",
                metadata={"tokens_held": tokens_held_count}
            )
            
            # Get detailed whale data from bronze_whales for this address
            whale_details = self.get_whale_details(session, wallet_address)
            
            # Create whale record
            whale_record = self.create_whale_record(wallet_address, tokens_held_count, whale_details)
            
            # Mark whale as completed and mark bronze_whales as processed
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="whale",
                entity_id=wallet_address,
                state="completed",
                metadata={
                    "tokens_held": tokens_held_count, 
                    "total_value": whale_record['total_value_held']
                }
            )
            
            # Mark the bronze_whales records for this wallet as processed
            self.mark_bronze_whales_processed(session, wallet_address)
            
            return whale_record
            
        except Exception as e:
            self.logger.error(f"Error processing whale {wallet_address}: {e}")
            self.failed_entities.append(wallet_address)
            
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="whale",
                entity_id=wallet_address,
                state="failed",
                error_message=str(e)
            )
            raise
            
    def execute(self) -> Dict[str, Any]:
        """Execute the silver whales task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            with get_db_session() as session:
                # Get unique whale addresses from unprocessed bronze_whales records
                unique_whales_result = self.get_unique_whales(session)
                
                if not unique_whales_result:
                    self.logger.info("No unique whales found")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_whales_found"}
                    )
                    return {
                        "status": "completed",
                        "processed_whales": 0,
                        "new_whales": 0,
                        "message": "No whales to process"
                    }
                
                self.logger.info(f"Found {len(unique_whales_result)} unique whale addresses")
                
                # Process whale addresses
                whale_records = []
                for whale_data in unique_whales_result:
                    try:
                        whale_record = self.process_whale(session, whale_data)
                        whale_records.append(whale_record)
                        self.processed_entities += 1
                        
                    except Exception as e:
                        # Error already logged in process_whale
                        continue
                
                # Store whale records if any were processed
                if whale_records:
                    df = pd.DataFrame(whale_records)
                    
                    upsert_result = self.upsert_records(
                        session=session,
                        df=df,
                        model_class=SilverWhale,
                        conflict_columns=["wallet_address"],
                        batch_size=50
                    )
                    
                    self.new_records = upsert_result.get('inserted', 0)
                    self.updated_records = upsert_result.get('updated', 0)
                    
                    self.logger.info(f"Stored {len(whale_records)} whale records")
            
            # Log task completion
            status = "completed" if not self.failed_entities else "partial_success"
            
            task_metrics = {
                "new_whales": self.new_records,
                "updated_whales": self.updated_records,
                "total_whales": self.new_records + self.updated_records
            }
            
            error_summary = {"failed_whales": self.failed_entities} if self.failed_entities else None
            
            self.complete_task_logging(
                task_log, status,
                task_metrics=task_metrics,
                error_summary=error_summary
            )
            
            result = {
                "status": status,
                "processed_whales": self.processed_entities,
                "new_whales": self.new_records,
                "updated_whales": self.updated_records,
                "total_whales": self.new_records + self.updated_records,
                "failed_whales": self.failed_entities
            }
            
            self.logger.info(f"Silver whales task completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Silver whales task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            raise


def process_silver_whales(**context) -> Dict[str, Any]:
    """Main entry point for the silver whales task."""
    task = SilverWhalesTask(context)
    return task.execute()