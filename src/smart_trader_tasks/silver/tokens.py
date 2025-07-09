"""
Silver tokens task - Select tokens from bronze layer for tracking.
Standardized implementation with simplified processing (all bronze tokens).
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_
from src.models.bronze import BronzeToken
from src.models.silver import SilverToken
from src.database.connection import get_db_session
from smart_trader_tasks.common import SilverTaskBase, safe_value
from src.config.settings import get_silver_tokens_config

logger = logging.getLogger(__name__)


class SilverTokensTask(SilverTaskBase):
    """Silver tokens task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("silver_tokens", context)
        self.config = get_silver_tokens_config()
        
    def get_unprocessed_bronze_tokens(self, session: Any) -> List[Dict[str, Any]]:
        """Get unprocessed bronze tokens."""
        bronze_query = session.query(BronzeToken).filter(
            BronzeToken.silver_processed == False
        )
        
        bronze_tokens_data = []
        for token in bronze_query.all():
            bronze_tokens_data.append({
                'token_address': token.token_address,
                'symbol': token.symbol,
                'name': token.name,
                'market_cap': token.market_cap,
                'liquidity': token.liquidity,
                'volume_24h_usd': token.volume_24h_usd,
                'price_change_24h_percent': token.price_change_24h_percent,
                'holder': token.holder,
                'trade_24h_count': token.trade_24h_count,
                'price': token.price,
                'ingested_at': token.ingested_at
            })
        
        return bronze_tokens_data
        
        
    def create_silver_tokens(self, bronze_tokens_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Create silver token records from bronze data."""
        df = pd.DataFrame(bronze_tokens_data)
        
        # Clean DataFrame immediately after creation
        import numpy as np
        df = df.replace([np.nan], [None])
        df = df.where(pd.notnull(df), None)
        
        # Process all bronze tokens without filtering (simplified approach)
        self.logger.info("Processing all bronze tokens without filtering...")
        
        # Prepare silver tokens data with simple structure
        silver_tokens_data = []
        for _, token in df.iterrows():
            # Convert holder to int if it's not None and not NaN
            holder_count = token['holder']
            if holder_count is not None and not pd.isna(holder_count) and isinstance(holder_count, float):
                holder_count = int(holder_count)
            elif pd.isna(holder_count):
                holder_count = None
            
            silver_token = {
                'token_address': safe_value(token['token_address']),
                'selection_score': 1.0,  # Simple default score
                'selection_reason': {
                    'selection_criteria': 'all_bronze_tokens',
                    'rank': len(silver_tokens_data) + 1
                },
                'selected_at': datetime.utcnow(),
                'whales_processed': False,
                'whales_processed_at': None,
                # Denormalized data with safe value conversion
                'symbol': safe_value(token['symbol']),
                'name': safe_value(token['name']),
                'market_cap': safe_value(token['market_cap']),
                'price_change_24h_percent': safe_value(token['price_change_24h_percent']),
                'volume_24h_usd': safe_value(token['volume_24h_usd']),
                'liquidity': safe_value(token['liquidity']),
                'holder_count': holder_count,
                'price': safe_value(token['price'])
            }
            silver_tokens_data.append(silver_token)
        
        return silver_tokens_data
        
    def mark_bronze_tokens_processed(self, session: Any, token_addresses: List[str]):
        """Mark bronze tokens as processed for silver layer."""
        try:
            # Update bronze tokens to mark as processed
            session.query(BronzeToken).filter(
                BronzeToken.token_address.in_(token_addresses)
            ).update({
                BronzeToken.silver_processed: True,
                BronzeToken.silver_processed_at: datetime.utcnow()
            }, synchronize_session=False)
            
            session.commit()
            
            self.logger.info(f"Marked {len(token_addresses)} bronze tokens as silver_processed=True")
            
        except Exception as e:
            self.logger.error(f"Failed to mark bronze tokens as processed: {e}")
            session.rollback()
            raise
            
    def execute(self) -> Dict[str, Any]:
        """Execute the silver tokens task."""
        task_log = self.start_task_logging({"approach": "simplified_all_tokens"})
        
        try:
            self.logger.info("Starting silver tokens selection - processing all bronze tokens")
            
            with get_db_session() as session:
                # Get unprocessed bronze tokens
                bronze_tokens_data = self.get_unprocessed_bronze_tokens(session)
                
                if not bronze_tokens_data:
                    self.logger.info("No unprocessed bronze tokens found")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_unprocessed_tokens"}
                    )
                    return {"status": "no_data", "records": 0}
                
                self.logger.info(f"Found {len(bronze_tokens_data)} unprocessed bronze tokens")
                
                # Create silver tokens (simplified - all bronze tokens)
                silver_tokens_data = self.create_silver_tokens(bronze_tokens_data)
                selected_tokens = len(silver_tokens_data)
                
                self.logger.info(f"Selected {selected_tokens} tokens (all unprocessed bronze tokens)")
                
                # Insert selected tokens into silver table
                if silver_tokens_data:
                    silver_df = pd.DataFrame(silver_tokens_data)
                    
                    upsert_result = self.upsert_records(
                        session=session,
                        df=silver_df,
                        model_class=SilverToken,
                        conflict_columns=["token_address"],
                        batch_size=self.config.batch_size
                    )
                    
                    self.logger.info(f"Silver tokens UPSERT result: {upsert_result}")
                    self.new_records = upsert_result.get('inserted', 0)
                    self.updated_records = upsert_result.get('updated', 0)
                    
                    # Update state for selected tokens
                    selected_addresses = [t['token_address'] for t in silver_tokens_data]
                    self.state_manager.mark_entities_completed(
                        task_name=self.task_name,
                        entity_type="token",
                        entity_ids=selected_addresses,
                        metadata={"selection_method": "all_bronze_tokens"}
                    )
                
                # Mark ALL bronze tokens as processed (selected and non-selected)
                all_token_addresses = [t['token_address'] for t in bronze_tokens_data]
                self.mark_bronze_tokens_processed(session, all_token_addresses)
                self.processed_entities = len(all_token_addresses)
                
                self.logger.info(f"Marked {self.processed_entities} bronze tokens as processed")
            
            # Task completion
            task_metrics = {
                "tokens_evaluated": len(bronze_tokens_data),
                "tokens_selected": selected_tokens,
                "selection_method": "all_bronze_tokens"
            }
            
            self.complete_task_logging(
                task_log, "success",
                task_metrics=task_metrics
            )
            
            self.logger.info(f"Silver tokens task completed: {selected_tokens} tokens selected from {self.processed_entities} evaluated")
            
            return {
                "status": "success",
                "records": self.processed_entities,
                "selected_tokens": selected_tokens,
                "evaluated_tokens": len(bronze_tokens_data)
            }
            
        except Exception as e:
            self.logger.error(f"Silver tokens task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            return {
                "status": "failed",
                "error": str(e),
                "records": self.processed_entities
            }


def process_silver_tokens(**context) -> Dict[str, Any]:
    """Main entry point for the silver tokens task."""
    task = SilverTokensTask(context)
    return task.execute()