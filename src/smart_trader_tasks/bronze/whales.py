"""
Bronze whales task - Fetch top holders for selected tokens from BirdEye API.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import select, text
from src.models.bronze import BronzeWhale
from src.models.silver import SilverToken
from src.database.connection import get_db_session
from src.config.settings import get_bronze_whales_config
from smart_trader_tasks.common import BronzeTaskBase

logger = logging.getLogger(__name__)


class BronzeWhalesTask(BronzeTaskBase):
    """Bronze whales task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("bronze_whales", context)
        self.config = get_bronze_whales_config()
        
    def get_tokens_to_process(self, session: Any) -> List[Dict[str, str]]:
        """Get tokens that need whale processing."""
        try:
            # Use raw SQL for consistency
            raw_query = text("""
                SELECT token_address, symbol, name, whales_processed 
                FROM silver.silver_tokens 
                WHERE whales_processed = false 
                LIMIT :limit
            """)
            result = session.execute(raw_query, {"limit": self.config.max_tokens_per_run})
            tokens_data = result.fetchall()
            
            self.logger.info(f"Found {len(tokens_data)} tokens via raw SQL")
            
            tokens_to_process = []
            for row in tokens_data:
                token_dict = {
                    'token_address': row._mapping.get('token_address'),
                    'symbol': row._mapping.get('symbol') or 'unknown',
                    'name': row._mapping.get('name') or 'unknown'
                }
                tokens_to_process.append(token_dict)
            
            return tokens_to_process
            
        except Exception as e:
            self.logger.error(f"Failed to get tokens to process: {e}")
            raise
            
    def fetch_whale_data(self, token_address: str) -> List[Dict[str, Any]]:
        """Fetch whale data for a single token."""
        whale_response = self.birdeye_client.get_token_holders(
            token_address=token_address,
            limit=self.config.top_holders_limit
        )
        
        if not whale_response.get('success', False):
            raise Exception(f"API failed: {whale_response.get('msg', 'Unknown error')}")
        
        # Normalize the response using the client's method
        holders = self.birdeye_client.normalize_holders_response(whale_response)
        return holders or []
        
    def process_token_whales(
        self, 
        session: Any, 
        token: Dict[str, str]
    ) -> int:
        """Process whales for a single token."""
        token_address = token['token_address']
        token_symbol = token['symbol']
        token_name = token['name']
        
        self.logger.info(f"Processing token {token_symbol} ({token_address})")
        
        try:
            # Create/update state for this token
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="token",
                entity_id=token_address,
                state="processing",
                metadata={"token_symbol": token_symbol}
            )
            
            # Fetch whale data from BirdEye API
            self.logger.info(f"Fetching whale data for token {token_symbol}")
            holders = self.fetch_whale_data(token_address)
            
            if not holders:
                self.logger.warning(f"No holder data found for token {token_address}")
                # Still mark as completed since API call succeeded
                self._mark_token_completed(session, token_address, 0)
                return 0
            
            # Process holders into records
            whale_records = []
            for holder in holders:
                whale_record = {
                    'token_address': token_address,
                    'wallet_address': holder['wallet_address'],
                    'token_symbol': token_symbol,
                    'token_name': token_name,
                    'rank': holder['rank'],
                    'amount': holder['amount'],
                    'ui_amount': holder['ui_amount'],
                    'decimals': holder['decimals'],
                    'mint': holder['mint'],
                    'token_account': holder['token_account'],
                    'fetched_at': datetime.utcnow(),
                    'batch_id': self.run_id,
                    'data_source': 'birdeye_v3'
                }
                whale_records.append(whale_record)
            
            if whale_records:
                # Convert to DataFrame
                df = pd.DataFrame(whale_records)
                
                # Deduplicate by token_address + wallet_address, keeping the best rank
                df_grouped = df.drop_duplicates(subset=['token_address', 'wallet_address'], keep='first')
                
                if len(df) != len(df_grouped):
                    self.logger.info(f"Merged {len(df)} holdings into {len(df_grouped)} unique wallet positions")
                
                upsert_result = self.upsert_records(
                    session=session,
                    df=df_grouped,
                    model_class=BronzeWhale,
                    conflict_columns=["token_address", "wallet_address"],
                    batch_size=self.config.batch_size
                )
                
                self.new_records += upsert_result.get('inserted', 0)
                self.updated_records += upsert_result.get('updated', 0)
                
                self.logger.info(f"Stored {len(whale_records)} whale records for token {token_symbol}")
            
            # Mark token as completed
            self._mark_token_completed(session, token_address, len(whale_records))
            
            # Rate limiting
            time.sleep(self.config.api_rate_limit_delay)
            
            return len(whale_records)
            
        except Exception as e:
            self.handle_api_error(token_address, e, {"token_symbol": token_symbol})
            raise
            
    def _mark_token_completed(self, session: Any, token_address: str, holders_found: int):
        """Mark token as completed in both state and silver_tokens table."""
        # Update state
        self.state_manager.create_or_update_state(
            task_name=self.task_name,
            entity_type="token",
            entity_id=token_address,
            state="completed",
            metadata={"holders_found": holders_found}
        )
        
        # Update silver_tokens table
        update_query = text("""
            UPDATE silver.silver_tokens 
            SET whales_processed = true, whales_processed_at = :timestamp 
            WHERE token_address = :token_address
        """)
        session.execute(update_query, {
            "timestamp": datetime.utcnow(), 
            "token_address": token_address
        })
        session.commit()
        
    def execute(self) -> Dict[str, Any]:
        """Execute the bronze whales task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            with get_db_session() as session:
                # Get tokens that need whale processing
                tokens_to_process = self.get_tokens_to_process(session)
                
                if not tokens_to_process:
                    self.logger.info("No tokens need whale processing")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_tokens_to_process"}
                    )
                    return {
                        "status": "completed",
                        "processed_tokens": 0,
                        "new_whales": 0,
                        "message": "No tokens to process"
                    }
                
                self.logger.info(f"Processing {len(tokens_to_process)} tokens for whale data")
                
                # Process tokens in batches
                for i in range(0, len(tokens_to_process), self.config.batch_size):
                    batch = tokens_to_process[i:i + self.config.batch_size]
                    
                    for token in batch:
                        token_address = token['token_address']
                        
                        try:
                            whales_count = self.process_token_whales(session, token)
                            self.processed_entities += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing token {token_address}: {e}")
                            continue
                    
                    # Brief pause between batches
                    if i + self.config.batch_size < len(tokens_to_process):
                        time.sleep(1)
            
            # Log task completion
            status = "completed" if not self.failed_entities else "partial_success"
            
            task_metrics = {
                "new_whales": self.new_records,
                "updated_whales": self.updated_records,
                "total_whales": self.new_records + self.updated_records
            }
            
            error_summary = {"failed_tokens": self.failed_entities} if self.failed_entities else None
            
            self.complete_task_logging(
                task_log, status,
                task_metrics=task_metrics,
                error_summary=error_summary
            )
            
            result = {
                "status": status,
                "processed_tokens": self.processed_entities,
                "new_whales": self.new_records,
                "updated_whales": self.updated_records,
                "total_whales": self.new_records + self.updated_records,
                "failed_tokens": self.failed_entities
            }
            
            self.logger.info(f"Bronze whales task completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Bronze whales task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            raise


def process_bronze_whales(**context) -> Dict[str, Any]:
    """Main entry point for the bronze whales task."""
    task = BronzeWhalesTask(context)
    return task.execute()