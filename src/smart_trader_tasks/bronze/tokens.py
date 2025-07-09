"""
Bronze tokens task - Fetch trending tokens from BirdEye API.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, Any, List
from src.models.bronze import BronzeToken
from src.database.connection import get_db_session
from src.config.settings import get_bronze_tokens_config
from smart_trader_tasks.common import BronzeTaskBase

logger = logging.getLogger(__name__)


class BronzeTokensTask(BronzeTaskBase):
    """Bronze tokens task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("bronze_tokens", context)
        self.config = get_bronze_tokens_config()
        
    def fetch_tokens_from_api(self) -> List[Dict[str, Any]]:
        """Fetch tokens from BirdEye API with pagination."""
        # Build filter parameters
        filter_params = {
            "min_liquidity": self.config.min_liquidity,
            "max_liquidity": self.config.max_liquidity,
            "min_volume_1h_usd": self.config.min_volume_1h_usd,
            "min_price_change_2h_percent": self.config.min_price_change_2h_percent,
            "min_price_change_24h_percent": self.config.min_price_change_24h_percent
        }
        
        self.logger.info(f"Filter parameters: {filter_params}")
        
        # Fetch tokens with pagination
        all_tokens = []
        offset = 0
        api_call_count = 0
        
        while len(all_tokens) < self.config.token_limit:
            remaining = self.config.token_limit - len(all_tokens)
            current_limit = min(self.config.api_pagination_limit, remaining)
            
            params = {
                "sort_by": "liquidity",
                "sort_type": "desc",
                "offset": offset,
                "limit": current_limit,
                **filter_params
            }
            
            api_call_count += 1
            
            try:
                response = self.birdeye_client.get_token_list(**params)
                tokens_data = self.birdeye_client.normalize_token_list_response(response)
                
                if not tokens_data:
                    self.logger.warning(f"No more tokens returned from API at offset {offset}")
                    break
                
                all_tokens.extend(tokens_data)
                offset += len(tokens_data)
                
                # Break if we got fewer tokens than requested (likely end of results)
                if len(tokens_data) < current_limit:
                    self.logger.info("Received fewer tokens than requested, likely end of results")
                    break
                
                # Rate limiting
                time.sleep(self.config.api_rate_limit_delay)
                
            except Exception as e:
                self.logger.error(f"API call #{api_call_count} failed: {e}")
                self.failed_entities.append(f"api_call_{api_call_count}")
                break
        
        # Limit to configured maximum
        return all_tokens[:self.config.token_limit]
        
    def process_tokens_batch(self, session: Any, tokens_batch: List[Dict[str, Any]]) -> int:
        """Process a batch of tokens."""
        # Convert to DataFrame
        df = pd.DataFrame(tokens_batch)
        
        # Add metadata columns
        df['ingested_at'] = datetime.utcnow()
        df['silver_processed'] = False
        df['silver_processed_at'] = None
        df['batch_id'] = self.run_id
        df['processing_date'] = datetime.utcnow().date()
        
        # Get existing tokens to track new vs updated
        existing_addresses = []
        if tokens_batch:
            existing_query = session.query(BronzeToken.token_address).filter(
                BronzeToken.token_address.in_([t['token_address'] for t in tokens_batch])
            )
            existing_addresses = [addr[0] for addr in existing_query.all()]
        
        # Calculate new vs updated
        batch_new = len([t for t in tokens_batch if t['token_address'] not in existing_addresses])
        batch_updated = len(tokens_batch) - batch_new
        
        self.new_records += batch_new
        self.updated_records += batch_updated
        
        # Perform UPSERT operation
        upsert_result = self.upsert_records(
            session=session,
            df=df,
            model_class=BronzeToken,
            conflict_columns=["token_address"],
            batch_size=self.config.batch_size
        )
        
        # Batch upsert completed
        
        # Update state for processed tokens
        token_addresses = [t['token_address'] for t in tokens_batch]
        self.state_manager.mark_entities_completed(
            task_name=self.task_name,
            entity_type="token",
            entity_ids=token_addresses,
            metadata={"batch_size": len(tokens_batch)}
        )
        
        return len(tokens_batch)
        
    def execute(self) -> Dict[str, Any]:
        """Execute the bronze tokens task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            self.logger.info(f"Starting bronze tokens task with config: TOKEN_LIMIT={self.config.token_limit}")
            
            # Fetch tokens from API
            tokens_to_process = self.fetch_tokens_from_api()
            
            if not tokens_to_process:
                self.logger.warning("No tokens to process")
                self.complete_task_logging(
                    task_log, "completed", 
                    task_metrics={"reason": "no_tokens_found"}
                )
                return {"status": "no_data", "records": 0}
            
            self.logger.info(f"Total tokens to process: {len(tokens_to_process)}")
            
            # Process tokens in batches
            batch_count = 0
            with get_db_session() as session:
                for i in range(0, len(tokens_to_process), self.config.batch_size):
                    batch_tokens = tokens_to_process[i:i + self.config.batch_size]
                    batch_count += 1
                    
                    # Processing batch
                    
                    try:
                        processed_count = self.process_tokens_batch(session, batch_tokens)
                        self.processed_entities += processed_count
                        # Batch completed
                        
                    except Exception as e:
                        self.logger.error(f"Batch {batch_count} failed: {e}")
                        batch_addresses = [t['token_address'] for t in batch_tokens]
                        self.failed_entities.extend(batch_addresses)
                        
                        # Mark failed tokens in state
                        self.state_manager.mark_entities_failed(
                            task_name=self.task_name,
                            entity_type="token",
                            entity_ids=batch_addresses,
                            error_message=str(e),
                            metadata={"batch": batch_count}
                        )
                        continue
            
            # Task completion
            status = "success" if not self.failed_entities else "partial_success"
            
            task_metrics = {
                "batches_processed": batch_count,
                "new_tokens": self.new_records,
                "updated_tokens": self.updated_records,
                "total_tokens": len(tokens_to_process)
            }
            
            error_summary = None
            if self.failed_entities:
                error_summary = {
                    "failed_count": len(self.failed_entities),
                    "failed_tokens": self.failed_entities[:10]  # Limit for logging
                }
            
            self.complete_task_logging(
                task_log, status, 
                task_metrics=task_metrics, 
                error_summary=error_summary
            )
            
            self.logger.info(f"Bronze tokens task completed: {self.processed_entities} tokens processed "
                           f"({self.new_records} new, {self.updated_records} updated), "
                           f"{len(self.failed_entities)} failed")
            
            return {
                "status": status,
                "records": self.processed_entities,
                "new_records": self.new_records,
                "updated_records": self.updated_records,
                "failed_records": len(self.failed_entities),
                "batches": batch_count
            }
            
        except Exception as e:
            self.logger.error(f"Bronze tokens task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed", 
                error_summary={"error": str(e)}
            )
            
            return {
                "status": "failed",
                "error": str(e),
                "records": self.processed_entities
            }


def process_bronze_tokens(**context) -> Dict[str, Any]:
    """Main entry point for the bronze tokens task."""
    task = BronzeTokensTask(context)
    return task.execute()