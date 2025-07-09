"""
Bronze whales task - Fetch top holders for selected tokens from BirdEye API.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
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
        """Get tokens that need whale processing, including those due for refresh."""
        try:
            # Query tokens that either haven't been processed or are due for refresh
            raw_query = text("""
                SELECT token_address, symbol, name, holder_count, whales_processed, whales_processed_at
                FROM silver.silver_tokens 
                WHERE whales_processed = false 
                   OR (whales_processed = true 
                       AND whales_processed_at < NOW() - MAKE_INTERVAL(hours => :hours))
                LIMIT :limit
            """)
            result = session.execute(raw_query, {
                "hours": self.config.refetch_interval_hours,
                "limit": self.config.max_tokens_per_run
            })
            tokens_data = result.fetchall()
            
            self.logger.info(f"Found {len(tokens_data)} tokens via raw SQL")
            
            tokens_to_process = []
            refetch_count = 0
            for row in tokens_data:
                token_dict = {
                    'token_address': row._mapping.get('token_address'),
                    'symbol': row._mapping.get('symbol') or 'unknown',
                    'name': row._mapping.get('name') or 'unknown',
                    'holder_count': row._mapping.get('holder_count'),
                    'whales_processed': row._mapping.get('whales_processed', False),
                    'whales_processed_at': row._mapping.get('whales_processed_at')
                }
                if token_dict['whales_processed']:
                    refetch_count += 1
                tokens_to_process.append(token_dict)
            
            if refetch_count > 0:
                self.logger.info(f"Including {refetch_count} tokens for refetch after interval")
            
            return tokens_to_process
            
        except Exception as e:
            self.logger.error(f"Failed to get tokens to process: {e}")
            raise
            
    def fetch_whale_data(self, token_address: str, holder_count: Optional[int] = None) -> List[Dict[str, Any]]:
        """Fetch whale data for a single token, skipping top holders."""
        # Calculate offset to skip top percentage of holders
        offset = 0
        limit = self.config.holders_per_token
        
        if holder_count and holder_count > 0:
            # Skip top percentage of holders (exchanges, protocols, etc.)
            skip_count = int(holder_count * (self.config.skip_top_percentage / 100.0))
            offset = skip_count
            
            # Ensure we don't exceed total holders
            if offset + limit > holder_count:
                limit = max(1, holder_count - offset)
            
            self.logger.info(f"Token {token_address}: {holder_count} holders, skipping top {skip_count} ({self.config.skip_top_percentage}%), fetching holders {offset+1}-{offset+limit}")
        else:
            # No holder count available, use legacy approach
            self.logger.warning(f"Token {token_address}: No holder count available, using legacy top holders approach")
            limit = self.config.top_holders_limit
        
        whale_response = self.birdeye_client.get_token_holders(
            token_address=token_address,
            limit=limit,
            offset=offset
        )
        
        if not whale_response.get('success', False):
            raise Exception(f"API failed: {whale_response.get('msg', 'Unknown error')}")
        
        # Normalize the response using the client's method
        holders = self.birdeye_client.normalize_holders_response(whale_response)
        return holders or []
    
    def should_refetch_whales(self, whales_processed_at: Optional[datetime]) -> bool:
        """Check if we need to refetch whales based on refetch interval."""
        if not whales_processed_at:
            return True
            
        hours_since_last_fetch = (datetime.utcnow() - whales_processed_at).total_seconds() / 3600
        return hours_since_last_fetch >= self.config.refetch_interval_hours
        
    def process_token_whales(
        self, 
        session: Any, 
        token: Dict[str, str]
    ) -> int:
        """Process whales for a single token."""
        token_address = token['token_address']
        token_symbol = token['symbol']
        token_name = token['name']
        holder_count = token.get('holder_count')
        
        # Processing token
        
        try:
            # Create/update state for this token
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="token",
                entity_id=token_address,
                state="processing",
                metadata={"token_symbol": token_symbol, "holder_count": holder_count}
            )
            
            # Fetch whale data from BirdEye API
            # Fetching whale data
            holders = self.fetch_whale_data(token_address, holder_count)
            
            if not holders:
                self.logger.warning(f"No holder data found for token {token_address}")
                # Still mark as completed since API call succeeded
                self._mark_token_completed(session, token_address, 0)
                self._mark_bronze_whales_processed(session, token_address)
                return 0
            
            # Get existing whale records to preserve state
            existing_whales = set()
            if holders:
                existing_query = session.query(BronzeWhale.token_address, BronzeWhale.wallet_address).filter(
                    BronzeWhale.token_address == token_address,
                    BronzeWhale.wallet_address.in_([h['wallet_address'] for h in holders])
                )
                existing_whales = {(row[0], row[1]) for row in existing_query.all()}

            # Process holders into records
            whale_records = []
            for holder in holders:
                whale_key = (token_address, holder['wallet_address'])
                is_existing = whale_key in existing_whales
                
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
                    'data_source': 'birdeye_v3',
                    # Only set processing state for NEW whales
                    'silver_processed': False if not is_existing else None,
                    'silver_processed_at': None if not is_existing else None
                }
                whale_records.append(whale_record)
            
            if whale_records:
                # Convert to DataFrame
                df = pd.DataFrame(whale_records)
                
                # Handle None values in processing state fields for new records
                df.loc[df['silver_processed'].isna(), 'silver_processed'] = False
                df.loc[df['silver_processed_at'].isna(), 'silver_processed_at'] = None
                
                # Deduplicate by token_address + wallet_address, keeping the best rank
                df_grouped = df.drop_duplicates(subset=['token_address', 'wallet_address'], keep='first')
                
                if len(df) != len(df_grouped):
                    # Merged holdings into unique wallet positions
                    pass
                
                # Calculate new vs updated whales
                new_whales = len([r for r in whale_records if not r.get('silver_processed') is None])
                updated_whales = len(whale_records) - new_whales
                
                self.logger.info(f"Token {token_address}: {new_whales} new whales, {updated_whales} updated whales (preserving processing state)")
                
                # Define columns to update during conflict (exclude processing state fields)
                update_columns = [
                    'token_symbol', 'token_name', 'rank', 'amount', 'ui_amount', 
                    'decimals', 'mint', 'token_account', 'fetched_at', 'batch_id', 'data_source'
                ]
                
                upsert_result = self.upsert_records(
                    session=session,
                    df=df_grouped,
                    model_class=BronzeWhale,
                    conflict_columns=["token_address", "wallet_address"],
                    update_columns=update_columns,
                    batch_size=self.config.batch_size
                )
                
                self.new_records += upsert_result.get('inserted', 0)
                self.updated_records += upsert_result.get('updated', 0)
                
                # Stored whale records
            
            # Mark token as completed (whales remain unprocessed for silver layer)
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
                
                # Count refetch vs new tokens
                refetch_tokens = sum(1 for t in tokens_to_process if t.get('whales_processed', False))
                new_tokens = len(tokens_to_process) - refetch_tokens
                self.logger.info(f"Processing {len(tokens_to_process)} tokens for whale data ({new_tokens} new, {refetch_tokens} refetch)")
                
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