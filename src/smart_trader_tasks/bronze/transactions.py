"""
Bronze transactions task - Fetch transaction history for whales from BirdEye API.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from sqlalchemy import select, text
from src.models.bronze import BronzeTransaction
from src.models.silver import SilverWhale
from src.database.connection import get_db_session
from src.config.settings import get_bronze_transactions_config
from smart_trader_tasks.common import BronzeTaskBase

logger = logging.getLogger(__name__)


class BronzeTransactionsTask(BronzeTaskBase):
    """Bronze transactions task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("bronze_transactions", context)
        self.config = get_bronze_transactions_config()
        
    def get_whales_to_process(self, session: Any) -> List[Dict[str, Any]]:
        """Get whales that need transaction processing."""
        try:
            # Use raw SQL for consistency
            raw_query = text("""
                SELECT wallet_address, tokens_held_count, transactions_processed_at
                FROM silver.silver_whales 
                WHERE transactions_processed = false 
                LIMIT :limit
            """)
            result = session.execute(raw_query, {"limit": self.config.max_wallets_per_run})
            whales_data = result.fetchall()
            
            self.logger.debug(f"Found {len(whales_data)} whales via raw SQL")
            
            whales_to_process = []
            for row in whales_data:
                whale_dict = {
                    'wallet_address': row._mapping.get('wallet_address'),
                    'tokens_held_count': row._mapping.get('tokens_held_count', 0),
                    'transactions_processed_at': row._mapping.get('transactions_processed_at')
                }
                whales_to_process.append(whale_dict)
            
            return whales_to_process
            
        except Exception as e:
            self.logger.error(f"Failed to get whales to process: {e}")
            raise
        
    def should_refetch_transactions(self, whale: Dict[str, Any]) -> bool:
        """Check if we need to refetch transactions based on refetch interval."""
        transactions_processed_at = whale.get('transactions_processed_at')
        if not transactions_processed_at:
            return True
            
        days_since_last_fetch = (datetime.utcnow() - transactions_processed_at).days
        return days_since_last_fetch >= self.config.refetch_interval_days
        
    def fetch_wallet_transactions(self, wallet_address: str) -> List[Dict[str, Any]]:
        """Fetch transaction data for a specific wallet using seek_by_time endpoint."""
        transactions_data = self.birdeye_client.get_wallet_transactions_seek_by_time(
            wallet_address=wallet_address,
            limit=self.config.max_transactions_per_wallet
        )
        
        if not transactions_data.get('success', False):
            raise Exception(f"API failed: {transactions_data.get('msg', 'Unknown error')}")
        
        # Use the new normalization method
        return self.birdeye_client.normalize_seek_by_time_response(transactions_data)
        
    def process_transactions(self, transactions: List[Dict[str, Any]], wallet_address: str) -> List[Dict[str, Any]]:
        """Process raw transaction data into records for a specific whale."""
        transaction_records = []
        
        for tx in transactions:
            # Filter by transaction type if configured
            tx_type = tx.get('tx_type', '').lower()
            if self.config.include_transaction_types and tx_type not in self.config.include_transaction_types:
                continue
            
            # Filter by minimum transaction value if configured
            tx_value = 0
            if tx.get('base_ui_change_amount') and tx.get('base_nearest_price'):
                base_amount = abs(tx['base_ui_change_amount'])
                base_price = tx['base_nearest_price']
                tx_value = base_amount * base_price
            
            if self.config.min_transaction_value_usd > 0 and tx_value < self.config.min_transaction_value_usd:
                continue
            
            transaction_record = {
                'transaction_hash': tx.get('transaction_hash', ''),
                'whale_id': f"whale_{wallet_address}",
                'wallet_address': wallet_address,
                'timestamp': tx.get('timestamp'),
                'tx_type': tx_type,
                'source': tx.get('source', ''),
                
                # Base token info
                'base_symbol': tx.get('base_symbol', ''),
                'base_address': tx.get('base_address', ''),
                'base_type_swap': tx.get('base_type_swap', ''),
                'base_ui_change_amount': tx.get('base_ui_change_amount'),
                'base_nearest_price': tx.get('base_nearest_price'),
                
                # Quote token info
                'quote_symbol': tx.get('quote_symbol', ''),
                'quote_address': tx.get('quote_address', ''),
                'quote_type_swap': tx.get('quote_type_swap', ''),
                'quote_ui_change_amount': tx.get('quote_ui_change_amount'),
                'quote_nearest_price': tx.get('quote_nearest_price'),
                
                # Metadata
                'fetched_at': datetime.utcnow(),
                'batch_id': self.run_id,
                'data_source': 'birdeye_v3'
            }
            transaction_records.append(transaction_record)
        
        return transaction_records
        
    def process_whale_transactions(self, session: Any, whale: Dict[str, Any]) -> int:
        """Process transactions for a single whale."""
        wallet_address = whale['wallet_address']
        tokens_held_count = whale.get('tokens_held_count', 0)
        
        try:
            # Create/update state for this whale
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="whale",
                entity_id=wallet_address,
                state="processing",
                metadata={"tokens_held": tokens_held_count}
            )
            
            # Check if we need to refetch
            if not self.should_refetch_transactions(whale):
                # Skipping wallet - recently fetched
                
                self.state_manager.create_or_update_state(
                    task_name=self.task_name,
                    entity_type="whale",
                    entity_id=wallet_address,
                    state="skipped",
                    metadata={"skip_reason": "recently_fetched"}
                )
                return 0
            
            # Fetch transaction data from BirdEye API
            # Fetching transactions
            transactions = self.fetch_wallet_transactions(wallet_address)
            
            if not transactions:
                # No transactions found
                # Still mark as completed since API call succeeded
                self._mark_whale_completed(session, wallet_address, 0)
                return 0
            
            # Process transactions into records
            transaction_records = self.process_transactions(transactions, wallet_address)
            
            if transaction_records:
                # Convert to DataFrame
                df = pd.DataFrame(transaction_records)
                
                # Deduplicate by transaction_hash, keeping the first occurrence
                # This handles cases where a single transaction involves multiple DEX protocols
                initial_count = len(df)
                df_deduped = df.drop_duplicates(subset=['transaction_hash'], keep='first')
                
                if len(df) != len(df_deduped):
                    # Deduplicated transactions
                    pass
                
                upsert_result = self.upsert_records(
                    session=session,
                    df=df_deduped,
                    model_class=BronzeTransaction,
                    conflict_columns=["transaction_hash"],
                    batch_size=self.config.batch_size
                )
                
                self.new_records += upsert_result.get('inserted', 0)
                self.updated_records += upsert_result.get('updated', 0)
                
                # Stored unique transactions
            
            # Mark whale as processed
            self._mark_whale_completed(session, wallet_address, len(df_deduped) if transaction_records else 0)
            
            # Rate limiting between wallet API calls
            time.sleep(self.config.wallet_api_delay)
            
            return len(df_deduped) if transaction_records else 0
            
        except Exception as e:
            self.handle_api_error(wallet_address, e, {"tokens_held": tokens_held_count})
            raise
            
    def _mark_whale_completed(self, session: Any, wallet_address: str, transactions_found: int):
        """Mark whale as completed in both state and silver_whales table."""
        # Update state
        self.state_manager.create_or_update_state(
            task_name=self.task_name,
            entity_type="whale",
            entity_id=wallet_address,
            state="completed",
            metadata={"transactions_found": transactions_found}
        )
        
        # Update silver_whales table using raw SQL
        update_query = text("""
            UPDATE silver.silver_whales 
            SET transactions_processed = true, transactions_processed_at = :timestamp 
            WHERE wallet_address = :wallet_address
        """)
        session.execute(update_query, {
            "timestamp": datetime.utcnow(), 
            "wallet_address": wallet_address
        })
        session.commit()
        
    def execute(self) -> Dict[str, Any]:
        """Execute the bronze transactions task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            with get_db_session() as session:
                # Get whales that need transaction processing
                whales_to_process = self.get_whales_to_process(session)
                
                if not whales_to_process:
                    self.logger.info("No whales need transaction processing")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_whales_to_process"}
                    )
                    return {
                        "status": "completed",
                        "processed_wallets": 0,
                        "new_transactions": 0,
                        "message": "No whales to process"
                    }
                
                # Only log final result
                
                # Process whales in batches
                for i in range(0, len(whales_to_process), self.config.wallet_batch_size):
                    batch = whales_to_process[i:i + self.config.wallet_batch_size]
                    
                    for whale in batch:
                        try:
                            transactions_count = self.process_whale_transactions(session, whale)
                            self.processed_entities += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing wallet {whale['wallet_address']}: {e}")
                            continue
                    
                    # Brief pause between batches
                    if i + self.config.wallet_batch_size < len(whales_to_process):
                        time.sleep(2)
            
            # Log task completion
            status = "completed" if not self.failed_entities else "partial_success"
            
            task_metrics = {
                "new_transactions": self.new_records,
                "updated_transactions": self.updated_records,
                "total_transactions": self.new_records + self.updated_records
            }
            
            error_summary = {"failed_wallets": self.failed_entities} if self.failed_entities else None
            
            self.complete_task_logging(
                task_log, status,
                task_metrics=task_metrics,
                error_summary=error_summary
            )
            
            result = {
                "status": status,
                "processed_wallets": self.processed_entities,
                "new_transactions": self.new_records,
                "updated_transactions": self.updated_records,
                "total_transactions": self.new_records + self.updated_records,
                "failed_wallets": self.failed_entities
            }
            
            self.logger.info(f"Bronze transactions: {status} - {self.processed_entities} wallets, {self.new_records + self.updated_records} transactions")
            return result
            
        except Exception as e:
            self.logger.error(f"Bronze transactions task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            raise


def process_bronze_transactions(**context) -> Dict[str, Any]:
    """Main entry point for the bronze transactions task."""
    task = BronzeTransactionsTask(context)
    return task.execute()