"""
Bronze transactions task - Fetch transaction history for whales from BirdEye API.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from sqlalchemy import select
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
        
    def get_whales_to_process(self, session: Any) -> List[Any]:
        """Get whales that need transaction processing."""
        whales_query = select(SilverWhale).where(
            SilverWhale.transactions_processed == False
        ).limit(self.config.max_wallets_per_run)
        
        return list(session.exec(whales_query))
        
    def should_refetch_transactions(self, whale: Any) -> bool:
        """Check if we need to refetch transactions based on refetch interval."""
        if not whale.transactions_processed_at:
            return True
            
        days_since_last_fetch = (datetime.utcnow() - whale.transactions_processed_at).days
        return days_since_last_fetch >= self.config.refetch_interval_days
        
    def fetch_wallet_transactions(self, wallet_address: str) -> List[Dict[str, Any]]:
        """Fetch transaction data for a wallet."""
        # Calculate date range for transaction fetching
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=self.config.lookback_days)
        
        transactions_data = self.birdeye_client.get_wallet_transactions(
            wallet_address=wallet_address,
            limit=self.config.max_transactions_per_wallet,
            before=int(end_date.timestamp()),
            after=int(start_date.timestamp())
        )
        
        if not transactions_data.get('success', False):
            raise Exception(f"API failed: {transactions_data.get('msg', 'Unknown error')}")
        
        return transactions_data.get('data', {}).get('items', [])
        
    def process_transactions(self, transactions: List[Dict[str, Any]], wallet_address: str) -> List[Dict[str, Any]]:
        """Process raw transaction data into records."""
        transaction_records = []
        
        for tx in transactions:
            # Filter by transaction type if configured
            tx_type = tx.get('txType', '').lower()
            if self.config.include_transaction_types and tx_type not in self.config.include_transaction_types:
                continue
            
            # Filter by minimum transaction value if configured
            tx_value = 0
            if tx.get('base') and tx.get('base', {}).get('uiChangeAmount'):
                base_amount = abs(tx['base']['uiChangeAmount'])
                base_price = tx['base'].get('nearestPrice', 0)
                tx_value = base_amount * base_price
            
            if self.config.min_transaction_value_usd > 0 and tx_value < self.config.min_transaction_value_usd:
                continue
            
            # Parse transaction timestamp
            tx_timestamp = datetime.utcfromtimestamp(tx.get('blockUnixTime', 0))
            
            # Extract base token info
            base_info = tx.get('base', {})
            quote_info = tx.get('quote', {})
            
            transaction_record = {
                'transaction_hash': tx.get('txHash', ''),
                'whale_id': f"whale_{wallet_address}",
                'wallet_address': wallet_address,
                'timestamp': tx_timestamp,
                'tx_type': tx_type,
                'source': tx.get('source', ''),
                
                # Base token info
                'base_symbol': base_info.get('symbol', ''),
                'base_address': base_info.get('address', ''),
                'base_type_swap': base_info.get('typeSwap', ''),
                'base_ui_change_amount': base_info.get('uiChangeAmount', 0),
                'base_nearest_price': base_info.get('nearestPrice', 0),
                
                # Quote token info
                'quote_symbol': quote_info.get('symbol', ''),
                'quote_address': quote_info.get('address', ''),
                'quote_type_swap': quote_info.get('typeSwap', ''),
                'quote_ui_change_amount': quote_info.get('uiChangeAmount', 0),
                'quote_nearest_price': quote_info.get('nearestPrice', 0),
                
                # Metadata
                'fetched_at': datetime.utcnow(),
                'batch_id': self.run_id,
                'data_source': 'birdeye_v3'
            }
            transaction_records.append(transaction_record)
        
        return transaction_records
        
    def process_whale_transactions(self, session: Any, whale: Any) -> int:
        """Process transactions for a single whale."""
        wallet_address = whale.wallet_address
        
        try:
            # Create/update state for this whale
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="whale",
                entity_id=wallet_address,
                state="processing",
                metadata={"tokens_held": whale.tokens_held_count}
            )
            
            # Check if we need to refetch
            if not self.should_refetch_transactions(whale):
                self.logger.info(f"Skipping wallet {wallet_address} - recently fetched")
                
                self.state_manager.create_or_update_state(
                    task_name=self.task_name,
                    entity_type="whale",
                    entity_id=wallet_address,
                    state="skipped",
                    metadata={"skip_reason": "recently_fetched"}
                )
                return 0
            
            # Fetch transaction data from BirdEye API
            self.logger.info(f"Fetching transactions for whale {wallet_address}")
            transactions = self.fetch_wallet_transactions(wallet_address)
            
            if not transactions:
                self.logger.info(f"No transactions found for wallet {wallet_address}")
                # Still mark as completed since API call succeeded
                self._mark_whale_completed(session, whale, 0)
                return 0
            
            # Process transactions into records
            transaction_records = self.process_transactions(transactions, wallet_address)
            
            if transaction_records:
                # Convert to DataFrame and upsert
                df = pd.DataFrame(transaction_records)
                
                upsert_result = self.upsert_records(
                    session=session,
                    df=df,
                    model_class=BronzeTransaction,
                    conflict_columns=["transaction_hash"],
                    batch_size=self.config.batch_size
                )
                
                self.new_records += upsert_result.get('inserted', 0)
                self.updated_records += upsert_result.get('updated', 0)
                
                self.logger.info(f"Stored {len(transaction_records)} transactions for wallet {wallet_address}")
            
            # Mark whale as processed
            self._mark_whale_completed(session, whale, len(transaction_records))
            
            # Rate limiting between wallet API calls
            time.sleep(self.config.wallet_api_delay)
            
            return len(transaction_records)
            
        except Exception as e:
            self.handle_api_error(wallet_address, e, {"tokens_held": whale.tokens_held_count})
            raise
            
    def _mark_whale_completed(self, session: Any, whale: Any, transactions_found: int):
        """Mark whale as completed in both state and silver_whales table."""
        # Update state
        self.state_manager.create_or_update_state(
            task_name=self.task_name,
            entity_type="whale",
            entity_id=whale.wallet_address,
            state="completed",
            metadata={"transactions_found": transactions_found}
        )
        
        # Update silver_whales table
        whale.transactions_processed = True
        whale.transactions_processed_at = datetime.utcnow()
        session.add(whale)
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
                
                self.logger.info(f"Processing {len(whales_to_process)} whales for transaction data")
                
                # Process whales in batches
                for i in range(0, len(whales_to_process), self.config.batch_size):
                    batch = whales_to_process[i:i + self.config.batch_size]
                    
                    for whale in batch:
                        try:
                            transactions_count = self.process_whale_transactions(session, whale)
                            self.processed_entities += 1
                            
                        except Exception as e:
                            self.logger.error(f"Error processing wallet {whale.wallet_address}: {e}")
                            continue
                    
                    # Brief pause between batches
                    if i + self.config.batch_size < len(whales_to_process):
                        time.sleep(2)
            
            # Log task completion
            status = "completed" if not self.failed_entities else "completed_with_errors"
            
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
            
            self.logger.info(f"Bronze transactions task completed: {result}")
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