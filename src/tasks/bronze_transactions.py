"""
Bronze transactions task - Fetch transaction history for whales from BirdEye API.
Stores transaction data for each whale in bronze_transactions table.
"""

import logging
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from sqlalchemy import and_, select
from src.services.birdeye_client import BirdEyeAPIClient
from src.models.bronze import BronzeTransaction
from src.models.silver import SilverWhale
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings, get_bronze_transactions_config

logger = logging.getLogger(__name__)


def process_bronze_transactions(**context) -> Dict[str, Any]:
    """
    Fetch transaction history for each whale from silver_whales table.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    settings = get_settings()
    config = get_bronze_transactions_config()
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="bronze_transactions",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"config": config.__dict__}
    )
    
    state_manager = StateManager()
    processed_wallets = 0
    new_transactions = 0
    updated_transactions = 0
    failed_wallets = []
    
    try:
        # Initialize BirdEye client
        api_key = settings.get_birdeye_api_key()
        birdeye_client = BirdEyeAPIClient(api_key)
        
        with get_db_session() as session:
            # Get whales that need transaction processing
            whales_query = select(SilverWhale).where(
                SilverWhale.transactions_processed == False
            ).limit(config.max_wallets_per_run)
            
            whales_to_process = session.exec(whales_query).all()
            
            if not whales_to_process:
                logger.info("No whales need transaction processing")
                return {
                    "status": "completed",
                    "processed_wallets": 0,
                    "new_transactions": 0,
                    "message": "No whales to process"
                }
            
            logger.info(f"Processing {len(whales_to_process)} whales for transaction data")
            
            # Calculate date range for transaction fetching
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=config.lookback_days)
            
            # Process whales in batches
            for i in range(0, len(whales_to_process), config.batch_size):
                batch = whales_to_process[i:i + config.batch_size]
                
                for whale in batch:
                    wallet_address = whale.wallet_address
                    
                    try:
                        # Create/update state for this whale
                        state_manager.create_or_update_state(
                            task_name="bronze_transactions",
                            entity_type="whale",
                            entity_id=wallet_address,
                            state="processing",
                            metadata={"tokens_held": whale.tokens_held_count}
                        )
                        
                        # Fetch transaction data from BirdEye API
                        logger.info(f"Fetching transactions for whale {wallet_address}")
                        
                        # Check if we need to refetch (based on refetch interval)
                        if whale.transactions_processed_at:
                            days_since_last_fetch = (datetime.utcnow() - whale.transactions_processed_at).days
                            if days_since_last_fetch < config.refetch_interval_days:
                                logger.info(f"Skipping wallet {wallet_address} - last fetched {days_since_last_fetch} days ago")
                                
                                state_manager.create_or_update_state(
                                    task_name="bronze_transactions",
                                    entity_type="whale",
                                    entity_id=wallet_address,
                                    state="skipped",
                                    metadata={"skip_reason": "recently_fetched"}
                                )
                                continue
                        
                        transactions_data = birdeye_client.get_wallet_transactions(
                            wallet_address=wallet_address,
                            limit=config.max_transactions_per_wallet,
                            before=int(end_date.timestamp()),
                            after=int(start_date.timestamp())
                        )
                        
                        if not transactions_data.get('success', False):
                            logger.warning(f"API failed for wallet {wallet_address}: {transactions_data}")
                            failed_wallets.append(wallet_address)
                            
                            state_manager.create_or_update_state(
                                task_name="bronze_transactions",
                                entity_type="whale",
                                entity_id=wallet_address,
                                state="failed",
                                error_message=f"API failed: {transactions_data.get('msg', 'Unknown error')}"
                            )
                            continue
                        
                        transactions = transactions_data.get('data', {}).get('items', [])
                        
                        if not transactions:
                            logger.info(f"No transactions found for wallet {wallet_address}")
                            # Still mark as completed since API call succeeded
                            state_manager.create_or_update_state(
                                task_name="bronze_transactions",
                                entity_type="whale",
                                entity_id=wallet_address,
                                state="completed",
                                metadata={"transactions_found": 0}
                            )
                            
                            # Update silver_whales table
                            whale.transactions_processed = True
                            whale.transactions_processed_at = datetime.utcnow()
                            session.add(whale)
                            session.commit()
                            
                            processed_wallets += 1
                            continue
                        
                        # Process transactions into DataFrame
                        transaction_records = []
                        for tx in transactions:
                            # Filter by transaction type if configured
                            tx_type = tx.get('txType', '').lower()
                            if config.include_transaction_types and tx_type not in config.include_transaction_types:
                                continue
                            
                            # Filter by minimum transaction value if configured
                            tx_value = 0
                            if tx.get('base') and tx.get('base', {}).get('uiChangeAmount'):
                                base_amount = abs(tx['base']['uiChangeAmount'])
                                base_price = tx['base'].get('nearestPrice', 0)
                                tx_value = base_amount * base_price
                            
                            if config.min_transaction_value_usd > 0 and tx_value < config.min_transaction_value_usd:
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
                                'batch_id': run_id,
                                'data_source': 'birdeye_v3'
                            }
                            transaction_records.append(transaction_record)
                        
                        if transaction_records:
                            # Convert to DataFrame and upsert
                            df = pd.DataFrame(transaction_records)
                            
                            upsert_result = upsert_dataframe(
                                session=session,
                                df=df,
                                table_name="bronze_transactions",
                                schema_name="bronze",
                                primary_keys=["transaction_hash"],
                                model_class=BronzeTransaction
                            )
                            
                            new_transactions += upsert_result.get('inserted', 0)
                            updated_transactions += upsert_result.get('updated', 0)
                            
                            logger.info(f"Stored {len(transaction_records)} transactions for wallet {wallet_address}")
                        
                        # Mark whale as processed
                        state_manager.create_or_update_state(
                            task_name="bronze_transactions",
                            entity_type="whale",
                            entity_id=wallet_address,
                            state="completed",
                            metadata={"transactions_found": len(transaction_records)}
                        )
                        
                        # Update silver_whales table
                        whale.transactions_processed = True
                        whale.transactions_processed_at = datetime.utcnow()
                        session.add(whale)
                        session.commit()
                        
                        processed_wallets += 1
                        
                        # Rate limiting between wallet API calls
                        time.sleep(config.wallet_api_delay)
                        
                    except Exception as e:
                        logger.error(f"Error processing wallet {wallet_address}: {e}")
                        failed_wallets.append(wallet_address)
                        
                        state_manager.create_or_update_state(
                            task_name="bronze_transactions",
                            entity_type="whale",
                            entity_id=wallet_address,
                            state="failed",
                            error_message=str(e)
                        )
                        continue
                
                # Brief pause between batches
                if i + config.batch_size < len(whales_to_process):
                    time.sleep(2)
        
        # Log task completion
        status = "completed" if not failed_wallets else "completed_with_errors"
        
        log_task_completion(
            log_id=task_log.id,
            status=status,
            entities_processed=processed_wallets,
            entities_failed=len(failed_wallets),
            task_metrics={
                "new_transactions": new_transactions,
                "updated_transactions": updated_transactions,
                "total_transactions": new_transactions + updated_transactions
            },
            error_summary={"failed_wallets": failed_wallets} if failed_wallets else None
        )
        
        result = {
            "status": status,
            "processed_wallets": processed_wallets,
            "new_transactions": new_transactions,
            "updated_transactions": updated_transactions,
            "total_transactions": new_transactions + updated_transactions,
            "failed_wallets": failed_wallets
        }
        
        logger.info(f"Bronze transactions task completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Bronze transactions task failed: {e}")
        
        log_task_completion(
            log_id=task_log.id,
            status="failed",
            entities_processed=processed_wallets,
            entities_failed=len(failed_wallets),
            error_summary={"error": str(e)}
        )
        
        raise