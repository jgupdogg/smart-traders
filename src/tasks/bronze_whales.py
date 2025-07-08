"""
Bronze whales task - Fetch top holders for selected tokens from BirdEye API.
Stores whale/holder data for each token in bronze_whales table.
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_, select
from src.services.birdeye_client import BirdEyeAPIClient
from src.models.bronze import BronzeWhale
from src.models.silver import SilverToken
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings, get_bronze_whales_config

logger = logging.getLogger(__name__)


def process_bronze_whales(**context) -> Dict[str, Any]:
    """
    Fetch top holders for each selected token from silver_tokens table.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    settings = get_settings()
    config = get_bronze_whales_config()
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="bronze_whales",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"config": config.__dict__}
    )
    
    state_manager = StateManager()
    processed_tokens = 0
    new_whales = 0
    updated_whales = 0
    failed_tokens = []
    
    try:
        # Initialize BirdEye client
        client = BirdEyeAPIClient(api_key=settings.birdeye_api_key)
        
        with get_db_session() as session:
            # Get tokens that need whale processing
            tokens_query = select(SilverToken).where(
                SilverToken.whales_processed == False
            ).limit(config.max_tokens_per_run)
            
            tokens_to_process = session.exec(tokens_query).all()
            
            if not tokens_to_process:
                logger.info("No tokens need whale processing")
                return {
                    "status": "completed",
                    "processed_tokens": 0,
                    "new_whales": 0,
                    "message": "No tokens to process"
                }
            
            logger.info(f"Processing {len(tokens_to_process)} tokens for whale data")
            
            # Process tokens in batches
            for i in range(0, len(tokens_to_process), config.batch_size):
                batch = tokens_to_process[i:i + config.batch_size]
                
                for token in batch:
                    token_address = token.token_address
                    
                    try:
                        # Create/update state for this token
                        state_manager.create_or_update_state(
                            task_name="bronze_whales",
                            entity_type="token",
                            entity_id=token_address,
                            state="processing",
                            metadata={"token_symbol": token.symbol}
                        )
                        
                        # Fetch whale data from BirdEye API
                        logger.info(f"Fetching whale data for token {token.symbol} ({token_address})")
                        
                        whale_data = client.get_token_holders(
                            token_address=token_address,
                            limit=config.top_holders_limit
                        )
                        
                        if not whale_data.get('success', False):
                            logger.warning(f"API failed for token {token_address}: {whale_data}")
                            failed_tokens.append(token_address)
                            
                            state_manager.create_or_update_state(
                                task_name="bronze_whales",
                                entity_type="token",
                                entity_id=token_address,
                                state="failed",
                                error_message=f"API failed: {whale_data.get('msg', 'Unknown error')}"
                            )
                            continue
                        
                        holders = whale_data.get('data', {}).get('items', [])
                        
                        if not holders:
                            logger.warning(f"No holder data found for token {token_address}")
                            # Still mark as completed since API call succeeded
                            state_manager.create_or_update_state(
                                task_name="bronze_whales",
                                entity_type="token",
                                entity_id=token_address,
                                state="completed",
                                metadata={"holders_found": 0}
                            )
                            
                            # Update silver_tokens table
                            token.whales_processed = True
                            token.whales_processed_at = datetime.utcnow()
                            session.add(token)
                            session.commit()
                            
                            processed_tokens += 1
                            continue
                        
                        # Process holders into DataFrame
                        whale_records = []
                        for rank, holder in enumerate(holders, 1):
                            # Filter by minimum holding percentage if configured
                            if config.min_holding_percentage > 0:
                                holding_percentage = holder.get('percentage', 0)
                                if holding_percentage < config.min_holding_percentage:
                                    continue
                            
                            whale_record = {
                                'token_address': token_address,
                                'wallet_address': holder.get('address', ''),
                                'token_symbol': token.symbol,
                                'token_name': token.name,
                                'rank': rank,
                                'amount': holder.get('amount', ''),
                                'ui_amount': holder.get('uiAmount', 0),
                                'decimals': holder.get('decimals', 0),
                                'mint': holder.get('mint', ''),
                                'token_account': holder.get('tokenAccount', ''),
                                'fetched_at': datetime.utcnow(),
                                'batch_id': run_id,
                                'data_source': 'birdeye_v3'
                            }
                            whale_records.append(whale_record)
                        
                        if whale_records:
                            # Convert to DataFrame and upsert
                            df = pd.DataFrame(whale_records)
                            
                            upsert_result = upsert_dataframe(
                                session=session,
                                df=df,
                                table_name="bronze_whales",
                                schema_name="bronze",
                                primary_keys=["token_address", "wallet_address"],
                                model_class=BronzeWhale
                            )
                            
                            new_whales += upsert_result.get('inserted', 0)
                            updated_whales += upsert_result.get('updated', 0)
                            
                            logger.info(f"Stored {len(whale_records)} whale records for token {token.symbol}")
                        
                        # Mark token as processed
                        state_manager.create_or_update_state(
                            task_name="bronze_whales",
                            entity_type="token",
                            entity_id=token_address,
                            state="completed",
                            metadata={"holders_found": len(whale_records)}
                        )
                        
                        # Update silver_tokens table
                        token.whales_processed = True
                        token.whales_processed_at = datetime.utcnow()
                        session.add(token)
                        session.commit()
                        
                        processed_tokens += 1
                        
                        # Rate limiting
                        time.sleep(config.api_rate_limit_delay)
                        
                    except Exception as e:
                        logger.error(f"Error processing token {token_address}: {e}")
                        failed_tokens.append(token_address)
                        
                        state_manager.create_or_update_state(
                            task_name="bronze_whales",
                            entity_type="token",
                            entity_id=token_address,
                            state="failed",
                            error_message=str(e)
                        )
                        continue
                
                # Brief pause between batches
                if i + config.batch_size < len(tokens_to_process):
                    time.sleep(1)
        
        # Log task completion
        status = "completed" if not failed_tokens else "completed_with_errors"
        
        log_task_completion(
            log_id=task_log.id,
            status=status,
            entities_processed=processed_tokens,
            entities_failed=len(failed_tokens),
            task_metrics={
                "new_whales": new_whales,
                "updated_whales": updated_whales,
                "total_whales": new_whales + updated_whales
            },
            error_summary={"failed_tokens": failed_tokens} if failed_tokens else None
        )
        
        result = {
            "status": status,
            "processed_tokens": processed_tokens,
            "new_whales": new_whales,
            "updated_whales": updated_whales,
            "total_whales": new_whales + updated_whales,
            "failed_tokens": failed_tokens
        }
        
        logger.info(f"Bronze whales task completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Bronze whales task failed: {e}")
        
        log_task_completion(
            log_id=task_log.id,
            status="failed",
            entities_processed=processed_tokens,
            entities_failed=len(failed_tokens),
            error_summary={"error": str(e)}
        )
        
        raise