"""
Silver whales task - Process unique whale addresses from bronze_whales table.
Identifies unique whale addresses across all tokens and stores them in silver_whales table.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_, select, func
from src.models.bronze import BronzeWhale
from src.models.silver import SilverWhale
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings, get_silver_whales_config

logger = logging.getLogger(__name__)


def process_silver_whales(**context) -> Dict[str, Any]:
    """
    Process unique whale addresses from bronze_whales table.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    settings = get_settings()
    config = get_silver_whales_config()
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="silver_whales",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"config": config.__dict__}
    )
    
    state_manager = StateManager()
    processed_whales = 0
    new_whales = 0
    updated_whales = 0
    failed_whales = []
    
    try:
        with get_db_session() as session:
            # Get ALL unique whale addresses from bronze_whales (no filters)
            unique_whales_query = select(
                BronzeWhale.wallet_address,
                func.count(BronzeWhale.token_address).label('tokens_held_count'),
                func.array_agg(BronzeWhale.token_address).label('token_addresses'),
                func.array_agg(BronzeWhale.token_symbol).label('token_symbols'),
                func.sum(BronzeWhale.ui_amount).label('total_tokens_held')
            ).group_by(
                BronzeWhale.wallet_address
            )
            
            unique_whales_result = session.exec(unique_whales_query).all()
            
            if not unique_whales_result:
                logger.info("No unique whales found meeting criteria")
                return {
                    "status": "completed",
                    "processed_whales": 0,
                    "new_whales": 0,
                    "message": "No whales to process"
                }
            
            logger.info(f"Found {len(unique_whales_result)} unique whale addresses")
            
            # Process whale addresses
            whale_records = []
            for whale_data in unique_whales_result:
                wallet_address = whale_data.wallet_address
                tokens_held_count = whale_data.tokens_held_count
                token_addresses = whale_data.token_addresses
                token_symbols = whale_data.token_symbols
                
                try:
                    # Process ALL whales without exclusions for now
                    
                    # Create state for this whale
                    state_manager.create_or_update_state(
                        task_name="silver_whales",
                        entity_type="whale",
                        entity_id=wallet_address,
                        state="processing",
                        metadata={"tokens_held": tokens_held_count}
                    )
                    
                    # Get detailed whale data from bronze_whales for this address
                    whale_details_query = select(BronzeWhale).where(
                        BronzeWhale.wallet_address == wallet_address
                    )
                    whale_details = session.exec(whale_details_query).all()
                    
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
                    
                    # Create whale record
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
                    whale_records.append(whale_record)
                    
                    # Mark whale as completed
                    state_manager.create_or_update_state(
                        task_name="silver_whales",
                        entity_type="whale",
                        entity_id=wallet_address,
                        state="completed",
                        metadata={"tokens_held": tokens_held_count, "total_value": total_value_held}
                    )
                    
                    processed_whales += 1
                    
                    # No limits - process all whales
                    
                except Exception as e:
                    logger.error(f"Error processing whale {wallet_address}: {e}")
                    failed_whales.append(wallet_address)
                    
                    state_manager.create_or_update_state(
                        task_name="silver_whales",
                        entity_type="whale",
                        entity_id=wallet_address,
                        state="failed",
                        error_message=str(e)
                    )
                    continue
            
            # Store whale records if any were processed
            if whale_records:
                df = pd.DataFrame(whale_records)
                
                upsert_result = upsert_dataframe(
                    session=session,
                    df=df,
                    table_name="silver_whales",
                    schema_name="silver",
                    primary_keys=["wallet_address"],
                    model_class=SilverWhale
                )
                
                new_whales += upsert_result.get('inserted', 0)
                updated_whales += upsert_result.get('updated', 0)
                
                logger.info(f"Stored {len(whale_records)} whale records")
        
        # Log task completion
        status = "completed" if not failed_whales else "partial_success"
        
        log_task_completion(
            log_id=task_log.id,
            status=status,
            entities_processed=processed_whales,
            entities_failed=len(failed_whales),
            task_metrics={
                "new_whales": new_whales,
                "updated_whales": updated_whales,
                "total_whales": new_whales + updated_whales
            },
            error_summary={"failed_whales": failed_whales} if failed_whales else None
        )
        
        result = {
            "status": status,
            "processed_whales": processed_whales,
            "new_whales": new_whales,
            "updated_whales": updated_whales,
            "total_whales": new_whales + updated_whales,
            "failed_whales": failed_whales
        }
        
        logger.info(f"Silver whales task completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Silver whales task failed: {e}")
        
        log_task_completion(
            log_id=task_log.id,
            status="failed",
            entities_processed=processed_whales,
            entities_failed=len(failed_whales),
            error_summary={"error": str(e)}
        )
        
        raise