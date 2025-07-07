"""
Bronze tokens task - Fetch trending tokens from BirdEye API.
Adapted from Delta Lake implementation to use PostgreSQL and pandas.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from src.services.birdeye_client import BirdEyeAPIClient
from src.models.bronze import BronzeToken
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings, get_bronze_tokens_config

logger = logging.getLogger(__name__)


def process_bronze_tokens(**context) -> Dict[str, Any]:
    """
    Fetch trending tokens from BirdEye API and store in bronze_tokens table.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    settings = get_settings()
    config = get_bronze_tokens_config()
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    # Use current time instead of Airflow execution_date to avoid Proxy issues
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="bronze_tokens",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"config": config.__dict__}
    )
    
    state_manager = StateManager()
    processed_tokens = 0
    new_tokens = 0
    updated_tokens = 0
    failed_tokens = []
    
    try:
        logger.info(f"Starting bronze tokens task with config: TOKEN_LIMIT={config.token_limit}")
        
        # Initialize BirdEye API client
        api_key = settings.get_birdeye_api_key()
        birdeye_client = BirdEyeAPIClient(api_key)
        
        # Build filter parameters
        filter_params = {
            "min_liquidity": config.min_liquidity,
            "max_liquidity": config.max_liquidity,
            "min_volume_1h_usd": config.min_volume_1h_usd,
            "min_price_change_2h_percent": config.min_price_change_2h_percent,
            "min_price_change_24h_percent": config.min_price_change_24h_percent
        }
        
        logger.info(f"Filter parameters: {filter_params}")
        
        # Fetch tokens with pagination
        all_tokens = []
        offset = 0
        api_call_count = 0
        
        while len(all_tokens) < config.token_limit:
            remaining = config.token_limit - len(all_tokens)
            current_limit = min(config.api_pagination_limit, remaining)
            
            params = {
                "sort_by": "liquidity",
                "sort_type": "desc",
                "offset": offset,
                "limit": current_limit,
                **filter_params
            }
            
            api_call_count += 1
            logger.info(f"API call #{api_call_count} - offset: {offset}, limit: {current_limit}")
            
            try:
                response = birdeye_client.get_token_list(**params)
                tokens_data = birdeye_client.normalize_token_list_response(response)
                
                logger.info(f"API call #{api_call_count} - received {len(tokens_data)} tokens")
                
                if not tokens_data:
                    logger.warning(f"No more tokens returned from API at offset {offset}")
                    break
                
                all_tokens.extend(tokens_data)
                offset += len(tokens_data)
                
                # Break if we got fewer tokens than requested (likely end of results)
                if len(tokens_data) < current_limit:
                    logger.info(f"Received fewer tokens than requested, likely end of results")
                    break
                
                # Rate limiting
                import time
                time.sleep(config.api_rate_limit_delay)
                
            except Exception as e:
                logger.error(f"API call #{api_call_count} failed: {e}")
                failed_tokens.append(f"api_call_{api_call_count}")
                break
        
        # Limit to configured maximum
        tokens_to_process = all_tokens[:config.token_limit]
        logger.info(f"Total tokens to process: {len(tokens_to_process)}")
        
        if not tokens_to_process:
            logger.warning("No tokens to process")
            log_task_completion(
                task_log.id, "completed", 0, 0, 0,
                task_metrics={"api_calls": api_call_count}
            )
            return {"status": "no_data", "records": 0}
        
        # Process tokens in batches
        batch_count = 0
        
        for i in range(0, len(tokens_to_process), config.batch_size):
            batch_tokens = tokens_to_process[i:i + config.batch_size]
            batch_count += 1
            
            logger.info(f"Processing batch {batch_count}: {len(batch_tokens)} tokens")
            
            try:
                # Convert to DataFrame
                df = pd.DataFrame(batch_tokens)
                
                # Add metadata columns
                df['ingested_at'] = datetime.utcnow()
                df['silver_processed'] = False
                df['silver_processed_at'] = None
                df['batch_id'] = run_id
                df['processing_date'] = datetime.utcnow().date()
                
                # Get existing tokens to track new vs updated
                with get_db_session() as session:
                    # Check which tokens already exist
                    existing_addresses = []
                    if batch_tokens:
                        existing_query = session.query(BronzeToken.token_address).filter(
                            BronzeToken.token_address.in_([t['token_address'] for t in batch_tokens])
                        )
                        existing_addresses = [addr[0] for addr in existing_query.all()]
                    
                    # Calculate new vs updated
                    batch_new = len([t for t in batch_tokens if t['token_address'] not in existing_addresses])
                    batch_updated = len(batch_tokens) - batch_new
                    
                    new_tokens += batch_new
                    updated_tokens += batch_updated
                    
                    # Perform UPSERT operation
                    upsert_result = upsert_dataframe(
                        session=session,
                        df=df,
                        model_class=BronzeToken,
                        conflict_columns=["token_address"],
                        batch_size=config.batch_size
                    )
                    
                    logger.info(f"Batch {batch_count} UPSERT result: {upsert_result}")
                    
                    # Update state for processed tokens
                    token_addresses = [t['token_address'] for t in batch_tokens]
                    state_manager.mark_entities_completed(
                        task_name="bronze_tokens",
                        entity_type="token",
                        entity_ids=token_addresses,
                        metadata={"batch": batch_count}
                    )
                    
                    processed_tokens += len(batch_tokens)
                    
                    logger.info(f"Batch {batch_count} completed: {len(batch_tokens)} tokens processed")
                    
            except Exception as e:
                logger.error(f"Batch {batch_count} failed: {e}")
                failed_tokens.extend([t['token_address'] for t in batch_tokens])
                
                # Mark failed tokens in state
                token_addresses = [t['token_address'] for t in batch_tokens]
                state_manager.mark_entities_failed(
                    task_name="bronze_tokens",
                    entity_type="token",
                    entity_ids=token_addresses,
                    error_message=str(e),
                    metadata={"batch": batch_count}
                )
                continue
        
        # Task completion
        status = "success" if not failed_tokens else "partial_success"
        
        task_metrics = {
            "api_calls": api_call_count,
            "batches_processed": batch_count,
            "new_tokens": new_tokens,
            "updated_tokens": updated_tokens,
            "filter_params": filter_params
        }
        
        error_summary = None
        if failed_tokens:
            error_summary = {
                "failed_count": len(failed_tokens),
                "failed_tokens": failed_tokens[:10]  # Limit for logging
            }
        
        log_task_completion(
            task_log.id, status, processed_tokens, len(failed_tokens), 0,
            task_metrics=task_metrics, error_summary=error_summary
        )
        
        logger.info(f"Bronze tokens task completed: {processed_tokens} tokens processed "
                   f"({new_tokens} new, {updated_tokens} updated), {len(failed_tokens)} failed")
        
        return {
            "status": status,
            "records": processed_tokens,
            "new_records": new_tokens,
            "updated_records": updated_tokens,
            "failed_records": len(failed_tokens),
            "batches": batch_count,
            "api_calls": api_call_count
        }
        
    except Exception as e:
        logger.error(f"Bronze tokens task failed: {e}")
        
        log_task_completion(
            task_log.id, "failed", processed_tokens, len(failed_tokens), 0,
            error_summary={"error": str(e)}
        )
        
        return {
            "status": "failed",
            "error": str(e),
            "records": processed_tokens
        }


def get_bronze_tokens_statistics() -> Dict[str, Any]:
    """Get statistics about bronze tokens table.
    
    Returns:
        Dictionary with table statistics
    """
    try:
        with get_db_session() as session:
            # Count total tokens
            total_count = session.query(BronzeToken).count()
            
            # Count processed tokens
            processed_count = session.query(BronzeToken).filter(
                BronzeToken.silver_processed == True
            ).count()
            
            # Count by batch
            batch_stats = session.query(
                BronzeToken.batch_id,
                session.query(BronzeToken).filter(
                    BronzeToken.batch_id == BronzeToken.batch_id
                ).count().label('count')
            ).group_by(BronzeToken.batch_id).all()
            
            return {
                "total_tokens": total_count,
                "processed_for_silver": processed_count,
                "pending_for_silver": total_count - processed_count,
                "batch_counts": {batch_id: count for batch_id, count in batch_stats}
            }
            
    except Exception as e:
        logger.error(f"Failed to get bronze tokens statistics: {e}")
        return {"error": str(e)}