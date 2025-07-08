"""
Bronze whales task - Fetch top holders for selected tokens from BirdEye API.
Stores whale/holder data for each token in bronze_whales table.
"""

import logging
import pandas as pd
import time
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_, select, text
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
        api_key = settings.get_birdeye_api_key()
        birdeye_client = BirdEyeAPIClient(api_key)
        
        with get_db_session() as session:
            # Debug: Check table structure first
            try:
                result = session.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'silver_tokens' AND table_schema = 'silver'"))
                columns = [row[0] for row in result]
                logger.info(f"silver_tokens columns: {columns}")
            except Exception as e:
                logger.warning(f"Could not check table structure: {e}")
            
            # Get tokens that need whale processing - try raw SQL first
            try:
                raw_query = text("""
                    SELECT * FROM silver.silver_tokens 
                    WHERE whales_processed = false 
                    LIMIT :limit
                """)
                result = session.execute(raw_query, {"limit": config.max_tokens_per_run})
                tokens_data = result.fetchall()
                
                logger.info(f"Found {len(tokens_data)} tokens via raw SQL")
                if tokens_data:
                    logger.info(f"First token columns: {list(tokens_data[0]._mapping.keys())}")
                    
                tokens_to_process = []
                for row in tokens_data:
                    # Create a simple dict with the data we need
                    token_dict = {
                        'token_address': row._mapping.get('token_address'),
                        'symbol': row._mapping.get('symbol'),
                        'name': row._mapping.get('name'),
                        'whales_processed': row._mapping.get('whales_processed'),
                        '_row': row  # Keep original row for updates
                    }
                    tokens_to_process.append(token_dict)
                    
            except Exception as e:
                logger.error(f"Raw SQL failed: {e}")
                # Fallback to SQLModel query
                tokens_query = select(SilverToken).where(
                    SilverToken.whales_processed == False
                ).limit(config.max_tokens_per_run)
                
                tokens_to_process = list(session.exec(tokens_query))
            
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
                    # Handle both dict format (from raw SQL) and SQLModel format
                    if isinstance(token, dict):
                        token_address = token['token_address']
                        token_symbol = token['symbol'] or 'unknown'
                        token_name = token['name'] or 'unknown'
                        original_token = token.get('_row')  # For updates
                    else:
                        # SQLModel object
                        token_address = token.token_address
                        token_symbol = token.symbol or 'unknown'
                        token_name = token.name or 'unknown'
                        original_token = token
                    
                    logger.info(f"Processing token {token_symbol} ({token_address})")
                    
                    try:
                        # Create/update state for this token
                        state_manager.create_or_update_state(
                            task_name="bronze_whales",
                            entity_type="token",
                            entity_id=token_address,
                            state="processing",
                            metadata={"token_symbol": token_symbol}
                        )
                        
                        # Fetch whale data from BirdEye API
                        logger.info(f"Fetching whale data for token {token_symbol} ({token_address})")
                        
                        whale_response = birdeye_client.get_token_holders(
                            token_address=token_address,
                            limit=config.top_holders_limit
                        )
                        
                        if not whale_response.get('success', False):
                            logger.warning(f"API failed for token {token_address}: {whale_response}")
                            failed_tokens.append(token_address)
                            
                            state_manager.create_or_update_state(
                                task_name="bronze_whales",
                                entity_type="token",
                                entity_id=token_address,
                                state="failed",
                                error_message=f"API failed: {whale_response.get('msg', 'Unknown error')}"
                            )
                            continue
                        
                        # Normalize the response using the client's method
                        holders = birdeye_client.normalize_holders_response(whale_response)
                        
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
                            if isinstance(token, dict):
                                # For raw SQL, need to update via raw SQL
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
                            else:
                                # SQLModel object
                                original_token.whales_processed = True
                                original_token.whales_processed_at = datetime.utcnow()
                                session.add(original_token)
                                session.commit()
                            
                            processed_tokens += 1
                            continue
                        
                        # Process holders into DataFrame
                        whale_records = []
                        for holder in holders:
                            # The normalized response already has rank, wallet_address, etc.
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
                                'batch_id': run_id,
                                'data_source': 'birdeye_v3'
                            }
                            whale_records.append(whale_record)
                        
                        if whale_records:
                            # Convert to DataFrame
                            df = pd.DataFrame(whale_records)
                            
                            # Deduplicate by token_address + wallet_address, keeping the best rank (first occurrence)
                            df_grouped = df.drop_duplicates(subset=['token_address', 'wallet_address'], keep='first')
                            
                            if len(df) != len(df_grouped):
                                logger.info(f"Merged {len(df)} holdings into {len(df_grouped)} unique wallet positions for token {token_symbol}")
                            
                            upsert_result = upsert_dataframe(
                                session=session,
                                df=df_grouped,
                                model_class=BronzeWhale,
                                conflict_columns=["token_address", "wallet_address"],
                                batch_size=config.batch_size
                            )
                            
                            new_whales += upsert_result.get('inserted', 0)
                            updated_whales += upsert_result.get('updated', 0)
                            
                            logger.info(f"Stored {len(whale_records)} whale records for token {token_symbol}")
                        
                        # Mark token as processed
                        state_manager.create_or_update_state(
                            task_name="bronze_whales",
                            entity_type="token",
                            entity_id=token_address,
                            state="completed",
                            metadata={"holders_found": len(whale_records)}
                        )
                        
                        # Update silver_tokens table
                        if isinstance(token, dict):
                            # For raw SQL, need to update via raw SQL
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
                        else:
                            # SQLModel object
                            original_token.whales_processed = True
                            original_token.whales_processed_at = datetime.utcnow()
                            session.add(original_token)
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
        status = "completed" if not failed_tokens else "partial_success"
        
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