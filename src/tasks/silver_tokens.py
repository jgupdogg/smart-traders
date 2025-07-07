"""
Silver tokens task - Select best tokens from bronze layer for tracking.
Uses configurable scoring algorithm to rank and select tokens.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_
from src.models.bronze import BronzeToken
from src.models.silver import SilverToken
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
# Removed config import - using simplified approach

logger = logging.getLogger(__name__)


def process_silver_tokens(**context) -> Dict[str, Any]:
    """
    Select the best tokens from bronze layer for tracking in silver layer.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    # No config needed for simplified approach
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    # Use current time instead of Airflow execution_date to avoid Proxy issues
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="silver_tokens",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"approach": "simplified_all_tokens"}
    )
    
    state_manager = StateManager()
    processed_tokens = 0
    selected_tokens = 0
    failed_tokens = []
    
    try:
        logger.info("Starting silver tokens selection - processing all bronze tokens")
        
        with get_db_session() as session:
            # Get unprocessed bronze tokens
            bronze_query = session.query(BronzeToken).filter(
                BronzeToken.silver_processed == False
            )
            
            # Convert to DataFrame for pandas operations
            bronze_tokens_data = []
            for token in bronze_query.all():
                bronze_tokens_data.append({
                    'token_address': token.token_address,
                    'symbol': token.symbol,
                    'name': token.name,
                    'market_cap': token.market_cap,
                    'liquidity': token.liquidity,
                    'volume_24h_usd': token.volume_24h_usd,
                    'price_change_24h_percent': token.price_change_24h_percent,
                    'holder': token.holder,
                    'trade_24h_count': token.trade_24h_count,
                    'price': token.price,
                    'ingested_at': token.ingested_at
                })
            
            if not bronze_tokens_data:
                logger.info("No unprocessed bronze tokens found")
                log_task_completion(
                    task_log.id, "completed", 0, 0, 0,
                    task_metrics={"reason": "no_unprocessed_tokens"}
                )
                return {"status": "no_data", "records": 0}
            
            logger.info(f"Found {len(bronze_tokens_data)} unprocessed bronze tokens")
            
            # Convert to DataFrame
            df = pd.DataFrame(bronze_tokens_data)
            
            # Simple approach: process all bronze tokens without filtering
            logger.info("Processing all bronze tokens without filtering...")
            
            # Use all tokens as selected
            selected_df = df
            
            logger.info(f"Selected {len(selected_df)} tokens (all unprocessed bronze tokens)")
            
            # Prepare silver tokens data with simple structure
            silver_tokens_data = []
            for _, token in selected_df.iterrows():
                silver_token = {
                    'token_address': token['token_address'],
                    'selection_score': 1.0,  # Simple default score
                    'selection_reason': {
                        'selection_criteria': 'all_bronze_tokens',
                        'rank': len(silver_tokens_data) + 1
                    },
                    'selected_at': datetime.utcnow(),
                    'whales_processed': False,
                    'whales_processed_at': None,
                    # Denormalized data
                    'symbol': token['symbol'],
                    'name': token['name'],
                    'market_cap': token['market_cap'],
                    'price_change_24h_percent': token['price_change_24h_percent'],
                    'volume_24h_usd': token['volume_24h_usd'],
                    'liquidity': token['liquidity'],
                    'holder_count': token['holder'],
                    'price': token['price']
                }
                silver_tokens_data.append(silver_token)
            
            # Insert selected tokens into silver table
            if silver_tokens_data:
                silver_df = pd.DataFrame(silver_tokens_data)
                
                upsert_result = upsert_dataframe(
                    session=session,
                    df=silver_df,
                    model_class=SilverToken,
                    conflict_columns=["token_address"],
                    batch_size=50
                )
                
                logger.info(f"Silver tokens UPSERT result: {upsert_result}")
                selected_tokens = len(silver_tokens_data)
                
                # Update state for selected tokens
                selected_addresses = [t['token_address'] for t in silver_tokens_data]
                state_manager.mark_entities_completed(
                    task_name="silver_tokens",
                    entity_type="token",
                    entity_ids=selected_addresses,
                    metadata={"selection_method": "all_bronze_tokens"}
                )
            
            # Mark ALL bronze tokens as processed (selected and non-selected)
            all_token_addresses = [t['token_address'] for t in bronze_tokens_data]
            _mark_bronze_tokens_processed(session, all_token_addresses)
            processed_tokens = len(all_token_addresses)
            
            logger.info(f"Marked {processed_tokens} bronze tokens as processed")
        
        # Task completion
        task_metrics = {
            "tokens_evaluated": len(bronze_tokens_data),
            "tokens_selected": selected_tokens,
            "selection_method": "all_bronze_tokens"
        }
        
        log_task_completion(
            task_log.id, "success", processed_tokens, 0, 0,
            task_metrics=task_metrics
        )
        
        logger.info(f"Silver tokens task completed: {selected_tokens} tokens selected from {processed_tokens} evaluated")
        
        return {
            "status": "success",
            "records": processed_tokens,
            "selected_tokens": selected_tokens,
            "evaluated_tokens": len(bronze_tokens_data)
        }
        
    except Exception as e:
        logger.error(f"Silver tokens task failed: {e}")
        
        log_task_completion(
            task_log.id, "failed", processed_tokens, 0, 0,
            error_summary={"error": str(e)}
        )
        
        return {
            "status": "failed",
            "error": str(e),
            "records": processed_tokens
        }


# Removed scoring functions - no longer needed for simplified approach


def _mark_bronze_tokens_processed(session, token_addresses: List[str]):
    """
    Mark bronze tokens as processed for silver layer.
    
    Args:
        session: Database session
        token_addresses: List of token addresses to mark as processed
    """
    try:
        # Update bronze tokens to mark as processed
        session.query(BronzeToken).filter(
            BronzeToken.token_address.in_(token_addresses)
        ).update({
            BronzeToken.silver_processed: True,
            BronzeToken.silver_processed_at: datetime.utcnow()
        }, synchronize_session=False)
        
        session.commit()
        
        logger.info(f"Marked {len(token_addresses)} bronze tokens as silver_processed=True")
        
    except Exception as e:
        logger.error(f"Failed to mark bronze tokens as processed: {e}")
        session.rollback()
        raise


def get_silver_tokens_statistics() -> Dict[str, Any]:
    """Get statistics about silver tokens selection.
    
    Returns:
        Dictionary with selection statistics
    """
    try:
        with get_db_session() as session:
            # Count silver tokens
            total_silver = session.query(SilverToken).count()
            
            # Count processed for whales
            processed_for_whales = session.query(SilverToken).filter(
                SilverToken.whales_processed == True
            ).count()
            
            # Get score statistics
            scores = [s[0] for s in session.query(SilverToken.selection_score).all() if s[0] is not None]
            
            score_stats = {}
            if scores:
                score_stats = {
                    "min_score": min(scores),
                    "max_score": max(scores),
                    "avg_score": sum(scores) / len(scores)
                }
            
            return {
                "total_silver_tokens": total_silver,
                "processed_for_whales": processed_for_whales,
                "pending_for_whales": total_silver - processed_for_whales,
                "score_statistics": score_stats
            }
            
    except Exception as e:
        logger.error(f"Failed to get silver tokens statistics: {e}")
        return {"error": str(e)}