"""
Smart traders task - Rank traders based on performance metrics and assign tiers.
Final step in the pipeline that identifies and ranks the most profitable traders.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import and_, select, func
from src.models.silver import SilverWalletPnL
from src.models.gold import SmartTrader
from src.database.connection import get_db_session
from src.database.operations import upsert_dataframe
from src.database.state_manager import StateManager, log_task_start, log_task_completion
from src.config.settings import get_settings, get_smart_traders_config

logger = logging.getLogger(__name__)


def calculate_performance_tier(
    score: float,
    total_pnl: float,
    win_rate: float,
    trade_count: int,
    config: Any
) -> str:
    """
    Assign performance tier based on composite metrics.
    
    Args:
        score: Composite smart trader score
        total_pnl: Total realized PnL
        win_rate: Win rate percentage
        trade_count: Number of trades
        config: Smart traders configuration
        
    Returns:
        Performance tier string
    """
    # Define tier thresholds
    elite_threshold = config.performance_tiers.get('elite', {})
    strong_threshold = config.performance_tiers.get('strong', {})
    promising_threshold = config.performance_tiers.get('promising', {})
    
    # ELITE tier - exceptional performance
    if (score >= elite_threshold.get('min_score', 0.8) and
        total_pnl >= elite_threshold.get('min_pnl', 50000) and
        win_rate >= elite_threshold.get('min_win_rate', 70) and
        trade_count >= elite_threshold.get('min_trades', 50)):
        return "ELITE"
    
    # STRONG tier - strong consistent performance
    elif (score >= strong_threshold.get('min_score', 0.6) and
          total_pnl >= strong_threshold.get('min_pnl', 20000) and
          win_rate >= strong_threshold.get('min_win_rate', 60) and
          trade_count >= strong_threshold.get('min_trades', 25)):
        return "STRONG"
    
    # PROMISING tier - good emerging performance
    elif (score >= promising_threshold.get('min_score', 0.4) and
          total_pnl >= promising_threshold.get('min_pnl', 5000) and
          win_rate >= promising_threshold.get('min_win_rate', 55) and
          trade_count >= promising_threshold.get('min_trades', 10)):
        return "PROMISING"
    
    # QUALIFIED tier - meets basic criteria
    else:
        return "QUALIFIED"


def process_smart_traders(**context) -> Dict[str, Any]:
    """
    Rank traders based on performance metrics and assign performance tiers.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        Dictionary with task execution results
    """
    settings = get_settings()
    config = get_smart_traders_config()
    
    # Start task execution logging
    run_id = context.get('run_id', datetime.now().strftime("%Y%m%d_%H%M%S"))
    execution_date = datetime.utcnow()
    
    task_log = log_task_start(
        task_name="smart_traders",
        run_id=run_id,
        execution_date=execution_date,
        metadata={"config": config.__dict__}
    )
    
    state_manager = StateManager()
    processed_traders = 0
    new_traders = 0
    updated_traders = 0
    failed_traders = []
    
    try:
        with get_db_session() as session:
            # Get all wallets that are eligible as smart traders
            eligible_wallets_query = select(SilverWalletPnL).where(
                SilverWalletPnL.smart_trader_eligible == True
            ).order_by(
                SilverWalletPnL.smart_trader_score.desc(),
                SilverWalletPnL.total_realized_pnl_usd.desc()
            )
            
            eligible_wallets = session.exec(eligible_wallets_query).all()
            
            if not eligible_wallets:
                logger.info("No wallets are eligible as smart traders")
                return {
                    "status": "completed",
                    "processed_traders": 0,
                    "new_traders": 0,
                    "message": "No eligible traders found"
                }
            
            logger.info(f"Processing {len(eligible_wallets)} eligible smart traders")
            
            # Calculate percentile rankings for tier assignment
            scores = [w.smart_trader_score for w in eligible_wallets if w.smart_trader_score is not None]
            pnl_values = [w.total_realized_pnl_usd for w in eligible_wallets if w.total_realized_pnl_usd is not None]
            
            if not scores or not pnl_values:
                logger.warning("No valid scores or PnL values found")
                return {
                    "status": "completed",
                    "processed_traders": 0,
                    "message": "No valid trader data"
                }
            
            # Calculate percentile thresholds
            score_95th = pd.Series(scores).quantile(0.95)
            score_80th = pd.Series(scores).quantile(0.80)
            score_60th = pd.Series(scores).quantile(0.60)
            
            pnl_95th = pd.Series(pnl_values).quantile(0.95)
            pnl_80th = pd.Series(pnl_values).quantile(0.80)
            pnl_60th = pd.Series(pnl_values).quantile(0.60)
            
            logger.info(f"Score percentiles - 95th: {score_95th:.3f}, 80th: {score_80th:.3f}, 60th: {score_60th:.3f}")
            logger.info(f"PnL percentiles - 95th: ${pnl_95th:.0f}, 80th: ${pnl_80th:.0f}, 60th: ${pnl_60th:.0f}")
            
            # Process eligible traders
            trader_records = []
            for rank, wallet in enumerate(eligible_wallets, 1):
                wallet_address = wallet.wallet_address
                
                try:
                    # Create/update state for this trader
                    state_manager.create_or_update_state(
                        task_name="smart_traders",
                        entity_type="trader",
                        entity_id=wallet_address,
                        state="processing",
                        metadata={"pnl": wallet.total_realized_pnl_usd, "score": wallet.smart_trader_score}
                    )
                    
                    # Assign performance tier
                    performance_tier = calculate_performance_tier(
                        score=wallet.smart_trader_score or 0,
                        total_pnl=wallet.total_realized_pnl_usd or 0,
                        win_rate=wallet.win_rate_percent or 0,
                        trade_count=wallet.total_trades or 0,
                        config=config
                    )
                    
                    # Calculate additional ranking metrics
                    roi_percent = 0
                    if wallet.total_volume_usd and wallet.total_volume_usd > 0:
                        roi_percent = (wallet.total_realized_pnl_usd or 0) / wallet.total_volume_usd * 100
                    
                    # Risk-adjusted return (simplified Sharpe ratio)
                    risk_adjusted_return = wallet.sharpe_ratio or 0
                    
                    # Consistency score based on win rate and trade frequency
                    consistency_score = 0
                    if wallet.win_rate_percent and wallet.trade_frequency_per_day:
                        consistency_score = (wallet.win_rate_percent / 100) * min(wallet.trade_frequency_per_day / 2, 1.0)
                    
                    # Create trader record
                    trader_record = {
                        'wallet_address': wallet_address,
                        'performance_tier': performance_tier,
                        'overall_rank': rank,
                        'tier_rank': 0,  # Will be calculated after grouping by tier
                        'composite_score': wallet.smart_trader_score or 0,
                        'total_realized_pnl_usd': wallet.total_realized_pnl_usd or 0,
                        'total_unrealized_pnl_usd': wallet.total_unrealized_pnl_usd or 0,
                        'win_rate_percent': wallet.win_rate_percent or 0,
                        'total_trades': wallet.total_trades or 0,
                        'avg_trade_size_usd': wallet.avg_trade_size_usd or 0,
                        'total_volume_usd': wallet.total_volume_usd or 0,
                        'trade_frequency_per_day': wallet.trade_frequency_per_day or 0,
                        'max_drawdown_percent': wallet.max_drawdown_percent or 0,
                        'sharpe_ratio': wallet.sharpe_ratio or 0,
                        'roi_percent': roi_percent,
                        'risk_adjusted_return': risk_adjusted_return,
                        'consistency_score': consistency_score,
                        'first_trade_at': wallet.first_trade_at,
                        'last_trade_at': wallet.last_trade_at,
                        'trading_days': wallet.trading_days or 0,
                        'tokens_traded_count': wallet.tokens_traded_count or 0,
                        'identified_at': datetime.utcnow(),
                        'last_updated_at': datetime.utcnow(),
                        'data_source': 'silver_wallet_pnl',
                        'analysis_period_days': (datetime.utcnow() - (wallet.first_trade_at or datetime.utcnow())).days,
                        'performance_metadata': {
                            'score_percentile': len([s for s in scores if s < (wallet.smart_trader_score or 0)]) / len(scores) * 100,
                            'pnl_percentile': len([p for p in pnl_values if p < (wallet.total_realized_pnl_usd or 0)]) / len(pnl_values) * 100,
                            'tier_criteria_met': {
                                'min_score': wallet.smart_trader_score or 0,
                                'min_pnl': wallet.total_realized_pnl_usd or 0,
                                'min_win_rate': wallet.win_rate_percent or 0,
                                'min_trades': wallet.total_trades or 0
                            }
                        }
                    }
                    
                    trader_records.append(trader_record)
                    
                    # Mark trader as processed
                    state_manager.create_or_update_state(
                        task_name="smart_traders",
                        entity_type="trader",
                        entity_id=wallet_address,
                        state="completed",
                        metadata={
                            "tier": performance_tier,
                            "rank": rank,
                            "score": wallet.smart_trader_score
                        }
                    )
                    
                    processed_traders += 1
                    
                    logger.info(f"Processed trader {wallet_address}: {performance_tier} tier, rank {rank}")
                    
                except Exception as e:
                    logger.error(f"Error processing trader {wallet_address}: {e}")
                    failed_traders.append(wallet_address)
                    
                    state_manager.create_or_update_state(
                        task_name="smart_traders",
                        entity_type="trader",
                        entity_id=wallet_address,
                        state="failed",
                        error_message=str(e)
                    )
                    continue
            
            # Calculate tier ranks
            if trader_records:
                df = pd.DataFrame(trader_records)
                
                # Calculate tier-specific rankings
                for tier in df['performance_tier'].unique():
                    tier_mask = df['performance_tier'] == tier
                    tier_df = df[tier_mask].sort_values('composite_score', ascending=False)
                    df.loc[tier_mask, 'tier_rank'] = range(1, len(tier_df) + 1)
                
                # Update trader records with tier ranks
                trader_records = df.to_dict('records')
                
                # Store trader records
                upsert_result = upsert_dataframe(
                    session=session,
                    df=df,
                    table_name="smart_traders",
                    schema_name="gold",
                    primary_keys=["wallet_address"],
                    model_class=SmartTrader
                )
                
                new_traders += upsert_result.get('inserted', 0)
                updated_traders += upsert_result.get('updated', 0)
                
                # Log tier distribution
                tier_counts = df['performance_tier'].value_counts().to_dict()
                logger.info(f"Tier distribution: {tier_counts}")
                logger.info(f"Stored {len(trader_records)} smart trader records")
        
        # Log task completion
        status = "completed" if not failed_traders else "completed_with_errors"
        
        log_task_completion(
            log_id=task_log.id,
            status=status,
            entities_processed=processed_traders,
            entities_failed=len(failed_traders),
            task_metrics={
                "new_traders": new_traders,
                "updated_traders": updated_traders,
                "total_traders": new_traders + updated_traders,
                "tier_distribution": tier_counts if 'tier_counts' in locals() else {}
            },
            error_summary={"failed_traders": failed_traders} if failed_traders else None
        )
        
        result = {
            "status": status,
            "processed_traders": processed_traders,
            "new_traders": new_traders,
            "updated_traders": updated_traders,
            "total_traders": new_traders + updated_traders,
            "tier_distribution": tier_counts if 'tier_counts' in locals() else {},
            "failed_traders": failed_traders
        }
        
        logger.info(f"Smart traders task completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Smart traders task failed: {e}")
        
        log_task_completion(
            log_id=task_log.id,
            status="failed",
            entities_processed=processed_traders,
            entities_failed=len(failed_traders),
            error_summary={"error": str(e)}
        )
        
        raise