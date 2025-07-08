"""
Smart traders task - Rank traders based on performance metrics and assign tiers.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy import select
from src.models.silver import SilverWalletPnL
from src.models.gold import SmartTrader
from src.database.connection import get_db_session
from src.config.settings import get_smart_traders_config
from smart_trader_tasks.common import GoldTaskBase

logger = logging.getLogger(__name__)


class SmartTradersTask(GoldTaskBase):
    """Smart traders task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("smart_traders", context)
        self.config = get_smart_traders_config()
        
    def get_eligible_wallets(self, session: Any) -> List[Any]:
        """Get all wallets that are eligible as smart traders."""
        eligible_wallets_query = select(SilverWalletPnL).where(
            SilverWalletPnL.smart_trader_eligible == True
        ).order_by(
            SilverWalletPnL.smart_trader_score.desc(),
            SilverWalletPnL.total_realized_pnl_usd.desc()
        )
        
        return list(session.exec(eligible_wallets_query))
        
    def calculate_performance_tier(
        self,
        score: float,
        total_pnl: float,
        win_rate: float,
        trade_count: int
    ) -> str:
        """Assign performance tier based on composite metrics."""
        # Define tier thresholds
        elite_threshold = self.config.performance_tiers.get('elite', {})
        strong_threshold = self.config.performance_tiers.get('strong', {})
        promising_threshold = self.config.performance_tiers.get('promising', {})
        
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
            
    def create_trader_record(
        self, 
        wallet: Any, 
        rank: int, 
        scores: List[float], 
        pnl_values: List[float]
    ) -> Dict[str, Any]:
        """Create a trader record from wallet data."""
        wallet_address = wallet.wallet_address
        
        # Assign performance tier
        performance_tier = self.calculate_performance_tier(
            score=wallet.smart_trader_score or 0,
            total_pnl=wallet.total_realized_pnl_usd or 0,
            win_rate=wallet.win_rate_percent or 0,
            trade_count=wallet.total_trades or 0
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
        
        return trader_record
        
    def process_trader(
        self, 
        wallet: Any, 
        rank: int, 
        scores: List[float], 
        pnl_values: List[float]
    ) -> Dict[str, Any]:
        """Process a single trader."""
        wallet_address = wallet.wallet_address
        
        try:
            # Create/update state for this trader
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="trader",
                entity_id=wallet_address,
                state="processing",
                metadata={"pnl": wallet.total_realized_pnl_usd, "score": wallet.smart_trader_score}
            )
            
            # Create trader record
            trader_record = self.create_trader_record(wallet, rank, scores, pnl_values)
            
            # Mark trader as processed
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="trader",
                entity_id=wallet_address,
                state="completed",
                metadata={
                    "tier": trader_record['performance_tier'],
                    "rank": rank,
                    "score": wallet.smart_trader_score
                }
            )
            
            self.logger.info(f"Processed trader {wallet_address}: {trader_record['performance_tier']} tier, rank {rank}")
            
            return trader_record
            
        except Exception as e:
            self.logger.error(f"Error processing trader {wallet_address}: {e}")
            self.failed_entities.append(wallet_address)
            
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="trader",
                entity_id=wallet_address,
                state="failed",
                error_message=str(e)
            )
            raise
            
    def calculate_tier_ranks(self, trader_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Calculate tier-specific rankings."""
        df = pd.DataFrame(trader_records)
        
        # Calculate tier-specific rankings
        for tier in df['performance_tier'].unique():
            tier_mask = df['performance_tier'] == tier
            tier_df = df[tier_mask].sort_values('composite_score', ascending=False)
            df.loc[tier_mask, 'tier_rank'] = range(1, len(tier_df) + 1)
        
        return df.to_dict('records')
        
    def execute(self) -> Dict[str, Any]:
        """Execute the smart traders task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            with get_db_session() as session:
                # Get all wallets that are eligible as smart traders
                eligible_wallets = self.get_eligible_wallets(session)
                
                if not eligible_wallets:
                    self.logger.info("No wallets are eligible as smart traders")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_eligible_traders"}
                    )
                    return {
                        "status": "completed",
                        "processed_traders": 0,
                        "new_traders": 0,
                        "message": "No eligible traders found"
                    }
                
                self.logger.info(f"Processing {len(eligible_wallets)} eligible smart traders")
                
                # Calculate percentile rankings for tier assignment
                scores = [w.smart_trader_score for w in eligible_wallets if w.smart_trader_score is not None]
                pnl_values = [w.total_realized_pnl_usd for w in eligible_wallets if w.total_realized_pnl_usd is not None]
                
                if not scores or not pnl_values:
                    self.logger.warning("No valid scores or PnL values found")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_valid_data"}
                    )
                    return {
                        "status": "completed",
                        "processed_traders": 0,
                        "message": "No valid trader data"
                    }
                
                # Calculate percentile thresholds
                percentiles = self.calculate_percentiles(scores)
                pnl_percentiles = self.calculate_percentiles(pnl_values)
                
                self.logger.info(f"Score percentiles: {percentiles}")
                self.logger.info(f"PnL percentiles: {pnl_percentiles}")
                
                # Process eligible traders
                trader_records = []
                for rank, wallet in enumerate(eligible_wallets, 1):
                    try:
                        trader_record = self.process_trader(wallet, rank, scores, pnl_values)
                        trader_records.append(trader_record)
                        self.processed_entities += 1
                        
                    except Exception as e:
                        # Error already logged in process_trader
                        continue
                
                # Calculate tier ranks
                if trader_records:
                    trader_records = self.calculate_tier_ranks(trader_records)
                    
                    # Store trader records
                    df = pd.DataFrame(trader_records)
                    
                    upsert_result = self.upsert_records(
                        session=session,
                        df=df,
                        model_class=SmartTrader,
                        conflict_columns=["wallet_address"],
                        batch_size=50
                    )
                    
                    self.new_records = upsert_result.get('inserted', 0)
                    self.updated_records = upsert_result.get('updated', 0)
                    
                    # Log tier distribution
                    tier_counts = df['performance_tier'].value_counts().to_dict()
                    self.logger.info(f"Tier distribution: {tier_counts}")
                    self.logger.info(f"Stored {len(trader_records)} smart trader records")
                else:
                    tier_counts = {}
            
            # Log task completion
            status = "completed" if not self.failed_entities else "completed_with_errors"
            
            task_metrics = {
                "new_traders": self.new_records,
                "updated_traders": self.updated_records,
                "total_traders": self.new_records + self.updated_records,
                "tier_distribution": tier_counts if 'tier_counts' in locals() else {}
            }
            
            error_summary = {"failed_traders": self.failed_entities} if self.failed_entities else None
            
            self.complete_task_logging(
                task_log, status,
                task_metrics=task_metrics,
                error_summary=error_summary
            )
            
            result = {
                "status": status,
                "processed_traders": self.processed_entities,
                "new_traders": self.new_records,
                "updated_traders": self.updated_records,
                "total_traders": self.new_records + self.updated_records,
                "tier_distribution": tier_counts if 'tier_counts' in locals() else {},
                "failed_traders": self.failed_entities
            }
            
            self.logger.info(f"Smart traders task completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Smart traders task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            raise


def process_smart_traders(**context) -> Dict[str, Any]:
    """Main entry point for the smart traders task."""
    task = SmartTradersTask(context)
    return task.execute()