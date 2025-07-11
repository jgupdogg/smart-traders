"""
Smart traders task - Rank traders based on performance metrics and assign tiers.
Enhanced implementation with tiered classification and coverage-based confidence scoring.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from sqlalchemy import select, text
from src.models.silver import SilverWalletPnL
from src.models.gold import SmartTrader
from src.database.connection import get_db_session
from src.config.settings import get_smart_traders_config
from smart_trader_tasks.common import GoldTaskBase

logger = logging.getLogger(__name__)


# Tier classification criteria (adjusted for realistic thresholds)
TIER_CRITERIA = {
    'ELITE': {
        'coverage_ratio': 85,
        'roi': 50,  # Reduced from 100%
        'win_rate': 70,
        'sharpe': 1.5,  # Reduced from 2.5
        'pnl': 10000,
        'trades': 50,
        'frequency': (0.5, 5),
        'drawdown': 20
    },
    'STRONG': {
        'coverage_ratio': 80,
        'roi': 25,  # Reduced from 60%
        'win_rate': 65,
        'sharpe': 1.0,  # Reduced from 2.0
        'pnl': 5000,
        'trades': 30,
        'frequency': (0.3, 7),
        'drawdown': 25
    },
    'PROMISING': {
        'coverage_ratio': 75,
        'roi': 15,  # Reduced from 40%
        'win_rate': 60,
        'sharpe': 0.5,  # Reduced from 1.5
        'pnl': 2000,
        'trades': 20,
        'frequency': (0.2, 10),
        'drawdown': 30
    },
    'QUALIFIED': {
        'coverage_ratio': 70,
        'roi': 5,   # Reduced from 20%
        'win_rate': 55,
        'sharpe': 0.2,  # Reduced from 1.0
        'pnl': 500,
        'trades': 10,
        'frequency': (0.01, 15),  # More lenient frequency
        'drawdown': 40
    },
    'EXPERIMENTAL': {
        'coverage_ratio': 60,
        'roi': 100,  # Reduced from 200%
        'win_rate': 75,
        'sharpe': 2.0,  # Reduced from 3.0
        'pnl': 1000,
        'trades': 15,
        'frequency': (0.01, 20),
        'drawdown': 35
    }
}


class SmartTradersTask(GoldTaskBase):
    """Smart traders task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("smart_traders", context)
        self.config = get_smart_traders_config()
        
    def get_eligible_wallets(self, session: Any) -> List[Dict[str, Any]]:
        """Get wallets that are eligible as smart traders and not yet processed."""
        try:
            # Use raw SQL to get all ROI and coverage metrics
            raw_query = text("""
                SELECT 
                    wallet_address, total_realized_pnl_usd, total_unrealized_pnl_usd, 
                    win_rate_percent, avg_win_usd, avg_loss_usd, trade_frequency_per_day,
                    total_trades, matched_trades, unmatched_trades, meaningful_trades,
                    coverage_ratio_percent, winning_trades, losing_trades, tokens_traded_count,
                    total_volume_usd, matched_volume_usd, matched_cost_basis_usd,
                    total_roi_percent, avg_roi_percent, median_roi_percent,
                    avg_trade_size_usd, first_trade_at, last_trade_at,
                    trading_days, max_drawdown_percent, sharpe_ratio, smart_trader_eligible,
                    smart_trader_score, gold_processed, last_calculated_at
                FROM silver.silver_wallet_pnl 
                WHERE smart_trader_eligible = true 
                AND gold_processed = false
                ORDER BY smart_trader_score DESC, total_realized_pnl_usd DESC
            """)
            result = session.execute(raw_query)
            wallets_data = result.fetchall()
            
            self.logger.info(f"Found {len(wallets_data)} eligible wallets via raw SQL")
            
            wallets_to_process = []
            for row in wallets_data:
                wallet_dict = {key: row._mapping.get(key) for key in [
                    'wallet_address', 'total_realized_pnl_usd', 'total_unrealized_pnl_usd',
                    'win_rate_percent', 'avg_win_usd', 'avg_loss_usd', 'trade_frequency_per_day',
                    'total_trades', 'matched_trades', 'unmatched_trades', 'meaningful_trades',
                    'coverage_ratio_percent', 'winning_trades', 'losing_trades', 'tokens_traded_count',
                    'total_volume_usd', 'matched_volume_usd', 'matched_cost_basis_usd',
                    'total_roi_percent', 'avg_roi_percent', 'median_roi_percent',
                    'avg_trade_size_usd', 'first_trade_at', 'last_trade_at',
                    'trading_days', 'max_drawdown_percent', 'sharpe_ratio', 'smart_trader_eligible',
                    'smart_trader_score', 'last_calculated_at'
                ]}
                wallets_to_process.append(wallet_dict)
            
            return wallets_to_process
            
        except Exception as e:
            self.logger.error(f"Failed to get eligible wallets: {e}")
            raise
        
    def calculate_performance_tier(self, wallet: Dict[str, Any]) -> Tuple[str, str]:
        """Assign performance tier based on comprehensive metrics."""
        coverage = wallet.get('coverage_ratio_percent') or 0
        roi = wallet.get('total_roi_percent') or 0
        win_rate = wallet.get('win_rate_percent') or 0
        sharpe = wallet.get('sharpe_ratio') or 0
        pnl = wallet.get('total_realized_pnl_usd') or 0
        trades = wallet.get('meaningful_trades') or 0
        frequency = wallet.get('trade_frequency_per_day') or 0
        drawdown = wallet.get('max_drawdown_percent') or 100
        
        # Determine coverage confidence
        if coverage >= 85:
            coverage_confidence = "High"
        elif coverage >= 80:
            coverage_confidence = "Good"
        elif coverage >= 70:
            coverage_confidence = "Acceptable"
        else:
            coverage_confidence = "Low"
        
        # Check tier criteria in order (most restrictive first)
        # Temporarily skip ROI requirement if cost basis data is missing (roi == 0)
        for tier_name, criteria in TIER_CRITERIA.items():
            roi_check = roi >= criteria['roi'] if roi > 0 else True  # Skip ROI check if ROI is 0 (data quality issue)
            
            if (coverage >= criteria['coverage_ratio'] and
                roi_check and
                win_rate >= criteria['win_rate'] and
                sharpe >= criteria['sharpe'] and
                pnl >= criteria['pnl'] and
                trades >= criteria['trades'] and
                criteria['frequency'][0] <= frequency <= criteria['frequency'][1] and
                drawdown <= criteria['drawdown']):
                return tier_name, coverage_confidence
        
        # If no tier criteria met, return UNQUALIFIED
        return "UNQUALIFIED", coverage_confidence
    
    def calculate_tier_score(self, wallet: Dict[str, Any], tier: str) -> float:
        """Calculate tier-specific score with coverage weighting."""
        coverage = (wallet.get('coverage_ratio_percent') or 0) / 100
        roi = wallet.get('total_roi_percent') or 0
        sharpe = wallet.get('sharpe_ratio') or 0
        win_rate = wallet.get('win_rate_percent') or 0
        pnl = wallet.get('total_realized_pnl_usd') or 0
        frequency = wallet.get('trade_frequency_per_day') or 0
        
        # Normalize scores to 0-1 scale
        roi_score = min(max(roi / 200, 0), 1.0)  # 200% ROI = max score
        sharpe_score = min(max(sharpe / 3, 0), 1.0)  # 3.0 = max sharpe
        win_rate_score = win_rate / 100
        volume_score = min(max(pnl / 20000, 0), 1.0)  # $20k = max score
        coverage_score = coverage
        
        # Activity penalty
        if frequency > 10:
            activity_penalty = 0.9  # Too many trades
        elif frequency < 0.2:
            activity_penalty = 0.95  # Too few trades
        else:
            activity_penalty = 1.0
        
        # Tier-specific weights
        if tier == "ELITE":
            weights = {'coverage': 0.20, 'roi': 0.25, 'sharpe': 0.25, 'win_rate': 0.20, 'volume': 0.10}
        elif tier == "STRONG":
            weights = {'coverage': 0.25, 'roi': 0.25, 'sharpe': 0.20, 'win_rate': 0.20, 'volume': 0.10}
        elif tier == "PROMISING":
            weights = {'coverage': 0.30, 'roi': 0.20, 'sharpe': 0.20, 'win_rate': 0.20, 'volume': 0.10}
        else:  # QUALIFIED/EXPERIMENTAL/UNQUALIFIED
            weights = {'coverage': 0.35, 'roi': 0.20, 'sharpe': 0.20, 'win_rate': 0.15, 'volume': 0.10}
        
        final_score = (
            coverage_score * weights['coverage'] +
            roi_score * weights['roi'] +
            sharpe_score * weights['sharpe'] +
            win_rate_score * weights['win_rate'] +
            volume_score * weights['volume']
        ) * activity_penalty * 100  # Convert to 0-100
        
        return round(final_score, 2)
            
    def create_trader_record(
        self, 
        wallet: Dict[str, Any], 
        rank: int, 
        scores: List[float], 
        pnl_values: List[float]
    ) -> Dict[str, Any]:
        """Create a trader record from wallet data using enhanced tier classification."""
        wallet_address = wallet['wallet_address']
        
        # Assign performance tier and coverage confidence using new logic
        performance_tier, coverage_confidence = self.calculate_performance_tier(wallet)
        
        # Calculate tier-specific score
        tier_score = self.calculate_tier_score(wallet, performance_tier)
        
        # Calculate days since first/last trade
        days_since_first_trade = 0
        if wallet.get('first_trade_at'):
            days_since_first_trade = (datetime.utcnow() - wallet['first_trade_at']).days
        
        # Check auto-follow eligibility
        auto_follow_eligible = (
            performance_tier in ['ELITE', 'STRONG'] and
            wallet.get('coverage_ratio_percent', 0) >= self.config.tier_criteria['elite']['coverage_ratio'] and
            days_since_first_trade >= 30
        )
        
        # Determine verified performance (30+ days of consistent performance)
        verified_performance = (
            days_since_first_trade >= 30 and
            wallet.get('meaningful_trades', 0) >= 20 and
            wallet.get('coverage_ratio_percent', 0) >= 75
        )
        
        # Build criteria met dictionary
        criteria = TIER_CRITERIA.get(performance_tier, {})
        tier_criteria_met = {
            'tier': performance_tier,
            'coverage_ratio': wallet.get('coverage_ratio_percent', 0) >= criteria.get('coverage_ratio', 0),
            'roi': wallet.get('total_roi_percent', 0) >= criteria.get('roi', 0),
            'win_rate': wallet.get('win_rate_percent', 0) >= criteria.get('win_rate', 0),
            'sharpe': wallet.get('sharpe_ratio', 0) >= criteria.get('sharpe', 0),
            'pnl': wallet.get('total_realized_pnl_usd', 0) >= criteria.get('pnl', 0),
            'trades': wallet.get('meaningful_trades', 0) >= criteria.get('trades', 0),
            'frequency_ok': (
                criteria.get('frequency', [0, 999])[0] <= 
                wallet.get('trade_frequency_per_day', 0) <= 
                criteria.get('frequency', [0, 999])[1]
            ),
            'drawdown': wallet.get('max_drawdown_percent', 100) <= criteria.get('drawdown', 100)
        }
        
        # Enhanced score breakdown
        score_breakdown = {
            'tier_score': tier_score,
            'coverage_score': wallet.get('coverage_ratio_percent', 0) / 100,
            'roi_score': min(max(wallet.get('total_roi_percent', 0) / 200, 0), 1.0),
            'sharpe_score': min(max(wallet.get('sharpe_ratio', 0) / 3, 0), 1.0),
            'win_rate_score': wallet.get('win_rate_percent', 0) / 100,
            'volume_score': min(max(wallet.get('total_realized_pnl_usd', 0) / 20000, 0), 1.0),
            'consistency_score': wallet.get('coverage_ratio_percent', 0) / 100 * wallet.get('win_rate_percent', 0) / 100
        }
        
        # Create trader record matching enhanced SmartTrader model
        trader_record = {
            'wallet_address': wallet_address,
            'performance_tier': performance_tier,
            'tier_score': tier_score,
            'tier_rank': 0,  # Will be calculated after grouping by tier
            'overall_rank': rank,
            'coverage_confidence': coverage_confidence,
            'coverage_ratio_percent': wallet.get('coverage_ratio_percent') or 0,
            'total_roi_percent': wallet.get('total_roi_percent') or 0,
            'avg_roi_percent': wallet.get('avg_roi_percent') or 0,
            'median_roi_percent': wallet.get('median_roi_percent') or 0,
            'total_realized_pnl_usd': wallet.get('total_realized_pnl_usd') or 0,
            'win_rate_percent': wallet.get('win_rate_percent') or 0,
            'sharpe_ratio': wallet.get('sharpe_ratio') or 0,
            'max_drawdown_percent': wallet.get('max_drawdown_percent') or 0,
            'meaningful_trades': wallet.get('meaningful_trades') or 0,
            'matched_trades': wallet.get('matched_trades') or 0,
            'total_trades': wallet.get('total_trades') or 0,
            'trade_frequency_per_day': wallet.get('trade_frequency_per_day') or 0,
            'trading_days': wallet.get('trading_days') or 0,
            'days_since_first_trade': days_since_first_trade,
            'total_volume_usd': wallet.get('total_volume_usd') or 0,
            'matched_volume_usd': wallet.get('matched_volume_usd') or 0,
            'matched_cost_basis_usd': wallet.get('matched_cost_basis_usd') or 0,
            'avg_trade_size_usd': wallet.get('avg_trade_size_usd') or 0,
            'last_30d_roi_percent': None,  # Would need additional calculation
            'last_30d_trades': None,
            'last_30d_win_rate': None,
            'last_30d_pnl_usd': None,
            'first_trade_at': wallet.get('first_trade_at') or datetime.utcnow(),
            'last_trade_at': wallet.get('last_trade_at') or datetime.utcnow(),
            'last_updated_at': datetime.utcnow(),
            'tokens_traded_count': wallet.get('tokens_traded_count') or 0,
            'top_tokens': None,  # Would need additional analysis from tokens_analyzed
            'consistency_score': score_breakdown['consistency_score'],
            'volatility_score': None,  # Would need additional calculation
            'auto_follow_eligible': auto_follow_eligible,
            'verified_performance': verified_performance,
            'score_breakdown': score_breakdown,
            'tier_criteria_met': tier_criteria_met,
            'source_silver_calculated_at': wallet.get('last_calculated_at') or datetime.utcnow(),
            'calculation_notes': f"Tier: {performance_tier}, Coverage: {coverage_confidence}"
        }
        
        return trader_record
    
    def mark_wallet_pnl_processed(self, session: Any, wallet_address: str):
        """Mark silver_wallet_pnl record as processed for gold layer."""
        try:
            # Mark silver_wallet_pnl record as gold_processed
            update_query = text("""
                UPDATE silver.silver_wallet_pnl 
                SET gold_processed = true, gold_processed_at = :timestamp 
                WHERE wallet_address = :wallet_address
            """)
            session.execute(update_query, {
                "timestamp": datetime.utcnow(), 
                "wallet_address": wallet_address
            })
            session.commit()
            
            # Marked silver_wallet_pnl as gold_processed
            
        except Exception as e:
            self.logger.error(f"Failed to mark silver_wallet_pnl as processed for wallet {wallet_address}: {e}")
            session.rollback()
            raise
        
    def process_trader(
        self, 
        wallet: Dict[str, Any], 
        rank: int, 
        scores: List[float], 
        pnl_values: List[float]
    ) -> Dict[str, Any]:
        """Process a single trader."""
        wallet_address = wallet['wallet_address']
        
        try:
            # Create/update state for this trader
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="trader",
                entity_id=wallet_address,
                state="processing",
                metadata={"pnl": wallet.get('total_realized_pnl_usd'), "score": wallet.get('smart_trader_score')}
            )
            
            # Create trader record
            trader_record = self.create_trader_record(wallet, rank, scores, pnl_values)
            
            # Mark trader as processed and mark silver_wallet_pnl as processed
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="trader",
                entity_id=wallet_address,
                state="completed",
                metadata={
                    "tier": trader_record['performance_tier'],
                    "rank": rank,
                    "score": wallet.get('smart_trader_score')
                }
            )
            
            # Processed trader
            
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
        
        # Calculate tier-specific rankings based on tier_score
        for tier in df['performance_tier'].unique():
            tier_mask = df['performance_tier'] == tier
            tier_df = df[tier_mask].sort_values('tier_score', ascending=False)
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
                scores = [w.get('smart_trader_score') for w in eligible_wallets if w.get('smart_trader_score') is not None]
                pnl_values = [w.get('total_realized_pnl_usd') for w in eligible_wallets if w.get('total_realized_pnl_usd') is not None]
                
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
                        batch_size=self.config.batch_size
                    )
                    
                    self.new_records = upsert_result.get('inserted', 0)
                    self.updated_records = upsert_result.get('updated', 0)
                    
                    # Mark all processed wallets as gold_processed in silver_wallet_pnl
                    processed_wallet_addresses = [t['wallet_address'] for t in trader_records]
                    for wallet_address in processed_wallet_addresses:
                        try:
                            self.mark_wallet_pnl_processed(session, wallet_address)
                        except Exception as e:
                            self.logger.error(f"Failed to mark wallet {wallet_address} as gold_processed: {e}")
                    
                    # Log tier distribution
                    tier_counts = df['performance_tier'].value_counts().to_dict()
                    self.logger.info(f"Tier distribution: {tier_counts}")
                    
                    # Log auto-follow eligible traders
                    auto_follow_count = df['auto_follow_eligible'].sum()
                    self.logger.info(f"Auto-follow eligible traders: {auto_follow_count}")
                    
                    # Log coverage confidence distribution
                    coverage_counts = df['coverage_confidence'].value_counts().to_dict()
                    self.logger.info(f"Coverage confidence distribution: {coverage_counts}")
                    
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