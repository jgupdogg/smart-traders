"""
Silver wallet PnL task - Calculate FIFO-based PnL for wallets.
Standardized implementation with consistent patterns and error handling.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from collections import defaultdict
from sqlalchemy import and_, select
from src.models.bronze import BronzeTransaction
from src.models.silver import SilverWhale, SilverWalletPnL
from src.database.connection import get_db_session
from src.config.settings import get_silver_wallet_pnl_config
from smart_trader_tasks.common import SilverTaskBase

logger = logging.getLogger(__name__)


@dataclass
class Purchase:
    """Represents a token purchase for FIFO calculation."""
    quantity: float
    price: float
    timestamp: datetime
    transaction_hash: str


@dataclass
class Sale:
    """Represents a token sale for FIFO calculation."""
    quantity: float
    price: float
    timestamp: datetime
    transaction_hash: str


class TokenPosition:
    """Manages FIFO position tracking for a single token."""
    
    def __init__(self, token_address: str, token_symbol: str):
        self.token_address = token_address
        self.token_symbol = token_symbol
        self.purchases: List[Purchase] = []
        self.total_quantity = 0
        self.realized_pnl = 0
        self.trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_purchase_value = 0
        self.total_sale_value = 0
        
    def add_purchase(self, quantity: float, price: float, timestamp: datetime, tx_hash: str):
        """Add a purchase to the position."""
        if quantity <= 0 or price <= 0:
            return
            
        purchase = Purchase(quantity, price, timestamp, tx_hash)
        self.purchases.append(purchase)
        self.total_quantity += quantity
        self.total_purchase_value += quantity * price
        
    def process_sale(self, quantity: float, price: float, timestamp: datetime, tx_hash: str) -> float:
        """Process a sale using FIFO method and return realized PnL."""
        if quantity <= 0 or price <= 0:
            return 0
            
        remaining_to_sell = quantity
        cost_basis = 0
        
        # FIFO: Remove from oldest purchases first
        while remaining_to_sell > 0 and self.purchases:
            oldest_purchase = self.purchases[0]
            
            if oldest_purchase.quantity <= remaining_to_sell:
                # Use entire purchase
                cost_basis += oldest_purchase.quantity * oldest_purchase.price
                remaining_to_sell -= oldest_purchase.quantity
                self.purchases.pop(0)
            else:
                # Use partial purchase
                cost_basis += remaining_to_sell * oldest_purchase.price
                oldest_purchase.quantity -= remaining_to_sell
                remaining_to_sell = 0
        
        # Calculate realized PnL for this sale
        sale_proceeds = quantity * price
        trade_pnl = sale_proceeds - cost_basis
        
        self.realized_pnl += trade_pnl
        self.total_quantity -= quantity
        self.total_sale_value += sale_proceeds
        self.trade_count += 1
        
        if trade_pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        
        return trade_pnl
    
    def get_unrealized_pnl(self, current_price: float) -> float:
        """Calculate unrealized PnL based on current price."""
        if self.total_quantity <= 0:
            return 0
            
        current_value = self.total_quantity * current_price
        cost_basis = sum(p.quantity * p.price for p in self.purchases)
        return current_value - cost_basis


class WalletPnLCalculator:
    """Calculates comprehensive PnL metrics for a wallet."""
    
    def __init__(self, wallet_address: str):
        self.wallet_address = wallet_address
        self.positions: Dict[str, TokenPosition] = {}
        self.total_realized_pnl = 0
        self.total_unrealized_pnl = 0
        self.trade_history: List[Dict] = []
        self.first_trade_date = None
        self.last_trade_date = None
        
    def process_transaction(self, tx: BronzeTransaction):
        """Process a single transaction."""
        if not tx.base_address or not tx.base_ui_change_amount:
            return
            
        token_address = tx.base_address
        token_symbol = tx.base_symbol or token_address[:8]
        
        # Initialize position if not exists
        if token_address not in self.positions:
            self.positions[token_address] = TokenPosition(token_address, token_symbol)
        
        position = self.positions[token_address]
        quantity = abs(tx.base_ui_change_amount)
        price = tx.base_nearest_price or 0
        
        # Skip if price is invalid
        if price <= 0:
            return
            
        # Update trade dates
        if self.first_trade_date is None or tx.timestamp < self.first_trade_date:
            self.first_trade_date = tx.timestamp
        if self.last_trade_date is None or tx.timestamp > self.last_trade_date:
            self.last_trade_date = tx.timestamp
        
        # Determine if buy or sell based on typeSwap
        if tx.base_type_swap == 'to':
            # Buying the token
            position.add_purchase(quantity, price, tx.timestamp, tx.transaction_hash)
        elif tx.base_type_swap == 'from':
            # Selling the token
            trade_pnl = position.process_sale(quantity, price, tx.timestamp, tx.transaction_hash)
            
            # Record trade for history
            self.trade_history.append({
                'timestamp': tx.timestamp,
                'token_address': token_address,
                'token_symbol': token_symbol,
                'quantity': quantity,
                'price': price,
                'pnl': trade_pnl,
                'tx_hash': tx.transaction_hash
            })
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive wallet metrics."""
        # Aggregate realized PnL from all positions
        self.total_realized_pnl = sum(pos.realized_pnl for pos in self.positions.values())
        
        # Calculate trade statistics
        total_trades = sum(pos.trade_count for pos in self.positions.values())
        winning_trades = sum(pos.winning_trades for pos in self.positions.values())
        losing_trades = sum(pos.losing_trades for pos in self.positions.values())
        
        win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Calculate average win/loss
        winning_pnl = sum(trade['pnl'] for trade in self.trade_history if trade['pnl'] > 0)
        losing_pnl = sum(trade['pnl'] for trade in self.trade_history if trade['pnl'] < 0)
        
        avg_win = winning_pnl / winning_trades if winning_trades > 0 else 0
        avg_loss = abs(losing_pnl) / losing_trades if losing_trades > 0 else 0
        
        # Calculate trading frequency
        trading_days = 0
        if self.first_trade_date and self.last_trade_date:
            trading_days = (self.last_trade_date - self.first_trade_date).days + 1
        
        trade_frequency = total_trades / trading_days if trading_days > 0 else 0
        
        # Calculate volume metrics
        total_volume = sum(pos.total_purchase_value + pos.total_sale_value for pos in self.positions.values())
        avg_trade_size = total_volume / total_trades if total_trades > 0 else 0
        
        # Calculate drawdown (simplified)
        cumulative_pnl = 0
        peak_pnl = 0
        max_drawdown = 0
        
        for trade in sorted(self.trade_history, key=lambda x: x['timestamp']):
            cumulative_pnl += trade['pnl']
            if cumulative_pnl > peak_pnl:
                peak_pnl = cumulative_pnl
            drawdown = (peak_pnl - cumulative_pnl) / peak_pnl if peak_pnl > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)
        
        # Calculate Sharpe ratio (simplified)
        if len(self.trade_history) > 1:
            returns = [trade['pnl'] for trade in self.trade_history]
            avg_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = avg_return / std_return if std_return > 0 else 0
        else:
            sharpe_ratio = 0
        
        # Smart trader eligibility
        smart_trader_eligible = (
            total_trades >= 5 and
            win_rate >= 50 and
            self.total_realized_pnl > 0
        )
        
        # Calculate composite score
        smart_trader_score = 0
        if smart_trader_eligible:
            # Weighted score based on multiple factors
            pnl_score = min(self.total_realized_pnl / 10000, 1.0)  # Normalize to max 1.0
            win_rate_score = win_rate / 100
            frequency_score = min(trade_frequency / 5, 1.0)  # Normalize to max 1.0
            volume_score = min(total_volume / 100000, 1.0)  # Normalize to max 1.0
            
            smart_trader_score = (
                pnl_score * 0.3 +
                win_rate_score * 0.25 +
                frequency_score * 0.2 +
                volume_score * 0.15 +
                min(abs(sharpe_ratio) / 2, 1.0) * 0.1
            )
        
        return {
            'wallet_address': self.wallet_address,
            'total_realized_pnl_usd': self.total_realized_pnl,
            'total_unrealized_pnl_usd': self.total_unrealized_pnl,
            'win_rate_percent': win_rate,
            'avg_win_usd': avg_win,
            'avg_loss_usd': avg_loss,
            'trade_frequency_per_day': trade_frequency,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'tokens_traded_count': len(self.positions),
            'total_volume_usd': total_volume,
            'avg_trade_size_usd': avg_trade_size,
            'first_trade_at': self.first_trade_date,
            'last_trade_at': self.last_trade_date,
            'trading_days': trading_days,
            'max_drawdown_percent': max_drawdown * 100,
            'sharpe_ratio': sharpe_ratio,
            'smart_trader_eligible': smart_trader_eligible,
            'smart_trader_score': smart_trader_score,
            'last_calculated_at': datetime.utcnow(),
            'calculation_method': 'fifo',
            'tokens_analyzed': {
                'count': len(self.positions),
                'tokens': [
                    {
                        'address': pos.token_address,
                        'symbol': pos.token_symbol,
                        'realized_pnl': pos.realized_pnl,
                        'trade_count': pos.trade_count
                    }
                    for pos in self.positions.values()
                ]
            }
        }


class SilverWalletPnLTask(SilverTaskBase):
    """Silver wallet PnL task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("silver_wallet_pnl", context)
        self.config = get_silver_wallet_pnl_config()
        
    def get_whales_to_process(self, session: Any) -> List[Any]:
        """Get whales that need PnL processing."""
        whales_query = select(SilverWhale).where(
            and_(
                SilverWhale.pnl_processed == False,
                SilverWhale.transactions_processed == True
            )
        )
        
        return list(session.exec(whales_query))
        
    def get_wallet_transactions(self, session: Any, wallet_address: str) -> List[Any]:
        """Get all transactions for a wallet."""
        transactions_query = select(BronzeTransaction).where(
            BronzeTransaction.wallet_address == wallet_address
        ).order_by(BronzeTransaction.timestamp)
        
        return list(session.exec(transactions_query))
        
    def calculate_wallet_pnl(self, wallet_address: str, transactions: List[Any]) -> Dict[str, Any]:
        """Calculate PnL for a wallet using FIFO method."""
        calculator = WalletPnLCalculator(wallet_address)
        
        # Process each transaction
        for tx in transactions:
            calculator.process_transaction(tx)
        
        # Get final metrics
        return calculator.calculate_metrics()
        
    def process_wallet(self, session: Any, whale: Any) -> Dict[str, Any]:
        """Process PnL for a single wallet."""
        wallet_address = whale.wallet_address
        
        try:
            # Create/update state for this wallet
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="wallet",
                entity_id=wallet_address,
                state="processing",
                metadata={"tokens_held": whale.tokens_held_count}
            )
            
            # Get all transactions for this wallet
            transactions = self.get_wallet_transactions(session, wallet_address)
            
            if len(transactions) < self.config.min_trades_for_calculation:
                self.logger.info(f"Skipping wallet {wallet_address} - only {len(transactions)} transactions")
                
                self.state_manager.create_or_update_state(
                    task_name=self.task_name,
                    entity_type="wallet",
                    entity_id=wallet_address,
                    state="skipped",
                    metadata={"skip_reason": "insufficient_trades", "transaction_count": len(transactions)}
                )
                return None
            
            # Calculate PnL using FIFO method
            self.logger.info(f"Calculating PnL for wallet {wallet_address} with {len(transactions)} transactions")
            
            metrics = self.calculate_wallet_pnl(wallet_address, transactions)
            
            # Only include if calculation method matches config
            if self.config.calculation_method != 'fifo':
                return None
            
            # Mark wallet as processed
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="wallet",
                entity_id=wallet_address,
                state="completed",
                metadata={
                    "total_pnl": metrics['total_realized_pnl_usd'],
                    "total_trades": metrics['total_trades'],
                    "win_rate": metrics['win_rate_percent']
                }
            )
            
            # Update silver_whales table
            whale.pnl_processed = True
            whale.pnl_processed_at = datetime.utcnow()
            session.add(whale)
            session.commit()
            
            self.logger.info(f"Calculated PnL for {wallet_address}: ${metrics['total_realized_pnl_usd']:.2f}")
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error processing wallet {wallet_address}: {e}")
            self.failed_entities.append(wallet_address)
            
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="wallet",
                entity_id=wallet_address,
                state="failed",
                error_message=str(e)
            )
            raise
            
    def execute(self) -> Dict[str, Any]:
        """Execute the silver wallet PnL task."""
        task_log = self.start_task_logging({"config": self.config.__dict__})
        
        try:
            with get_db_session() as session:
                # Get whales that need PnL processing
                whales_to_process = self.get_whales_to_process(session)
                
                if not whales_to_process:
                    self.logger.info("No whales need PnL processing")
                    self.complete_task_logging(
                        task_log, "completed",
                        task_metrics={"reason": "no_whales_to_process"}
                    )
                    return {
                        "status": "completed",
                        "processed_wallets": 0,
                        "new_pnl_records": 0,
                        "message": "No wallets to process"
                    }
                
                self.logger.info(f"Processing {len(whales_to_process)} wallets for PnL calculation")
                
                # Process wallets
                pnl_records = []
                for whale in whales_to_process:
                    try:
                        metrics = self.process_wallet(session, whale)
                        if metrics:  # Skip if insufficient trades or wrong calculation method
                            pnl_records.append(metrics)
                        self.processed_entities += 1
                        
                    except Exception as e:
                        # Error already logged in process_wallet
                        continue
                
                # Store PnL records if any were processed
                if pnl_records:
                    df = pd.DataFrame(pnl_records)
                    
                    upsert_result = self.upsert_records(
                        session=session,
                        df=df,
                        model_class=SilverWalletPnL,
                        conflict_columns=["wallet_address"],
                        batch_size=50
                    )
                    
                    self.new_records = upsert_result.get('inserted', 0)
                    self.updated_records = upsert_result.get('updated', 0)
                    
                    self.logger.info(f"Stored {len(pnl_records)} PnL records")
            
            # Log task completion
            status = "completed" if not self.failed_entities else "completed_with_errors"
            
            task_metrics = {
                "new_pnl_records": self.new_records,
                "updated_pnl_records": self.updated_records,
                "total_pnl_records": self.new_records + self.updated_records
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
                "new_pnl_records": self.new_records,
                "updated_pnl_records": self.updated_records,
                "total_pnl_records": self.new_records + self.updated_records,
                "failed_wallets": self.failed_entities
            }
            
            self.logger.info(f"Silver wallet PnL task completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Silver wallet PnL task failed: {e}")
            
            self.complete_task_logging(
                task_log, "failed",
                error_summary={"error": str(e)}
            )
            
            raise


def process_silver_wallet_pnl(**context) -> Dict[str, Any]:
    """Main entry point for the silver wallet PnL task."""
    task = SilverWalletPnLTask(context)
    return task.execute()