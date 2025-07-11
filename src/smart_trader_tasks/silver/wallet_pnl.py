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
from sqlalchemy import and_, select, text
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


@dataclass
class SaleResult:
    """Result of processing a sale with FIFO matching."""
    matched_quantity: float
    unmatched_quantity: float
    matched_pnl: float
    cost_basis: float
    sale_proceeds: float


class TokenPosition:
    """Manages FIFO position tracking for a single token."""
    
    def __init__(self, token_address: str, token_symbol: str):
        self.token_address = token_address
        self.token_symbol = token_symbol
        self.purchases: List[Purchase] = []
        self.total_quantity = 0
        self.realized_pnl = 0
        self.matched_trade_count = 0
        self.unmatched_trade_count = 0
        self.winning_trades = 0
        self.losing_trades = 0
        self.total_purchase_value = 0
        self.total_sale_value = 0
        self.matched_sale_value = 0
        self.unmatched_sale_value = 0
        self.matched_cost_basis = 0  # Track cost basis for matched sales
        self.trade_rois: List[float] = []  # Track ROI for each matched trade
        
    def add_purchase(self, quantity: float, price: float, timestamp: datetime, tx_hash: str):
        """Add a purchase to the position."""
        if quantity <= 0 or price <= 0:
            return
            
        purchase = Purchase(quantity, price, timestamp, tx_hash)
        self.purchases.append(purchase)
        self.total_quantity += quantity
        self.total_purchase_value += quantity * price
        
    def process_sale(self, quantity: float, price: float, timestamp: datetime, tx_hash: str) -> SaleResult:
        """Process a sale using FIFO method and return detailed results."""
        if quantity <= 0 or price <= 0:
            return SaleResult(0, 0, 0, 0, 0)
            
        sale_proceeds = quantity * price
        remaining_to_sell = quantity
        cost_basis = 0
        matched_quantity = 0
        
        # Calculate how much we can match against existing purchases
        available_quantity = sum(p.quantity for p in self.purchases)
        
        if available_quantity >= quantity:
            # Full match possible
            matched_quantity = quantity
            unmatched_quantity = 0
        elif available_quantity > 0:
            # Partial match
            matched_quantity = available_quantity
            unmatched_quantity = quantity - available_quantity
        else:
            # No match possible
            matched_quantity = 0
            unmatched_quantity = quantity
        
        # Process matched portion using FIFO
        if matched_quantity > 0:
            remaining_to_match = matched_quantity
            
            while remaining_to_match > 0 and self.purchases:
                oldest_purchase = self.purchases[0]
                
                if oldest_purchase.quantity <= remaining_to_match:
                    # Use entire purchase
                    cost_basis += oldest_purchase.quantity * oldest_purchase.price
                    remaining_to_match -= oldest_purchase.quantity
                    self.purchases.pop(0)
                else:
                    # Use partial purchase
                    cost_basis += remaining_to_match * oldest_purchase.price
                    oldest_purchase.quantity -= remaining_to_match
                    remaining_to_match = 0
        
        # Calculate PnL only for matched portion
        matched_proceeds = matched_quantity * price
        matched_pnl = matched_proceeds - cost_basis
        
        # Update position tracking
        self.total_quantity -= quantity
        self.total_sale_value += sale_proceeds
        
        if matched_quantity > 0:
            self.realized_pnl += matched_pnl
            self.matched_trade_count += 1
            self.matched_sale_value += matched_proceeds
            self.matched_cost_basis += cost_basis
            
            # Calculate ROI for this trade
            if cost_basis > 0:
                trade_roi = ((matched_proceeds - cost_basis) / cost_basis) * 100
                self.trade_rois.append(trade_roi)
            
            # Only count as winning trade if profit is at least $50 to filter out minor gains
            if matched_pnl >= 50.0:
                self.winning_trades += 1
            elif matched_pnl <= -50.0:
                self.losing_trades += 1
            # Trades between -$50 and +$50 are not counted as winning or losing
        
        if unmatched_quantity > 0:
            self.unmatched_trade_count += 1
            self.unmatched_sale_value += unmatched_quantity * price
        
        return SaleResult(
            matched_quantity=matched_quantity,
            unmatched_quantity=unmatched_quantity,
            matched_pnl=matched_pnl,
            cost_basis=cost_basis,
            sale_proceeds=sale_proceeds
        )
    
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
        self.matched_trade_history: List[Dict] = []
        self.unmatched_trade_history: List[Dict] = []
        self.first_trade_date = None
        self.last_trade_date = None
        
    def process_transaction(self, tx: Dict[str, Any]):
        """Process a single transaction, handling both base and quote tokens."""
        timestamp = tx.get('timestamp')
        transaction_hash = tx.get('transaction_hash')
        
        # Update trade dates
        if timestamp:
            if self.first_trade_date is None or timestamp < self.first_trade_date:
                self.first_trade_date = timestamp
            if self.last_trade_date is None or timestamp > self.last_trade_date:
                self.last_trade_date = timestamp
        
        # Process base token if data is available
        base_address = tx.get('base_address')
        base_ui_change_amount = tx.get('base_ui_change_amount')
        base_nearest_price = tx.get('base_nearest_price')
        base_type_swap = tx.get('base_type_swap')
        
        if base_address and base_ui_change_amount and base_nearest_price and base_nearest_price > 0:
            self._process_token_side(
                token_address=base_address,
                token_symbol=tx.get('base_symbol') or base_address[:8],
                ui_change_amount=base_ui_change_amount,
                price=base_nearest_price,
                type_swap=base_type_swap,
                timestamp=timestamp,
                tx_hash=transaction_hash,
                side='base'
            )
        
        # Process quote token if data is available
        quote_address = tx.get('quote_address')
        quote_ui_change_amount = tx.get('quote_ui_change_amount')
        quote_nearest_price = tx.get('quote_nearest_price')
        quote_type_swap = tx.get('quote_type_swap')
        
        if quote_address and quote_ui_change_amount and quote_nearest_price and quote_nearest_price > 0:
            self._process_token_side(
                token_address=quote_address,
                token_symbol=tx.get('quote_symbol') or quote_address[:8],
                ui_change_amount=quote_ui_change_amount,
                price=quote_nearest_price,
                type_swap=quote_type_swap,
                timestamp=timestamp,
                tx_hash=transaction_hash,
                side='quote'
            )
    
    def _process_token_side(self, token_address: str, token_symbol: str, ui_change_amount: float, 
                           price: float, type_swap: str, timestamp: datetime, tx_hash: str, side: str):
        """Process one side of a token transaction."""
        # Initialize position if not exists
        if token_address not in self.positions:
            self.positions[token_address] = TokenPosition(token_address, token_symbol)
        
        position = self.positions[token_address]
        quantity = abs(ui_change_amount)
        
        # Determine if this is a buy or sell based on type_swap
        if type_swap == 'to':
            # Wallet is receiving this token (buying)
            position.add_purchase(quantity, price, timestamp, tx_hash)
            # Purchase processed
            
        elif type_swap == 'from':
            # Wallet is giving up this token (selling)
            sale_result = position.process_sale(quantity, price, timestamp, tx_hash)
            
            # Record matched trades for history (only trades with known cost basis)
            if sale_result.matched_quantity > 0:
                self.matched_trade_history.append({
                    'timestamp': timestamp,
                    'token_address': token_address,
                    'token_symbol': token_symbol,
                    'quantity': sale_result.matched_quantity,
                    'price': price,
                    'pnl': sale_result.matched_pnl,
                    'cost_basis': sale_result.cost_basis,
                    'tx_hash': tx_hash,
                    'side': side
                })
            
            # Record unmatched trades separately
            if sale_result.unmatched_quantity > 0:
                self.unmatched_trade_history.append({
                    'timestamp': timestamp,
                    'token_address': token_address,
                    'token_symbol': token_symbol,
                    'quantity': sale_result.unmatched_quantity,
                    'price': price,
                    'proceeds': sale_result.unmatched_quantity * price,
                    'tx_hash': tx_hash,
                    'side': side
                })
            
            # Sale processed
    
    def calculate_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive wallet metrics."""
        # Filter out positions with no meaningful activity
        active_positions = {addr: pos for addr, pos in self.positions.items() 
                          if pos.matched_trade_count > 0 or pos.unmatched_trade_count > 0 or pos.total_purchase_value > 0}
        
        # Aggregate realized PnL from all positions (only matched trades)
        self.total_realized_pnl = sum(pos.realized_pnl for pos in active_positions.values())
        
        # Calculate trade statistics (only count matched trades for PnL metrics)
        matched_trades = sum(pos.matched_trade_count for pos in active_positions.values())
        unmatched_trades = sum(pos.unmatched_trade_count for pos in active_positions.values())
        total_trades = matched_trades + unmatched_trades
        
        winning_trades = sum(pos.winning_trades for pos in active_positions.values())
        losing_trades = sum(pos.losing_trades for pos in active_positions.values())
        
        # Calculate meaningful trades (winning or losing with >= $50 difference)
        meaningful_trades = winning_trades + losing_trades
        neutral_trades = matched_trades - meaningful_trades
        
        # Calculate win rate only for meaningful trades (>= $50 profit/loss)
        win_rate = (winning_trades / meaningful_trades * 100) if meaningful_trades > 0 else 0
        
        # Calculate coverage ratio
        coverage_ratio = (matched_trades / total_trades * 100) if total_trades > 0 else 0
        
        # Calculate average win/loss from meaningful trades only (>= $50 profit/loss)
        winning_trades_list = [trade for trade in self.matched_trade_history if trade['pnl'] >= 50.0]
        losing_trades_list = [trade for trade in self.matched_trade_history if trade['pnl'] <= -50.0]
        
        winning_pnl = sum(trade['pnl'] for trade in winning_trades_list)
        losing_pnl = sum(trade['pnl'] for trade in losing_trades_list)
        
        avg_win = winning_pnl / len(winning_trades_list) if winning_trades_list else 0
        avg_loss = abs(losing_pnl) / len(losing_trades_list) if losing_trades_list else 0
        
        # Calculate trading frequency based on matched trades
        trading_days = 0
        if self.first_trade_date and self.last_trade_date:
            trading_days = (self.last_trade_date - self.first_trade_date).days + 1
        
        trade_frequency = matched_trades / trading_days if trading_days > 0 else 0
        
        # Calculate volume metrics (separate buy and sell volumes)
        total_purchase_volume = sum(pos.total_purchase_value for pos in active_positions.values())
        matched_sale_volume = sum(pos.matched_sale_value for pos in active_positions.values())
        unmatched_sale_volume = sum(pos.unmatched_sale_value for pos in active_positions.values())
        total_sale_volume = sum(pos.total_sale_value for pos in active_positions.values())
        total_volume = total_purchase_volume + total_sale_volume
        
        # Calculate ROI metrics (only for matched trades)
        total_matched_cost_basis = sum(pos.matched_cost_basis for pos in active_positions.values())
        total_roi_percent = 0
        avg_roi_percent = 0
        median_roi_percent = 0
        
        if total_matched_cost_basis > 0:
            # Overall ROI based on total realized PnL and total cost basis
            total_roi_percent = (self.total_realized_pnl / total_matched_cost_basis) * 100
        
        # Collect all trade ROIs for average and median calculations
        all_trade_rois = []
        for pos in active_positions.values():
            all_trade_rois.extend(pos.trade_rois)
        
        if all_trade_rois:
            avg_roi_percent = np.mean(all_trade_rois)
            median_roi_percent = np.median(all_trade_rois)
        
        # Calculate average trade size based on matched trades only
        avg_trade_size = (total_purchase_volume + matched_sale_volume) / (matched_trades * 2) if matched_trades > 0 else 0
        
        # Calculate drawdown using cumulative PnL over time (matched trades only)
        cumulative_pnl = 0
        peak_pnl = 0
        max_drawdown = 0
        
        for trade in sorted(self.matched_trade_history, key=lambda x: x['timestamp']):
            cumulative_pnl += trade['pnl']
            if cumulative_pnl > peak_pnl:
                peak_pnl = cumulative_pnl
            if peak_pnl > 0:
                drawdown = (peak_pnl - cumulative_pnl) / peak_pnl
                max_drawdown = max(max_drawdown, drawdown)
        
        # Calculate Sharpe ratio (simplified - using matched trade PnL as returns)
        sharpe_ratio = 0
        if len(self.matched_trade_history) > 1:
            returns = [trade['pnl'] for trade in self.matched_trade_history]
            avg_return = np.mean(returns)
            std_return = np.std(returns)
            sharpe_ratio = avg_return / std_return if std_return > 0 else 0
        
        # Enhanced smart trader eligibility (based on meaningful trades with >= $50 profit/loss)
        smart_trader_eligible = (
            meaningful_trades >= 5 and  # Minimum meaningful trade count (>= $50 profit/loss)
            coverage_ratio >= 70 and  # At least 70% of trades must have known cost basis
            win_rate >= 60 and    # Minimum win rate (for meaningful trades)
            self.total_realized_pnl > 500 and  # Minimum absolute profit ($500)
            total_roi_percent >= 20 and  # Minimum 20% ROI on matched trades
            trading_days >= 2     # At least 2 days of activity
        )
        
        # Calculate composite score with better weighting
        smart_trader_score = 0
        if smart_trader_eligible and total_volume > 0:
            # Normalize scores to 0-1 range
            pnl_score = min(max(self.total_realized_pnl / 5000, 0), 1.0)  # $5k = max score
            win_rate_score = min(win_rate / 100, 1.0)
            volume_score = min(total_volume / 50000, 1.0)  # $50k = max score
            frequency_score = min(trade_frequency / 10, 1.0)  # 10 trades/day = max score
            sharpe_score = min(max(sharpe_ratio / 3, 0), 1.0) if sharpe_ratio > 0 else 0  # 3.0 = max sharpe
            roi_score = min(max(total_roi_percent / 200, 0), 1.0)  # 200% ROI = max score
            
            smart_trader_score = (
                pnl_score * 0.25 +      # Absolute profitability
                roi_score * 0.25 +      # Return on investment
                win_rate_score * 0.20 + # Consistency matters
                volume_score * 0.15 +   # Scale of trading
                frequency_score * 0.10 + # Activity level
                sharpe_score * 0.05     # Risk-adjusted returns
            )
        
        # PnL summary calculated
        
        return {
            'wallet_address': self.wallet_address,
            'total_realized_pnl_usd': round(self.total_realized_pnl, 2),
            'total_unrealized_pnl_usd': round(self.total_unrealized_pnl, 2),
            'win_rate_percent': round(win_rate, 2),
            'avg_win_usd': round(avg_win, 2),
            'avg_loss_usd': round(avg_loss, 2),
            'trade_frequency_per_day': round(trade_frequency, 3),
            'total_trades': int(total_trades),
            'matched_trades': int(matched_trades),
            'unmatched_trades': int(unmatched_trades),
            'meaningful_trades': int(meaningful_trades),
            'neutral_trades': int(neutral_trades),
            'coverage_ratio_percent': round(coverage_ratio, 2),
            'winning_trades': int(winning_trades),
            'losing_trades': int(losing_trades),
            'tokens_traded_count': len(active_positions),
            'total_volume_usd': round(total_volume, 2),
            'matched_volume_usd': round(total_purchase_volume + matched_sale_volume, 2),
            'unmatched_volume_usd': round(unmatched_sale_volume, 2),
            'avg_trade_size_usd': round(avg_trade_size, 2),
            'matched_cost_basis_usd': round(total_matched_cost_basis, 2),
            'total_roi_percent': round(total_roi_percent, 2),
            'avg_roi_percent': round(avg_roi_percent, 2),
            'median_roi_percent': round(median_roi_percent, 2),
            'first_trade_at': self.first_trade_date,
            'last_trade_at': self.last_trade_date,
            'trading_days': int(trading_days),
            'max_drawdown_percent': round(max_drawdown * 100, 2),
            'sharpe_ratio': round(sharpe_ratio, 3),
            'smart_trader_eligible': smart_trader_eligible,
            'smart_trader_score': round(smart_trader_score, 4),
            'last_calculated_at': datetime.utcnow(),
            'calculation_method': 'fifo_cost_basis_matched_min50',
            'tokens_analyzed': {
                'count': len(active_positions),
                'tokens': [
                    {
                        'address': pos.token_address,
                        'symbol': pos.token_symbol,
                        'realized_pnl': round(pos.realized_pnl, 2),
                        'matched_trades': pos.matched_trade_count,
                        'unmatched_trades': pos.unmatched_trade_count,
                        'purchase_volume': round(pos.total_purchase_value, 2),
                        'matched_sale_volume': round(pos.matched_sale_value, 2),
                        'unmatched_sale_volume': round(pos.unmatched_sale_value, 2),
                        'matched_cost_basis': round(pos.matched_cost_basis, 2),
                        'roi_percent': round((pos.realized_pnl / pos.matched_cost_basis * 100) if pos.matched_cost_basis > 0 else 0, 2),
                        'avg_trade_roi': round(np.mean(pos.trade_rois) if pos.trade_rois else 0, 2)
                    }
                    for pos in active_positions.values()
                    if pos.matched_trade_count > 0 or pos.unmatched_trade_count > 0 or abs(pos.realized_pnl) > 1
                ]
            }
        }


class SilverWalletPnLTask(SilverTaskBase):
    """Silver wallet PnL task implementation."""
    
    def __init__(self, context: Dict[str, Any]):
        super().__init__("silver_wallet_pnl", context)
        self.config = get_silver_wallet_pnl_config()
        
    def get_whales_to_process(self, session: Any) -> List[Dict[str, Any]]:
        """Get whales that need PnL processing."""
        try:
            # First, get overall whale processing status
            status_query = text("""
                SELECT 
                    COUNT(*) as total_whales,
                    COUNT(CASE WHEN pnl_processed = true THEN 1 END) as pnl_processed,
                    COUNT(CASE WHEN pnl_processed = false THEN 1 END) as pnl_unprocessed,
                    COUNT(CASE WHEN transactions_processed = true THEN 1 END) as transactions_processed,
                    COUNT(CASE WHEN pnl_processed = false AND transactions_processed = true THEN 1 END) as ready_for_pnl
                FROM silver.silver_whales
            """)
            status_result = session.execute(status_query)
            status = status_result.fetchone()
            
            self.logger.info(f"Whale processing status: {status.total_whales} total, {status.pnl_processed} PnL processed, {status.transactions_processed} transactions processed, {status.ready_for_pnl} ready for PnL")
            
            # Use raw SQL for consistency - process ALL unprocessed wallets
            raw_query = text("""
                SELECT wallet_address, tokens_held_count, pnl_processed_at, transactions_processed_at
                FROM silver.silver_whales 
                WHERE pnl_processed = false 
                AND transactions_processed = true
                ORDER BY wallet_address
            """)
            result = session.execute(raw_query)
            whales_data = result.fetchall()
            
            self.logger.info(f"Found {len(whales_data)} whales ready for PnL processing")
            
            whales_to_process = []
            for row in whales_data:
                whale_dict = {
                    'wallet_address': row._mapping.get('wallet_address'),
                    'tokens_held_count': row._mapping.get('tokens_held_count', 0),
                    'pnl_processed_at': row._mapping.get('pnl_processed_at'),
                    'transactions_processed_at': row._mapping.get('transactions_processed_at')
                }
                whales_to_process.append(whale_dict)
            
            return whales_to_process
            
        except Exception as e:
            self.logger.error(f"Failed to get whales to process: {e}")
            raise
        
    def get_wallet_transactions(self, session: Any, wallet_address: str) -> List[Dict[str, Any]]:
        """Get all transactions for a wallet."""
        try:
            # Use raw SQL to avoid SQLModel column access issues
            raw_query = text("""
                SELECT 
                    transaction_hash, wallet_address, timestamp, tx_type, source,
                    base_symbol, base_address, base_type_swap, base_ui_change_amount, base_nearest_price,
                    quote_symbol, quote_address, quote_type_swap, quote_ui_change_amount, quote_nearest_price
                FROM bronze.bronze_transactions 
                WHERE wallet_address = :wallet_address
                ORDER BY timestamp ASC
            """)
            result = session.execute(raw_query, {"wallet_address": wallet_address})
            transactions_data = result.fetchall()
            
            transactions = []
            for row in transactions_data:
                tx_dict = {
                    'transaction_hash': row._mapping.get('transaction_hash'),
                    'wallet_address': row._mapping.get('wallet_address'),
                    'timestamp': row._mapping.get('timestamp'),
                    'tx_type': row._mapping.get('tx_type'),
                    'source': row._mapping.get('source'),
                    'base_symbol': row._mapping.get('base_symbol'),
                    'base_address': row._mapping.get('base_address'),
                    'base_type_swap': row._mapping.get('base_type_swap'),
                    'base_ui_change_amount': row._mapping.get('base_ui_change_amount'),
                    'base_nearest_price': row._mapping.get('base_nearest_price'),
                    'quote_symbol': row._mapping.get('quote_symbol'),
                    'quote_address': row._mapping.get('quote_address'),
                    'quote_type_swap': row._mapping.get('quote_type_swap'),
                    'quote_ui_change_amount': row._mapping.get('quote_ui_change_amount'),
                    'quote_nearest_price': row._mapping.get('quote_nearest_price')
                }
                transactions.append(tx_dict)
            
            # Found transactions for wallet
            return transactions
            
        except Exception as e:
            self.logger.error(f"Failed to get transactions for wallet {wallet_address}: {e}")
            raise
        
    def calculate_wallet_pnl(self, wallet_address: str, transactions: List[Any]) -> Dict[str, Any]:
        """Calculate PnL for a wallet using FIFO method."""
        calculator = WalletPnLCalculator(wallet_address)
        
        # Process each transaction
        for tx in transactions:
            calculator.process_transaction(tx)
        
        # Get final metrics
        return calculator.calculate_metrics()
        
    def process_wallet(self, session: Any, whale: Dict[str, Any]) -> Dict[str, Any]:
        """Process PnL for a single wallet."""
        wallet_address = whale['wallet_address']
        tokens_held_count = whale.get('tokens_held_count', 0)
        
        try:
            # Create/update state for this wallet
            self.state_manager.create_or_update_state(
                task_name=self.task_name,
                entity_type="wallet",
                entity_id=wallet_address,
                state="processing",
                metadata={"tokens_held": tokens_held_count}
            )
            
            # Get all transactions for this wallet
            transactions = self.get_wallet_transactions(session, wallet_address)
            
            if len(transactions) < self.config.min_trades_for_calculation:
                # Skipping wallet - insufficient transactions
                self.logger.info(f"Skipping wallet {wallet_address}: only {len(transactions)} transactions (minimum: {self.config.min_trades_for_calculation})")
                
                self.state_manager.create_or_update_state(
                    task_name=self.task_name,
                    entity_type="wallet",
                    entity_id=wallet_address,
                    state="skipped",
                    metadata={"skip_reason": "insufficient_trades", "transaction_count": len(transactions)}
                )
                
                # Don't mark whale as completed if skipped - only mark completed when PnL is actually calculated
                # self._mark_whale_completed(session, wallet_address)
                return None
            
            # Calculate PnL using FIFO method
            # Calculating PnL
            
            metrics = self.calculate_wallet_pnl(wallet_address, transactions)
            
            # Only include if calculation method matches config (allow fifo methods)
            if self.config.calculation_method not in ['fifo', 'fifo_enhanced', 'fifo_cost_basis_matched', 'fifo_cost_basis_matched_min50']:
                self.logger.warning(f"Skipping wallet {wallet_address}: calculation method {self.config.calculation_method} not in allowed list")
                return None
            
            # Mark wallet state as completed (but don't mark whale as completed yet)
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
            
            # Don't mark whale as completed here - will do after successful DB insert
            # self._mark_whale_completed(session, wallet_address)
            
            # Calculated PnL
            
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
    
    def _mark_whale_completed(self, session: Any, wallet_address: str):
        """Mark whale as completed in silver_whales table."""
        update_query = text("""
            UPDATE silver.silver_whales 
            SET pnl_processed = true, pnl_processed_at = :timestamp 
            WHERE wallet_address = :wallet_address
        """)
        session.execute(update_query, {
            "timestamp": datetime.utcnow(), 
            "wallet_address": wallet_address
        })
        session.commit()
            
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
                wallet_addresses_to_mark = []  # Track which wallets to mark as completed
                
                for whale in whales_to_process:
                    try:
                        metrics = self.process_wallet(session, whale)
                        if metrics:  # Skip if insufficient trades or wrong calculation method
                            pnl_records.append(metrics)
                            wallet_addresses_to_mark.append(whale['wallet_address'])
                        self.processed_entities += 1
                        
                    except Exception as e:
                        # Error already logged in process_wallet
                        continue
                
                # Store PnL records if any were processed
                successfully_stored_wallets = []
                if pnl_records:
                    self.logger.info(f"Storing {len(pnl_records)} PnL records to database")
                    df = pd.DataFrame(pnl_records)
                    
                    try:
                        upsert_result = self.upsert_records(
                            session=session,
                            df=df,
                            model_class=SilverWalletPnL,
                            conflict_columns=["wallet_address"],
                            batch_size=self.config.batch_size
                        )
                        
                        self.new_records = upsert_result.get('inserted', 0)
                        self.updated_records = upsert_result.get('updated', 0)
                        
                        # Only mark wallets as completed after successful DB insert
                        for wallet_address in wallet_addresses_to_mark:
                            try:
                                self._mark_whale_completed(session, wallet_address)
                                successfully_stored_wallets.append(wallet_address)
                            except Exception as e:
                                self.logger.error(f"Failed to mark whale {wallet_address} as completed: {e}")
                        
                        self.logger.info(f"Successfully stored {self.new_records + self.updated_records} PnL records and marked {len(successfully_stored_wallets)} whales as completed")
                        
                    except Exception as e:
                        self.logger.error(f"Failed to store PnL records: {e}")
                        # Don't mark any whales as completed if DB insert failed
                        raise
            
            # Log task completion
            status = "completed" if not self.failed_entities else "partial_success"
            
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