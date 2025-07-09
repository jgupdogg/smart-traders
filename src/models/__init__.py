"""
SQLModel database models for the Solana Smart Traders pipeline.
"""

from .bronze import BronzeToken, BronzeWhale, BronzeTransaction
from .bronze_webhook import BronzeWebhookTransaction
from .silver import SilverToken, SilverWhale, SilverWalletPnL
from .gold import SmartTrader
from .state import EntityProcessingState

__all__ = [
    # Bronze layer
    "BronzeToken",
    "BronzeWhale", 
    "BronzeTransaction",
    "BronzeWebhookTransaction",
    # Silver layer
    "SilverToken",
    "SilverWhale",
    "SilverWalletPnL",
    # Gold layer
    "SmartTrader",
    # Pipeline management
    "EntityProcessingState"
]