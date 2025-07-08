"""
BirdEye API client adapted from existing codebase.
Provides access to token data, holder information, and transaction data.
"""

import time
import logging
import requests
from typing import Dict, Any, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class BirdEyeAPIClient:
    """BirdEye API client for Solana token data."""
    
    BASE_URL = "https://public-api.birdeye.so"
    
    def __init__(self, api_key: str, chain: str = "solana"):
        """Initialize BirdEye API client.
        
        Args:
            api_key: BirdEye API key
            chain: Blockchain to query (default: solana)
        """
        self.api_key = api_key
        self.chain = chain
        self.headers = {
            "accept": "application/json",
            "X-API-KEY": self.api_key,
            "x-chain": self.chain
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make HTTP request to BirdEye API with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data
        """
        url = f"{self.BASE_URL}{endpoint}"
        
        try:
            response = self.session.get(url, params=params or {})
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API-level errors
            if not data.get('success', True):
                logger.warning(f"API returned success=false for {endpoint}: {data}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {endpoint}: {e}")
            raise
    
    def get_token_list(
        self,
        sort_by: str = "liquidity",
        sort_type: str = "desc",
        offset: int = 0,
        limit: int = 50,
        min_liquidity: Optional[float] = None,
        max_liquidity: Optional[float] = None,
        min_volume_1h_usd: Optional[float] = None,
        min_price_change_2h_percent: Optional[float] = None,
        min_price_change_24h_percent: Optional[float] = None
    ) -> Dict[str, Any]:
        """Get list of tokens with filtering options.
        
        Args:
            sort_by: Field to sort by (liquidity, volume_24h, etc.)
            sort_type: Sort direction (asc, desc)
            offset: Number of records to skip
            limit: Maximum number of records to return
            min_liquidity: Minimum liquidity filter
            max_liquidity: Maximum liquidity filter
            min_volume_1h_usd: Minimum 1-hour volume filter
            min_price_change_2h_percent: Minimum 2-hour price change filter
            min_price_change_24h_percent: Minimum 24-hour price change filter
            
        Returns:
            API response with token list
        """
        params = {
            "sort_by": sort_by,
            "sort_type": sort_type,
            "offset": offset,
            "limit": limit
        }
        
        # Add optional filters
        if min_liquidity is not None:
            params["min_liquidity"] = min_liquidity
        if max_liquidity is not None:
            params["max_liquidity"] = max_liquidity
        if min_volume_1h_usd is not None:
            params["min_volume_1h_usd"] = min_volume_1h_usd
        if min_price_change_2h_percent is not None:
            params["min_price_change_2h_percent"] = min_price_change_2h_percent
        if min_price_change_24h_percent is not None:
            params["min_price_change_24h_percent"] = min_price_change_24h_percent
        
        logger.debug(f"Getting token list with params: {params}")
        return self._make_request("/defi/tokenlist", params)
    
    def get_token_top_holders(
        self,
        token_address: str,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Get top holders for a specific token using v3 API.
        
        Args:
            token_address: Token mint address
            offset: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            API response with holder data
        """
        params = {
            "address": token_address,  # Token address to get holders for
            "offset": offset,
            "limit": limit
        }
        
        logger.debug(f"Getting top holders for token {token_address}")
        return self._make_request("/defi/v3/token/holder", params)
    
    def get_token_holders(self, token_address: str, limit: int = 20, offset: int = 0) -> Dict[str, Any]:
        """Alias for get_token_top_holders to match bronze_whales task usage.
        
        Args:
            token_address: Token mint address
            limit: Maximum number of records to return
            offset: Number of records to skip
            
        Returns:
            API response with holder data
        """
        return self.get_token_top_holders(token_address, offset, limit)
    
    def get_wallet_transactions(
        self,
        wallet_address: str,
        limit: int = 100,
        before: Optional[str] = None,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get transaction history for a wallet.
        
        Args:
            wallet_address: Wallet address
            limit: Maximum number of transactions to return
            before: Get transactions before this transaction signature
            after: Get transactions after this transaction signature
            
        Returns:
            API response with transaction data
        """
        params = {
            "wallet": wallet_address,
            "limit": limit
        }
        
        if before:
            params["before"] = before
        if after:
            params["after"] = after
        
        logger.debug(f"Getting transactions for wallet {wallet_address}")
        return self._make_request("/trader/tx", params)
    
    def get_token_price(self, token_address: str) -> Dict[str, Any]:
        """Get current price for a token.
        
        Args:
            token_address: Token mint address
            
        Returns:
            API response with price data
        """
        params = {"address": token_address}
        
        logger.debug(f"Getting price for token {token_address}")
        return self._make_request("/defi/price", params)
    
    def get_token_metadata(self, token_address: str) -> Dict[str, Any]:
        """Get metadata for a token.
        
        Args:
            token_address: Token mint address
            
        Returns:
            API response with token metadata
        """
        params = {"address": token_address}
        
        logger.debug(f"Getting metadata for token {token_address}")
        return self._make_request("/defi/token_meta", params)
    
    # Data normalization methods (adapted from existing codebase)
    
    def normalize_token_list_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize token list response to consistent format.
        
        Args:
            response: Raw API response
            
        Returns:
            List of normalized token dictionaries
        """
        if not response.get('success', True) or 'data' not in response:
            logger.warning("Invalid token list response format")
            return []
        
        data = response['data']
        tokens = data.get('tokens', data.get('items', []))
        
        normalized_tokens = []
        for token in tokens:
            try:
                normalized_token = {
                    "token_address": str(token.get('address', '')),
                    "logo_uri": token.get('logo_uri'),  # API uses logo_uri not logoURI
                    "name": token.get('name'),
                    "symbol": token.get('symbol'),
                    "decimals": token.get('decimals'),
                    "market_cap": float(token.get('market_cap', 0)) if token.get('market_cap') is not None else None,
                    "fdv": float(token.get('fdv', 0)) if token.get('fdv') is not None else None,
                    "liquidity": float(token.get('liquidity', 0)) if token.get('liquidity') is not None else None,
                    "last_trade_unix_time": token.get('last_trade_unix_time'),
                    "volume_1h_usd": float(token.get('volume_1h_usd', 0)) if token.get('volume_1h_usd') is not None else None,
                    "volume_1h_change_percent": float(token.get('volume_1h_change_percent', 0)) if token.get('volume_1h_change_percent') is not None else None,
                    "volume_2h_usd": float(token.get('volume_2h_usd', 0)) if token.get('volume_2h_usd') is not None else None,
                    "volume_2h_change_percent": float(token.get('volume_2h_change_percent', 0)) if token.get('volume_2h_change_percent') is not None else None,
                    "volume_4h_usd": float(token.get('volume_4h_usd', 0)) if token.get('volume_4h_usd') is not None else None,
                    "volume_4h_change_percent": float(token.get('volume_4h_change_percent', 0)) if token.get('volume_4h_change_percent') is not None else None,
                    "volume_8h_usd": float(token.get('volume_8h_usd', 0)) if token.get('volume_8h_usd') is not None else None,
                    "volume_8h_change_percent": float(token.get('volume_8h_change_percent', 0)) if token.get('volume_8h_change_percent') is not None else None,
                    "volume_24h_usd": float(token.get('volume_24h_usd', 0)) if token.get('volume_24h_usd') is not None else None,
                    "volume_24h_change_percent": float(token.get('volume_24h_change_percent', 0)) if token.get('volume_24h_change_percent') is not None else None,
                    "trade_1h_count": token.get('trade_1h_count'),
                    "trade_2h_count": token.get('trade_2h_count'),
                    "trade_4h_count": token.get('trade_4h_count'),
                    "trade_8h_count": token.get('trade_8h_count'),
                    "trade_24h_count": token.get('trade_24h_count'),
                    "price": float(token.get('price', 0)) if token.get('price') is not None else None,
                    "price_change_1h_percent": float(token.get('price_change_1h_percent', 0)) if token.get('price_change_1h_percent') is not None else None,
                    "price_change_2h_percent": float(token.get('price_change_2h_percent', 0)) if token.get('price_change_2h_percent') is not None else None,
                    "price_change_4h_percent": float(token.get('price_change_4h_percent', 0)) if token.get('price_change_4h_percent') is not None else None,
                    "price_change_8h_percent": float(token.get('price_change_8h_percent', 0)) if token.get('price_change_8h_percent') is not None else None,
                    "price_change_24h_percent": float(token.get('price_change_24h_percent', 0)) if token.get('price_change_24h_percent') is not None else None,
                    "holder": token.get('holder'),
                    "recent_listing_time": token.get('recent_listing_time')
                }
                
                # Extract extensions data if present
                extensions = token.get('extensions', {})
                if extensions:
                    normalized_token.update({
                        "coingecko_id": extensions.get('coingecko_id'),
                        "serum_v3_usdc": extensions.get('serum_v3_usdc'),
                        "serum_v3_usdt": extensions.get('serum_v3_usdt'),
                        "website": extensions.get('website'),
                        "telegram": extensions.get('telegram'),
                        "twitter": extensions.get('twitter'),
                        "description": extensions.get('description'),
                        "discord": extensions.get('discord'),
                        "medium": extensions.get('medium')
                    })
                else:
                    # Set defaults for extensions if not present
                    normalized_token.update({
                        "coingecko_id": None,
                        "serum_v3_usdc": None,
                        "serum_v3_usdt": None,
                        "website": None,
                        "telegram": None,
                        "twitter": None,
                        "description": None,
                        "discord": None,
                        "medium": None
                    })
                
                # Only add tokens with valid addresses
                if normalized_token["token_address"]:
                    normalized_tokens.append(normalized_token)
                    
            except Exception as e:
                logger.warning(f"Failed to normalize token data: {e}, token: {token}")
                continue
        
        logger.info(f"Normalized {len(normalized_tokens)} tokens from response")
        return normalized_tokens
    
    def normalize_holders_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize holders response to consistent format for v3 API.
        
        Args:
            response: Raw API response from /defi/v3/token/holder
            
        Returns:
            List of normalized holder dictionaries
        """
        if not response.get('success', True) or 'data' not in response:
            logger.warning(f"Invalid holders response format: {response}")
            return []
        
        data = response['data']
        holders = data.get('items', [])
        
        if not holders:
            logger.warning("No holders found in response")
            return []
        
        normalized_holders = []
        for idx, holder in enumerate(holders):
            try:
                # Map v3 API response format to our internal format
                normalized_holder = {
                    "wallet_address": str(holder.get('owner', '')),
                    "rank": idx + 1,  # Calculate rank based on position
                    "amount": str(holder.get('amount', '0')),
                    "ui_amount": float(holder.get('ui_amount', 0)) if holder.get('ui_amount') is not None else 0.0,
                    "decimals": int(holder.get('decimals', 9)),
                    "mint": str(holder.get('mint', '')),
                    "token_account": str(holder.get('token_account', ''))
                }
                
                # Only add holders with valid wallet addresses
                if normalized_holder["wallet_address"] and normalized_holder["wallet_address"] != '':
                    normalized_holders.append(normalized_holder)
                else:
                    logger.warning(f"Skipping holder with invalid address: {holder}")
                    
            except Exception as e:
                logger.warning(f"Failed to normalize holder data: {e}, holder: {holder}")
                continue
        
        logger.info(f"Normalized {len(normalized_holders)} holders from response")
        return normalized_holders
    
    def normalize_transactions_response(self, response: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Normalize transactions response to consistent format.
        
        Args:
            response: Raw API response
            
        Returns:
            List of normalized transaction dictionaries
        """
        if not response.get('success', True) or 'data' not in response:
            logger.warning("Invalid transactions response format")
            return []
        
        data = response['data']
        transactions = data if isinstance(data, list) else data.get('items', data.get('trades', []))
        
        normalized_transactions = []
        for transaction in transactions:
            try:
                base = transaction.get('base', {})
                quote = transaction.get('quote', {})
                
                normalized_transaction = {
                    "transaction_hash": str(transaction.get('tx_hash', '')),
                    "timestamp": transaction.get('block_unix_time'),
                    "tx_type": transaction.get('tx_type'),
                    "source": transaction.get('source'),
                    "base_symbol": base.get('symbol'),
                    "base_address": base.get('address'),
                    "base_type_swap": base.get('type_swap'),
                    "base_ui_change_amount": float(base.get('ui_change_amount', 0)) if base.get('ui_change_amount') is not None else None,
                    "base_nearest_price": float(base.get('nearest_price', 0)) if base.get('nearest_price') is not None else None,
                    "quote_symbol": quote.get('symbol'),
                    "quote_address": quote.get('address'),
                    "quote_type_swap": quote.get('type_swap'),
                    "quote_ui_change_amount": float(quote.get('ui_change_amount', 0)) if quote.get('ui_change_amount') is not None else None,
                    "quote_nearest_price": float(quote.get('nearest_price', 0)) if quote.get('nearest_price') is not None else None
                }
                
                # Only add transactions with valid hashes
                if normalized_transaction["transaction_hash"]:
                    normalized_transactions.append(normalized_transaction)
                    
            except Exception as e:
                logger.warning(f"Failed to normalize transaction data: {e}, transaction: {transaction}")
                continue
        
        logger.info(f"Normalized {len(normalized_transactions)} transactions from response")
        return normalized_transactions


# Utility functions for rate limiting and batch processing

def rate_limited_call(func, delay: float = 0.5):
    """Decorator to add rate limiting to API calls.
    
    Args:
        func: Function to call
        delay: Delay in seconds between calls
    """
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        time.sleep(delay)
        return result
    return wrapper


def batch_process_with_delay(
    items: List[Any],
    process_func,
    batch_size: int = 10,
    delay_between_batches: float = 1.0
) -> List[Any]:
    """Process items in batches with delays to respect rate limits.
    
    Args:
        items: List of items to process
        process_func: Function to process each item
        batch_size: Number of items per batch
        delay_between_batches: Delay between batches in seconds
        
    Returns:
        List of processed results
    """
    results = []
    
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        batch_results = []
        for item in batch:
            try:
                result = process_func(item)
                batch_results.append(result)
            except Exception as e:
                logger.error(f"Failed to process item {item}: {e}")
                continue
        
        results.extend(batch_results)
        
        # Delay between batches (except for the last batch)
        if i + batch_size < len(items):
            time.sleep(delay_between_batches)
    
    return results