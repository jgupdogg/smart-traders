import os
import logging
import httpx
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class HeliusWebhookManager:
    """Manages Helius webhook registration and updates."""
    
    def __init__(self):
        self.api_key = os.getenv("HELIUS_API_KEY")
        if not self.api_key:
            raise ValueError("HELIUS_API_KEY is not set")
        
        self.base_url = "https://api.helius.xyz/v0"
        self.headers = {
            "Content-Type": "application/json"
        }
        
        # Configuration from environment
        self.transaction_types = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP").split(",")
        self.transaction_types = [t.strip() for t in self.transaction_types]
        self.webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
        self.auth_header = os.getenv("HELIUS_AUTH_HEADER", "")
    
    def get_addresses(self) -> List[str]:
        """
        Get addresses to monitor from gold.top_traders in MinIO.
        Falls back to environment variable if gold layer unavailable.
        """
        try:
            # Try to fetch from gold layer first
            gold_addresses = self._fetch_from_gold_layer()
            if gold_addresses:
                logger.info(f"Fetched {len(gold_addresses)} addresses from gold layer")
                return gold_addresses
        except Exception as e:
            logger.warning(f"Failed to fetch from gold layer: {e}")
        
        # Fallback to environment variable
        logger.info("Falling back to environment variable addresses")
        addresses_env = os.getenv("HELIUS_ADDRESSES", "")
        if addresses_env:
            return [addr.strip() for addr in addresses_env.split(",") if addr.strip()]
        
        # Default empty list
        logger.warning("No addresses found in gold layer or environment")
        return []
    
    def _fetch_from_gold_layer(self) -> List[str]:
        """Fetch top trader addresses from gold layer in PostgreSQL using same criteria as smart traders task."""
        try:
            import psycopg2
        except ImportError:
            logger.error("psycopg2 not available for database connection")
            raise ImportError("psycopg2 required for database connection")
        
        # Get database configuration from environment
        db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'solana-smart-traders'),
            'user': os.getenv('POSTGRES_USER', 'trader'),
            'password': os.getenv('POSTGRES_PASSWORD', 'trader_password')
        }
        
        conn = None
        try:
            # Connect to PostgreSQL
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Query top 100 smart traders using same criteria as smart traders task
            # This matches the selection logic in smart_traders.py
            query = """
            SELECT wallet_address 
            FROM gold.smart_traders 
            WHERE wallet_address IS NOT NULL
              AND wallet_address != ''
            ORDER BY composite_score DESC, total_realized_pnl_usd DESC
            LIMIT 100
            """
            
            cursor.execute(query)
            result = cursor.fetchall()
            addresses = [row[0] for row in result if row[0]]
            
            logger.info(f"Successfully fetched {len(addresses)} addresses from gold layer")
            return addresses
            
        except Exception as e:
            logger.error(f"Error querying gold layer: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    async def get_all_webhooks(self) -> List[Dict]:
        """Get all existing webhooks."""
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/webhooks?api-key={self.api_key}",
                    headers=self.headers
                )
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Error fetching webhooks: {e}")
                return []
    
    async def create_webhook(self, webhook_url: str) -> Optional[Dict]:
        """Create a new webhook."""
        addresses = self.get_addresses()
        if not addresses:
            logger.warning("No addresses configured for webhook monitoring")
            return None
        
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": self.transaction_types,
            "accountAddresses": addresses,
            "webhookType": self.webhook_type
        }
        
        if self.auth_header:
            payload["authHeader"] = self.auth_header
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}/webhooks?api-key={self.api_key}",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Webhook created successfully: {result}")
                return result
            except Exception as e:
                logger.error(f"Error creating webhook: {e}")
                return None
    
    async def edit_webhook(self, webhook_id: str, webhook_url: str) -> Optional[Dict]:
        """Update an existing webhook."""
        addresses = self.get_addresses()
        if not addresses:
            logger.warning("No addresses configured for webhook monitoring")
            return None
        
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": self.transaction_types,
            "accountAddresses": addresses,
            "webhookType": self.webhook_type
        }
        
        if self.auth_header:
            payload["authHeader"] = self.auth_header
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.put(
                    f"{self.base_url}/webhooks/{webhook_id}?api-key={self.api_key}",
                    headers=self.headers,
                    json=payload
                )
                response.raise_for_status()
                result = response.json()
                logger.info(f"Webhook updated successfully: {result}")
                return result
            except Exception as e:
                logger.error(f"Error updating webhook: {e}")
                return None
    
    async def update_or_create_webhook(self, webhook_url: str) -> bool:
        """Update existing webhook or create new one."""
        try:
            # Get existing webhooks
            existing_webhooks = await self.get_all_webhooks()
            
            if existing_webhooks and len(existing_webhooks) > 0:
                # Update the first webhook
                webhook_id = existing_webhooks[0].get("webhookID")
                if webhook_id:
                    logger.info(f"Updating existing webhook {webhook_id}")
                    result = await self.edit_webhook(webhook_id, webhook_url)
                    return result is not None
                else:
                    logger.warning("Webhook found but no ID, creating new one")
                    result = await self.create_webhook(webhook_url)
                    return result is not None
            else:
                # Create new webhook
                logger.info("No existing webhooks, creating new one")
                result = await self.create_webhook(webhook_url)
                return result is not None
                
        except Exception as e:
            logger.error(f"Error in update_or_create_webhook: {e}")
            return False