"""
Webhook updater service to sync webhook addresses with latest smart traders.
This ensures webhook addresses are always up-to-date with the latest DAG results.
"""

import logging
import asyncio
import os
from typing import List, Optional
import psycopg2
import sys
sys.path.append('/opt/airflow/src/services/webhook-listener')
from helius_helper import HeliusWebhookManager

logger = logging.getLogger(__name__)


class WebhookAddressUpdater:
    """Updates webhook addresses to match latest smart traders from gold layer."""
    
    def __init__(self):
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': int(os.getenv('POSTGRES_PORT', '5432')),
            'database': os.getenv('POSTGRES_DB', 'solana-smart-traders'),
            'user': os.getenv('POSTGRES_USER', 'trader'),
            'password': os.getenv('POSTGRES_PASSWORD', 'trader_password')
        }
        self.webhook_url = os.getenv('WEBHOOK_BASE_URL', 'https://e8c38011b3fb.ngrok.app/webhooks')
        
    def get_current_smart_traders(self) -> List[str]:
        """Get current top 100 smart traders from gold layer."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Query top 100 smart traders using enhanced tier scoring
            query = """
            SELECT wallet_address 
            FROM gold.smart_traders 
            WHERE wallet_address IS NOT NULL
              AND wallet_address != ''
            ORDER BY 
                CASE performance_tier 
                    WHEN 'ELITE' THEN 1 
                    WHEN 'STRONG' THEN 2 
                    WHEN 'PROMISING' THEN 3 
                    WHEN 'QUALIFIED' THEN 4 
                    WHEN 'EXPERIMENTAL' THEN 5 
                    ELSE 6 
                END,
                tier_score DESC, 
                total_realized_pnl_usd DESC
            LIMIT 100
            """
            
            cursor.execute(query)
            result = cursor.fetchall()
            addresses = [row[0] for row in result if row[0]]
            
            logger.info(f"Retrieved {len(addresses)} smart trader addresses from gold layer")
            return addresses
            
        except Exception as e:
            logger.error(f"Error querying smart traders: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    async def update_webhook_addresses(self, webhook_url: Optional[str] = None) -> bool:
        """Update webhook addresses with latest smart traders."""
        try:
            # Get current smart traders
            addresses = self.get_current_smart_traders()
            
            if not addresses:
                logger.warning("No smart trader addresses found")
                return False
            
            # Use provided webhook URL or default
            url = webhook_url or self.webhook_url
            if not url:
                logger.error("No webhook URL provided")
                return False
                
            # Update webhook with Helius
            logger.info(f"Updating webhook with {len(addresses)} addresses")
            
            # Set Helius API key from Airflow variables if not in environment
            if not os.getenv('HELIUS_API_KEY'):
                try:
                    from airflow.models import Variable
                    helius_key = Variable.get('HELIUS_API_KEY')
                    os.environ['HELIUS_API_KEY'] = helius_key
                except Exception as e:
                    logger.error(f"Failed to get Helius API key from Airflow variables: {e}")
                    
            helius = HeliusWebhookManager()
            
            # Override the addresses method to use our current list
            helius.get_addresses = lambda: addresses
            
            success = await helius.update_or_create_webhook(url)
            
            if success:
                logger.info("Successfully updated webhook addresses with latest smart traders")
                return True
            else:
                logger.error("Failed to update webhook addresses")
                return False
                
        except Exception as e:
            logger.error(f"Error updating webhook addresses: {e}")
            return False


async def update_webhook_after_dag_run(webhook_url: Optional[str] = None) -> bool:
    """
    Function to be called after smart traders DAG completes.
    Updates webhook addresses to match latest smart traders.
    
    Args:
        webhook_url: Optional webhook URL, will auto-detect if not provided
        
    Returns:
        bool: True if update was successful, False otherwise
    """
    logger.info("Starting webhook address update after DAG run")
    
    try:
        updater = WebhookAddressUpdater()
        success = await updater.update_webhook_addresses(webhook_url)
        
        if success:
            logger.info("Webhook addresses successfully updated after DAG run")
        else:
            logger.error("Failed to update webhook addresses after DAG run")
            
        return success
        
    except Exception as e:
        logger.error(f"Error in webhook update after DAG run: {e}")
        return False


def sync_update_webhook_after_dag_run(webhook_url: Optional[str] = None) -> bool:
    """
    Synchronous wrapper for updating webhook addresses after DAG run.
    Can be called from Airflow tasks.
    """
    return asyncio.run(update_webhook_after_dag_run(webhook_url))


if __name__ == "__main__":
    # Test the webhook updater
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(update_webhook_after_dag_run())
    print(f"Update result: {result}")