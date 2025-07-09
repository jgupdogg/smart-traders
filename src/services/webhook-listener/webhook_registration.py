"""
Webhook registration utilities for automatic Helius webhook setup.
"""

import os
import asyncio
import logging
from pathlib import Path
from typing import Optional
import httpx
from helius_helper import HeliusWebhookManager

logger = logging.getLogger(__name__)


async def get_webhook_url() -> Optional[str]:
    """
    Get the webhook URL from environment or ngrok.
    
    Returns:
        The webhook URL to register with Helius, or None if not available.
    """
    # Try environment variable first (for production)
    webhook_base_url = os.getenv("WEBHOOK_BASE_URL")
    if webhook_base_url:
        # Remove trailing slash and add webhook path
        webhook_url = webhook_base_url.rstrip("/") + "/webhooks"
        logger.info(f"Using webhook URL from environment: {webhook_url}")
        return webhook_url
    
    # Try ngrok (for development)
    try:
        ngrok_url = await get_ngrok_url()
        if ngrok_url:
            logger.info(f"Using ngrok webhook URL: {ngrok_url}")
            return ngrok_url
    except Exception as e:
        logger.warning(f"Failed to get ngrok URL: {e}")
    
    # Try localhost as fallback (for local development)
    localhost_url = "http://localhost:8000/webhooks"
    logger.warning(f"Using localhost fallback: {localhost_url}")
    return localhost_url


async def get_ngrok_url(max_retries: int = 10, retry_delay: int = 1) -> Optional[str]:
    """
    Get the public URL from ngrok API.
    
    Args:
        max_retries: Maximum number of retries to get ngrok URL
        retry_delay: Delay between retries in seconds
        
    Returns:
        The ngrok webhook URL or None if not available
    """
    # Use ngrok container name when running in Docker
    ngrok_host = os.getenv("NGROK_HOST", "ngrok")
    ngrok_api_url = f"http://{ngrok_host}:4040/api/tunnels"
    
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(ngrok_api_url)
                response.raise_for_status()
                data = response.json()
                
                # Find the HTTPS tunnel
                for tunnel in data.get("tunnels", []):
                    if tunnel.get("proto") == "https":
                        public_url = tunnel.get("public_url")
                        if public_url:
                            return public_url + "/webhooks"
                
                logger.debug(f"No HTTPS tunnel found in ngrok response (attempt {attempt + 1}/{max_retries})")
                
        except Exception as e:
            logger.debug(f"Waiting for ngrok to start... (attempt {attempt + 1}/{max_retries}): {e}")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
    
    return None


async def register_webhook_on_startup():
    """
    Register webhook with Helius on FastAPI startup.
    
    This function is called during FastAPI startup event.
    """
    try:
        # Skip if no API key
        if not os.getenv("HELIUS_API_KEY"):
            logger.warning("HELIUS_API_KEY not set, skipping webhook registration")
            return False
        
        # Get webhook URL
        webhook_url = await get_webhook_url()
        if not webhook_url:
            logger.error("Could not determine webhook URL")
            return False
        
        # Register with Helius
        logger.info("Registering webhook with Helius...")
        helius = HeliusWebhookManager()
        success = await helius.update_or_create_webhook(webhook_url)
        
        if success:
            logger.info(f"Successfully registered webhook URL: {webhook_url}")
            
            # Write URL to file for reference
            data_dir = Path("/app/data")
            data_dir.mkdir(parents=True, exist_ok=True)
            
            with open(data_dir / "current_webhook_url.txt", "w") as f:
                f.write(webhook_url)
                
            return True
        else:
            logger.error("Failed to register webhook with Helius")
            return False
        
    except Exception as e:
        logger.error(f"Error during webhook registration: {e}")
        return False


async def test_webhook_registration():
    """
    Test function to verify webhook registration works.
    """
    logger.info("Testing webhook registration...")
    success = await register_webhook_on_startup()
    
    if success:
        logger.info("✅ Webhook registration test passed!")
    else:
        logger.error("❌ Webhook registration test failed!")
    
    return success


if __name__ == "__main__":
    # Allow running this module directly for testing
    asyncio.run(test_webhook_registration())