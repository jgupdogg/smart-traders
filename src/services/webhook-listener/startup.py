import os
import sys
import time
import json
import logging
import asyncio
import httpx
from dotenv import load_dotenv
from helius_helper import HeliusWebhookManager

# Load environment variables
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def get_ngrok_url(max_retries: int = 30, retry_delay: int = 2) -> str:
    """
    Get the public URL from ngrok API.
    Retries multiple times as ngrok might take a moment to start.
    """
    # Use ngrok container name when running in Docker
    ngrok_host = os.getenv("NGROK_HOST", "ngrok")
    ngrok_api_url = f"http://{ngrok_host}:4040/api/tunnels"
    
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(ngrok_api_url)
                response.raise_for_status()
                data = response.json()
                
                # Find the HTTPS tunnel
                for tunnel in data.get("tunnels", []):
                    if tunnel.get("proto") == "https":
                        public_url = tunnel.get("public_url")
                        if public_url:
                            return public_url + "/webhooks"
                
                logger.warning(f"No HTTPS tunnel found in ngrok response (attempt {attempt + 1}/{max_retries})")
                
        except (httpx.HTTPError, httpx.ConnectError) as e:
            logger.info(f"Waiting for ngrok to start... (attempt {attempt + 1}/{max_retries})")
        
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
    
    raise RuntimeError("Failed to get ngrok URL after all retries")

async def register_webhook_with_helius():
    """Register the ngrok webhook URL with Helius."""
    try:
        # Skip if no API key
        if not os.getenv("HELIUS_API_KEY"):
            logger.warning("HELIUS_API_KEY not set, skipping webhook registration")
            return False
        
        # Get ngrok URL
        logger.info("Getting ngrok URL...")
        webhook_url = await get_ngrok_url()
        logger.info(f"Got ngrok URL: {webhook_url}")
        
        # Register with Helius
        logger.info("Registering webhook with Helius...")
        helius = HeliusWebhookManager()
        success = await helius.update_or_create_webhook(webhook_url)
        
        if success:
            logger.info(f"Successfully registered webhook URL: {webhook_url}")
            # Write URL to file for reference
            with open("/app/data/current_webhook_url.txt", "w") as f:
                f.write(webhook_url)
        else:
            logger.error("Failed to register webhook with Helius")
        
        return success
        
    except Exception as e:
        logger.error(f"Error during webhook registration: {e}")
        return False

async def main():
    """Main startup function."""
    # Give ngrok a moment to fully start
    await asyncio.sleep(5)
    
    # Register webhook
    success = await register_webhook_with_helius()
    
    if success:
        logger.info("Webhook registration complete!")
    else:
        logger.warning("Webhook registration failed, but service will continue")
    
    # Start the FastAPI app
    logger.info("Starting FastAPI application...")
    os.system("uvicorn main:app --host 0.0.0.0 --port 8000")

if __name__ == "__main__":
    asyncio.run(main())