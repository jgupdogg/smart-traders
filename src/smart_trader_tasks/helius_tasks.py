"""
Helius Integration Tasks

Core business logic for updating Helius webhooks with top trader addresses.
Extracted from webhook DAGs for use in the smart trader identification pipeline.
"""
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from io import BytesIO

import pandas as pd
import requests
from sqlalchemy import select, text
from src.database.connection import get_db_session
from src.models.gold import SmartTrader
from src.config.settings import get_settings

# Helius configuration constants
HELIUS_API_BASE_URL = "https://api.helius.xyz/v0"
HELIUS_MAX_ADDRESSES = 500  # Limit to top 500 traders to avoid spam
HELIUS_MAX_TRADE_FREQUENCY = 10.0  # Max trades per day to avoid high-frequency traders
HELIUS_WEBHOOK_TYPE = "enhanced"
HELIUS_TRANSACTION_TYPES = ["SWAP", "TRANSFER"]
HELIUS_REQUEST_TIMEOUT = 30
HELIUS_TIER_PRIORITY = {
    "ELITE": 4,
    "STRONG": 3,
    "PROMISING": 2,
    "QUALIFIED": 1
}




def read_latest_gold_traders() -> List[Dict[str, Any]]:
    """Read the latest gold smart traders using database connection"""
    logger = logging.getLogger(__name__)
    
    try:
        with get_db_session() as session:
            # Query smart traders ordered by performance tier and PnL
            query = text("""
                SELECT 
                    wallet_address,
                    performance_tier,
                    total_realized_pnl_usd as total_pnl,
                    win_rate_percent as win_rate,
                    total_trades as trade_count,
                    roi_percentage as roi,
                    trade_frequency_per_day
                FROM gold.smart_traders
                WHERE is_active = true
                    AND (trade_frequency_per_day IS NULL OR trade_frequency_per_day <= :max_frequency)
                ORDER BY 
                    CASE performance_tier
                        WHEN 'ELITE' THEN 4
                        WHEN 'STRONG' THEN 3 
                        WHEN 'PROMISING' THEN 2
                        WHEN 'QUALIFIED' THEN 1
                        ELSE 0
                    END DESC,
                    total_realized_pnl_usd DESC
                LIMIT :max_addresses
            """)
            
            result = session.execute(query, {
                "max_frequency": HELIUS_MAX_TRADE_FREQUENCY,
                "max_addresses": HELIUS_MAX_ADDRESSES
            })
            rows = result.fetchall()
            
            if not rows:
                logger.warning("No gold smart traders found in database")
                return []
            
            # Convert to list of dictionaries
            all_traders = []
            for row in rows:
                trader_dict = {
                    'wallet_address': row._mapping.get('wallet_address'),
                    'performance_tier': row._mapping.get('performance_tier'),
                    'total_pnl': float(row._mapping.get('total_pnl')) if row._mapping.get('total_pnl') is not None else 0.0,
                    'win_rate': float(row._mapping.get('win_rate')) if row._mapping.get('win_rate') is not None else 0.0,
                    'trade_count': int(row._mapping.get('trade_count')) if row._mapping.get('trade_count') is not None else 0,
                    'roi': float(row._mapping.get('roi')) if row._mapping.get('roi') is not None else 0.0
                }
                all_traders.append(trader_dict)
            
            logger.info(f"✅ Read {len(all_traders)} gold traders from database")
            logger.info(f"Filtered: trade_frequency <= {HELIUS_MAX_TRADE_FREQUENCY} trades/day, top {HELIUS_MAX_ADDRESSES} traders")
            
            # Log performance tier breakdown
            tier_counts = {}
            for trader in all_traders:
                tier = trader.get('performance_tier', 'UNKNOWN')
                tier_counts[tier] = tier_counts.get(tier, 0) + 1
            logger.info(f"Performance tier breakdown: {tier_counts}")
            
            return all_traders
            
    except Exception as e:
        logger.error(f"Failed to read gold traders from database: {e}")
        return []


def get_helius_api_key() -> str:
    """Get Helius API key from Airflow Variables or environment variables"""
    logger = logging.getLogger(__name__)
    
    # First try Airflow Variables
    api_key = None
    try:
        from airflow.models import Variable
        api_key = Variable.get('HELIUS_API_KEY')
        logger.info("Retrieved API key from Airflow Variables")
    except:
        # Fallback to environment variable
        api_key = os.environ.get('HELIUS_API_KEY')
        if api_key:
            logger.info("Retrieved API key from environment variables")
    
    if not api_key:
        raise ValueError("HELIUS_API_KEY not found in Airflow Variables or environment variables")
    
    # Clean the API key (remove whitespace)
    api_key = api_key.strip()
    
    if not api_key:
        raise ValueError("HELIUS_API_KEY is empty after cleaning")
    
    # Log masked API key for debugging
    masked_key = f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    logger.info(f"Using API key: {masked_key}")
    
    return api_key


def get_request_headers(api_key: str) -> Dict[str, str]:
    """Get consistent request headers for Helius API calls"""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }


def list_all_webhooks(api_key: str) -> List[Dict[str, Any]]:
    """List all webhooks in the Helius account"""
    logger = logging.getLogger(__name__)
    
    try:
        # Try multiple authorization methods
        headers = get_request_headers(api_key)
        
        # First try with Bearer token
        response = requests.get(
            f"{HELIUS_API_BASE_URL}/webhooks",
            headers=headers,
            timeout=HELIUS_REQUEST_TIMEOUT
        )
        
        # If Bearer token fails, try with query parameter
        if response.status_code == 401:
            logger.warning("Bearer token failed, trying query parameter")
            headers = {"Content-Type": "application/json"}
            response = requests.get(
                f"{HELIUS_API_BASE_URL}/webhooks?api-key={api_key}",
                headers=headers,
                timeout=HELIUS_REQUEST_TIMEOUT
            )
        
        response.raise_for_status()
        
        webhooks = response.json()
        logger.info(f"Found {len(webhooks)} webhooks in Helius account")
        
        for webhook in webhooks:
            logger.info(f"Webhook ID: {webhook.get('webhookID')}, URL: {webhook.get('webhookURL')}")
            
        return webhooks
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.error("401 Unauthorized - API key is invalid or does not have permission to access webhooks")
        logger.error(f"HTTP Error listing webhooks from Helius: {e}")
        return []
    except Exception as e:
        logger.error(f"Error listing webhooks from Helius: {e}")
        return []


def get_current_webhook(api_key: str) -> Optional[Dict[str, Any]]:
    """Get current webhook configuration from Helius"""
    logger = logging.getLogger(__name__)
    
    
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.get(
            f"{HELIUS_API_BASE_URL}/webhooks?api-key={api_key}",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        webhooks = response.json()
        if not webhooks:
            logger.warning("No webhooks found in Helius account")
            return None
            
        # Use the first webhook
        webhook = webhooks[0]
        logger.info(f"Found webhook: {webhook.get('webhookID')}")
        return webhook
        
    except Exception as e:
        logger.error(f"Error fetching webhook from Helius: {e}")
        return None


def create_webhook(api_key: str, webhook_url: str, addresses: List[str]) -> Optional[str]:
    """Create a new webhook in Helius"""
    logger = logging.getLogger(__name__)
    
    try:
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": HELIUS_TRANSACTION_TYPES,
            "accountAddresses": addresses,
            "webhookType": HELIUS_WEBHOOK_TYPE
        }
        
        # Try multiple authorization methods
        headers = get_request_headers(api_key)
        
        # First try with Bearer token
        response = requests.post(
            f"{HELIUS_API_BASE_URL}/webhooks",
            headers=headers,
            json=payload,
            timeout=HELIUS_REQUEST_TIMEOUT
        )
        
        # If Bearer token fails, try with query parameter
        if response.status_code == 401:
            logger.warning("Bearer token failed, trying query parameter")
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                f"{HELIUS_API_BASE_URL}/webhooks?api-key={api_key}",
                headers=headers,
                json=payload,
                timeout=HELIUS_REQUEST_TIMEOUT
            )
        
        response.raise_for_status()
        
        result = response.json()
        webhook_id = result.get('webhookID')
        logger.info(f"Created new webhook: {webhook_id}")
        return webhook_id
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.error("401 Unauthorized - API key is invalid or does not have permission to create webhooks")
        logger.error(f"HTTP Error creating webhook: {e}")
        return None
    except Exception as e:
        logger.error(f"Error creating webhook: {e}")
        return None


def update_webhook(api_key: str, webhook_id: str, webhook_url: str, addresses: List[str]) -> bool:
    """Update existing webhook with new addresses"""
    logger = logging.getLogger(__name__)
    
    try:
        payload = {
            "webhookURL": webhook_url,
            "transactionTypes": HELIUS_TRANSACTION_TYPES,
            "accountAddresses": addresses,
            "webhookType": HELIUS_WEBHOOK_TYPE
        }
        
        # Try multiple authorization methods
        headers = get_request_headers(api_key)
        
        # First try with Bearer token
        response = requests.put(
            f"{HELIUS_API_BASE_URL}/webhooks/{webhook_id}",
            headers=headers,
            json=payload,
            timeout=HELIUS_REQUEST_TIMEOUT
        )
        
        # If Bearer token fails, try with query parameter
        if response.status_code == 401:
            logger.warning("Bearer token failed, trying query parameter")
            headers = {"Content-Type": "application/json"}
            response = requests.put(
                f"{HELIUS_API_BASE_URL}/webhooks/{webhook_id}?api-key={api_key}",
                headers=headers,
                json=payload,
                timeout=HELIUS_REQUEST_TIMEOUT
            )
        
        response.raise_for_status()
        
        logger.info(f"Successfully updated webhook {webhook_id}")
        return True
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.error("401 Unauthorized - API key is invalid or does not have permission to update webhooks")
        elif e.response.status_code == 404:
            logger.error(f"404 Not Found - Webhook {webhook_id} does not exist")
        logger.error(f"HTTP Error updating webhook: {e}")
        return False
    except Exception as e:
        logger.error(f"Error updating webhook: {e}")
        return False


def update_helius_webhook(**context):
    """
    Update Helius webhook with addresses from gold top traders
    
    This is the main task function called by the DAG.
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Step 1: Read latest gold traders
        logger.info("Reading latest gold top traders...")
        gold_traders = read_latest_gold_traders()
        
        if not gold_traders:
            logger.warning("No gold traders found, skipping webhook update")
            return {
                "status": "skipped",
                "reason": "no_gold_traders",
                "timestamp": datetime.utcnow().isoformat()
            }
        
        # Extract unique wallet addresses
        wallet_addresses = list(set(
            trader['wallet_address'] 
            for trader in gold_traders 
            if trader.get('wallet_address')
        ))
        
        # Already limited by database query, but double-check
        if len(wallet_addresses) > HELIUS_MAX_ADDRESSES:
            logger.warning(f"Too many addresses! Limiting from {len(wallet_addresses)} to {HELIUS_MAX_ADDRESSES}")
            wallet_addresses = wallet_addresses[:HELIUS_MAX_ADDRESSES]
        
        logger.info(f"Selected {len(wallet_addresses)} high-quality traders (max {HELIUS_MAX_TRADE_FREQUENCY} trades/day)")
        
        logger.info(f"Found {len(wallet_addresses)} unique wallet addresses to monitor")
        
        # Step 2: Get Helius API key and webhook ID
        api_key = get_helius_api_key()
        
        # Step 2.5: Test API key by listing webhooks
        logger.info("Testing API key by listing all webhooks...")
        all_webhooks = list_all_webhooks(api_key)
        if not all_webhooks:
            logger.error("API key test failed - could not retrieve webhooks")
            return {
                "status": "failed",
                "reason": "api_key_invalid",
                "timestamp": datetime.utcnow().isoformat(),
                "message": "API key is invalid or does not have permission to access webhooks"
            }
        
        logger.info(f"API key test passed - found {len(all_webhooks)} webhooks")
        
        # Get webhook ID from Airflow Variables or environment variables
        try:
            from airflow.models import Variable
            webhook_id = Variable.get('HELIUS_WEBHOOK_ID')
        except:
            # Fallback to environment variable
            webhook_id = os.environ.get('HELIUS_WEBHOOK_ID')
            if not webhook_id:
                logger.error("HELIUS_WEBHOOK_ID not found in Airflow Variables or environment variables")
                return {
                    "status": "failed",
                    "reason": "no_webhook_id",
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        # Clean the webhook ID (remove any whitespace/tabs)
        webhook_id = webhook_id.strip()
        
        logger.info(f"Using existing webhook ID: {webhook_id}")
        
        # Step 3: Get current webhook URL (single API call for specific webhook)
        try:
            # Try multiple authorization methods
            headers = get_request_headers(api_key)
            
            # First try with Bearer token
            response = requests.get(
                f"{HELIUS_API_BASE_URL}/webhooks/{webhook_id}",
                headers=headers,
                timeout=HELIUS_REQUEST_TIMEOUT
            )
            
            # If Bearer token fails, try with query parameter
            if response.status_code == 401:
                logger.warning("Bearer token failed for webhook fetch, trying query parameter")
                headers = {"Content-Type": "application/json"}
                response = requests.get(
                    f"{HELIUS_API_BASE_URL}/webhooks/{webhook_id}?api-key={api_key}",
                    headers=headers,
                    timeout=HELIUS_REQUEST_TIMEOUT
                )
            
            response.raise_for_status()
            
            current_webhook = response.json()
            webhook_url = current_webhook.get('webhookURL')
            current_addresses = current_webhook.get('accountAddresses', [])
            
            logger.info(f"Current webhook has {len(current_addresses)} addresses")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("401 Unauthorized - API key is invalid or does not have permission to access this webhook")
                logger.error("Available webhooks in your account:")
                for webhook in all_webhooks:
                    logger.error(f"  - {webhook.get('webhookID')}: {webhook.get('webhookURL')}")
                return {
                    "status": "failed",
                    "reason": "unauthorized_webhook_access",
                    "webhook_id": webhook_id,
                    "available_webhooks": [w.get('webhookID') for w in all_webhooks],
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": f"API key does not have permission to access webhook {webhook_id}"
                }
            elif e.response.status_code == 404:
                logger.error(f"Webhook {webhook_id} not found in Helius account")
                logger.error("Available webhooks in your account:")
                for webhook in all_webhooks:
                    logger.error(f"  - {webhook.get('webhookID')}: {webhook.get('webhookURL')}")
                
                return {
                    "status": "failed",
                    "reason": "webhook_not_found",
                    "webhook_id": webhook_id,
                    "available_webhooks": [w.get('webhookID') for w in all_webhooks],
                    "timestamp": datetime.utcnow().isoformat(),
                    "message": f"Webhook {webhook_id} does not exist in Helius account"
                }
            else:
                raise e
        except Exception as e:
            logger.error(f"Error fetching webhook details: {e}")
            # Fallback: try to get URL from environment or use placeholder
            webhook_url = os.environ.get('HELIUS_WEBHOOK_URL', 'https://webhook-placeholder.com')
            logger.warning(f"Using fallback webhook URL: {webhook_url}")
            current_addresses = []
        
        # Step 4: Update webhook with new addresses
        success = update_webhook(api_key, webhook_id, webhook_url, wallet_addresses)
        
        if not success:
            raise Exception("Failed to update webhook addresses")
        
        # Log webhook update success
        logger.info(f"Webhook {webhook_id} updated with {len(wallet_addresses)} addresses")
        logger.info(f"Last update: {datetime.utcnow().isoformat()}")
        
        # Get performance tier breakdown
        tier_breakdown = {}
        for trader in gold_traders[:len(wallet_addresses)]:
            tier = trader.get('performance_tier', 'unknown')
            tier_breakdown[tier] = tier_breakdown.get(tier, 0) + 1
        
        logger.info(f"✅ Successfully updated Helius webhook with {len(wallet_addresses)} addresses")
        logger.info(f"Performance tier breakdown: {tier_breakdown}")
        
        return {
            "status": "success",
            "webhook_id": webhook_id,
            "addresses_count": len(wallet_addresses),
            "performance_tiers": tier_breakdown,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in Helius webhook update: {e}")
        logger.error(f"Error time: {datetime.utcnow().isoformat()}")
        
        # Return error info but don't raise - this is non-critical
        return {
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }