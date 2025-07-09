import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

# Import models and database utilities
import sys
sys.path.append('/app/src')
from src.models.bronze_webhook import BronzeWebhookTransaction
from webhook_registration import register_webhook_on_startup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Webhook Listener", version="4.0.0")

# Configuration
DATA_DIR = Path(os.getenv("DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)  # Only for URL file storage

# Database configuration
def get_database_url():
    """Get database URL from environment variables."""
    user = os.getenv('POSTGRES_USER', 'trader')
    password = os.getenv('POSTGRES_PASSWORD', 'trader_password')
    host = os.getenv('POSTGRES_HOST', 'postgres')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'solana-smart-traders')
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"

# Create database engine and session
engine = create_engine(
    get_database_url(),
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600
)
SessionLocal = sessionmaker(bind=engine)

class WebhookResponse(BaseModel):
    status: str
    message_id: str
    timestamp: str

@app.on_event("startup")
async def startup_event():
    """Initialize webhook listener and register with Helius."""
    logger.info("FastAPI webhook listener starting with PostgreSQL storage")
    
    # Register webhook with Helius
    try:
        await register_webhook_on_startup()
        logger.info("Webhook registered with Helius successfully")
    except Exception as e:
        logger.error(f"Failed to register webhook with Helius: {e}")
        # Don't fail startup - continue with webhook listener
    
    logger.info("Ready to receive webhooks at /webhooks endpoint")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown of webhook listener."""
    engine.dispose()
    logger.info("Webhook listener shutdown complete")

@app.get("/")
async def root():
    return {
        "message": "Webhook Listener is running with PostgreSQL storage", 
        "version": "4.0.0",
        "mode": "postgresql",
        "storage": "postgres_bronze_layer",
        "table": "bronze.bronze_webhook_transactions"
    }

@app.get("/health")
async def health_check():
    # Test database connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "unhealthy"
    
    return {
        "status": db_status, 
        "timestamp": datetime.utcnow().isoformat(),
        "mode": "postgresql",
        "storage_ready": db_status == "healthy"
    }

@app.post("/webhooks")
async def webhook_listener(request: Request) -> WebhookResponse:
    """
    Receive webhook payloads and store them in PostgreSQL bronze layer.
    """
    try:
        # Get the raw payload
        payload = await request.json()
        
        # Generate unique ID and timestamp
        webhook_id = str(uuid4())
        timestamp = datetime.utcnow()
        
        # Extract transaction details from Helius webhook payload
        transaction_signature = None
        account_address = None
        token_address = None
        webhook_type = "UNKNOWN"
        
        try:
            # Parse Helius webhook structure
            if isinstance(payload, list) and len(payload) > 0:
                first_event = payload[0]
                transaction_signature = first_event.get("signature")
                webhook_type = first_event.get("type", "UNKNOWN")
                
                # Extract account/token from the transaction
                if "accountData" in first_event:
                    account_data = first_event.get("accountData", [])
                    if account_data:
                        account_address = account_data[0].get("account")
                        
                # For token transfers, extract token address
                if "tokenTransfers" in first_event:
                    token_transfers = first_event.get("tokenTransfers", [])
                    if token_transfers:
                        token_address = token_transfers[0].get("mint")
        except Exception as e:
            logger.warning(f"Could not parse Helius webhook structure: {e}")
        
        # Write to PostgreSQL bronze layer
        try:
            # Create webhook record
            webhook_record = BronzeWebhookTransaction(
                webhook_id=webhook_id,
                transaction_signature=transaction_signature,
                webhook_type=webhook_type,
                webhook_timestamp=timestamp,
                account_address=account_address,
                token_address=token_address,
                raw_payload=payload,
                source_ip=request.client.host if request.client else None,
                user_agent=request.headers.get("user-agent"),
                ingested_at=timestamp,
                processing_status="pending"
            )
            
            # Insert into database with UPSERT logic
            with SessionLocal() as session:
                try:
                    # Try to insert new record
                    session.add(webhook_record)
                    session.commit()
                    logger.info(f"Webhook stored: {webhook_id} (signature: {transaction_signature})")
                except Exception as db_error:
                    session.rollback()
                    # Handle duplicate webhook_id by updating existing record
                    if "duplicate key" in str(db_error).lower():
                        existing = session.query(BronzeWebhookTransaction).filter_by(webhook_id=webhook_id).first()
                        if existing:
                            existing.raw_payload = payload
                            existing.ingested_at = timestamp
                            session.commit()
                            logger.info(f"Webhook updated: {webhook_id} (signature: {transaction_signature})")
                    else:
                        logger.error(f"Database error: {db_error}")
                        raise
        except Exception as e:
            logger.error(f"Failed to store webhook in database: {e}")
            raise HTTPException(status_code=500, detail="Failed to store webhook")
            
        
        return WebhookResponse(
            status="success",
            message_id=webhook_id,
            timestamp=timestamp.isoformat()
        )
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/webhooks/count")
async def get_webhook_count():
    """
    Get webhook processing count from PostgreSQL.
    """
    try:
        with SessionLocal() as session:
            total_count = session.query(BronzeWebhookTransaction).count()
            pending_count = session.query(BronzeWebhookTransaction).filter_by(processing_status="pending").count()
            processed_count = session.query(BronzeWebhookTransaction).filter_by(processing_status="processed").count()
            failed_count = session.query(BronzeWebhookTransaction).filter_by(processing_status="failed").count()
            
        return {
            "mode": "postgresql",
            "storage": "postgres_bronze_layer",
            "table": "bronze.bronze_webhook_transactions",
            "total_webhooks": total_count,
            "pending": pending_count,
            "processed": processed_count,
            "failed": failed_count
        }
    except Exception as e:
        logger.error(f"Error getting webhook count: {e}")
        raise HTTPException(status_code=500, detail="Error getting webhook count")

@app.get("/webhooks/status")
async def get_webhook_status():
    """
    Get current webhook registration status.
    """
    try:
        url_file = DATA_DIR / "current_webhook_url.txt"
        if url_file.exists():
            with open(url_file, 'r') as f:
                webhook_url = f.read().strip()
            return {
                "status": "registered",
                "webhook_url": webhook_url,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
                "storage_mode": "postgresql",
                "table": "bronze.bronze_webhook_transactions"
            }
        else:
            return {
                "status": "not_registered",
                "webhook_url": None,
                "helius_configured": bool(os.getenv("HELIUS_API_KEY")),
                "storage_mode": "postgresql",
                "table": "bronze.bronze_webhook_transactions"
            }
    except Exception as e:
        logger.error(f"Error getting webhook status: {e}")
        raise HTTPException(status_code=500, detail="Error getting webhook status")

@app.get("/webhooks/cleanup")
async def cleanup_status():
    """
    Cleanup old webhook records from PostgreSQL.
    """
    try:
        from datetime import timedelta
        with SessionLocal() as session:
            # Get count of old records (older than 7 days)
            from sqlalchemy import func
            cutoff_date = datetime.utcnow() - timedelta(days=7)
            old_count = session.query(BronzeWebhookTransaction).filter(
                BronzeWebhookTransaction.ingested_at < cutoff_date
            ).count()
            
        return {
            "status": "available",
            "message": "PostgreSQL cleanup available",
            "mode": "postgresql",
            "old_records_count": old_count,
            "cutoff_days": 7
        }
    except Exception as e:
        logger.error(f"Error getting cleanup status: {e}")
        raise HTTPException(status_code=500, detail="Error getting cleanup status")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)