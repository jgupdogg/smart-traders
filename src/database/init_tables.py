"""
Database table initialization script.
Creates all tables defined in SQLModel schemas.
"""

import logging
from sqlmodel import SQLModel
from src.database.connection import get_database_connection
from src.models.bronze import BronzeToken, BronzeWhale, BronzeTransaction
from src.models.silver import SilverToken, SilverWhale, SilverWalletPnL
from src.models.gold import SmartTrader
from src.models.pipeline import PipelineState, EntityState
from src.models.state import EntityProcessingState, TaskExecutionLog, DataQualityCheck

logger = logging.getLogger(__name__)


def create_all_tables():
    """Create all database tables."""
    try:
        db = get_database_connection()
        engine = db.engine
        
        logger.info("Creating all database tables...")
        SQLModel.metadata.create_all(engine)
        logger.info("All tables created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        return False


def init_database():
    """Initialize database with all required tables."""
    logger.info("Initializing database schema...")
    
    success = create_all_tables()
    
    if success:
        logger.info("Database initialization completed successfully")
        return {"status": "success", "message": "All tables created"}
    else:
        logger.error("Database initialization failed")
        return {"status": "failed", "message": "Table creation failed"}


if __name__ == "__main__":
    # Can be run standalone
    result = init_database()
    print(f"Database initialization: {result}")