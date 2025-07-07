"""
Database connection management for the Solana Smart Traders pipeline.
Provides connection pooling, session management, and connection utilities.
"""

import os
import logging
from contextlib import contextmanager
from typing import Generator, Optional
from sqlalchemy import create_engine, event
from sqlalchemy.pool import QueuePool
from sqlmodel import Session, SQLModel
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection manager with connection pooling and retry logic."""
    
    def __init__(self, database_url: Optional[str] = None):
        """Initialize database connection manager.
        
        Args:
            database_url: PostgreSQL connection string. If None, builds from environment variables.
        """
        self.database_url = database_url or self._build_database_url()
        self.engine: Optional[Engine] = None
        self._setup_engine()
    
    def _build_database_url(self) -> str:
        """Build database URL from environment variables."""
        user = os.getenv('POSTGRES_USER', 'trader')
        password = os.getenv('POSTGRES_PASSWORD', 'trader_password')
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'solana_data')
        
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    
    def _setup_engine(self):
        """Setup SQLAlchemy engine with connection pooling."""
        self.engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=10,                    # Number of connections to maintain in pool
            max_overflow=20,                 # Additional connections beyond pool_size
            pool_pre_ping=True,              # Validate connections before use
            pool_recycle=3600,               # Recycle connections after 1 hour
            echo=False,                      # Set to True for SQL query logging
            connect_args={
                "connect_timeout": 30,       # Connection timeout in seconds
                "application_name": "solana_smart_traders"
            }
        )
        
        # Add connection event listeners for logging
        @event.listens_for(self.engine, "connect")
        def on_connect(dbapi_conn, connection_record):
            logger.debug(f"New database connection established: {connection_record}")
        
        @event.listens_for(self.engine, "checkout")
        def on_checkout(dbapi_conn, connection_record, connection_proxy):
            logger.debug("Connection checked out from pool")
        
        @event.listens_for(self.engine, "checkin")
        def on_checkin(dbapi_conn, connection_record):
            logger.debug("Connection checked back into pool")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    def test_connection(self) -> bool:
        """Test database connection with retry logic.
        
        Returns:
            True if connection successful, raises exception if failed.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT 1").fetchone()
                logger.info("Database connection test successful")
                return result[0] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get database session with automatic cleanup.
        
        Yields:
            SQLModel Session instance
        """
        session = Session(self.engine)
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {e}")
            session.rollback()
            raise
        finally:
            session.close()
    
    def create_all_tables(self):
        """Create all tables defined in SQLModel metadata."""
        try:
            SQLModel.metadata.create_all(self.engine)
            logger.info("All database tables created successfully")
        except Exception as e:
            logger.error(f"Failed to create database tables: {e}")
            raise
    
    def get_pool_status(self) -> dict:
        """Get connection pool status for monitoring.
        
        Returns:
            Dictionary with pool statistics
        """
        if not self.engine or not hasattr(self.engine.pool, 'size'):
            return {"status": "no_pool"}
        
        pool = self.engine.pool
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid()
        }


# Global database connection instance
_db_connection: Optional[DatabaseConnection] = None


def get_database_connection() -> DatabaseConnection:
    """Get the global database connection instance.
    
    Returns:
        DatabaseConnection instance
    """
    global _db_connection
    if _db_connection is None:
        _db_connection = DatabaseConnection()
    return _db_connection


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """Convenience function to get a database session.
    
    Yields:
        SQLModel Session instance
    """
    db = get_database_connection()
    with db.get_session() as session:
        yield session


def initialize_database():
    """Initialize database connection and create tables."""
    logger.info("Initializing database connection...")
    
    db = get_database_connection()
    
    # Test connection
    db.test_connection()
    
    # Create tables
    db.create_all_tables()
    
    logger.info("Database initialization completed")


def close_database_connection():
    """Close the global database connection."""
    global _db_connection
    if _db_connection and _db_connection.engine:
        _db_connection.engine.dispose()
        _db_connection = None
        logger.info("Database connection closed")


# Health check function for monitoring
def health_check() -> dict:
    """Perform database health check.
    
    Returns:
        Dictionary with health status and metrics
    """
    try:
        db = get_database_connection()
        
        # Test connection
        connection_ok = db.test_connection()
        
        # Get pool status
        pool_status = db.get_pool_status()
        
        return {
            "status": "healthy" if connection_ok else "unhealthy",
            "connection_test": connection_ok,
            "pool_status": pool_status,
            "database_url_host": db.database_url.split('@')[1].split('/')[0] if '@' in db.database_url else "unknown"
        }
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "connection_test": False
        }