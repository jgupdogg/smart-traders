-- PostgreSQL initialization script for Solana Smart Traders
-- This script creates the basic database structure

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schemas for organization
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS pipeline;

-- Grant permissions
GRANT USAGE ON SCHEMA bronze TO trader;
GRANT USAGE ON SCHEMA silver TO trader;
GRANT USAGE ON SCHEMA gold TO trader;
GRANT USAGE ON SCHEMA pipeline TO trader;

GRANT CREATE ON SCHEMA bronze TO trader;
GRANT CREATE ON SCHEMA silver TO trader;
GRANT CREATE ON SCHEMA gold TO trader;
GRANT CREATE ON SCHEMA pipeline TO trader;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO trader;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO trader;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO trader;
ALTER DEFAULT PRIVILEGES IN SCHEMA pipeline GRANT ALL ON TABLES TO trader;

-- Create indexes for common query patterns (will be extended by SQLModel/Alembic)
-- These are basic indexes that will be useful across all tables

-- Set timezone
SET timezone = 'UTC';

-- Create a basic logging table for pipeline operations
CREATE TABLE IF NOT EXISTS pipeline.operation_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    operation VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    details JSONB,
    duration_seconds FLOAT
);

-- Create index for operation log
CREATE INDEX IF NOT EXISTS idx_operation_log_timestamp ON pipeline.operation_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_operation_log_operation ON pipeline.operation_log(operation);
CREATE INDEX IF NOT EXISTS idx_operation_log_status ON pipeline.operation_log(status);

-- Log the initialization
INSERT INTO pipeline.operation_log (operation, status, message) 
VALUES ('database_init', 'success', 'Database initialized successfully');

-- Display completion message
DO $$
BEGIN
    RAISE NOTICE 'Solana Smart Traders database initialized successfully';
    RAISE NOTICE 'Schemas created: bronze, silver, gold, pipeline';
    RAISE NOTICE 'Extensions enabled: uuid-ossp, pg_trgm';
END
$$;