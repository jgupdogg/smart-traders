-- Reset Silver PnL Pipeline
-- This script clears all silver PnL data and resets processing flags
-- so that the new cost basis matching logic can be applied

BEGIN;

-- 1. Clear all existing silver_wallet_pnl records
TRUNCATE TABLE silver.silver_wallet_pnl;

-- 2. Reset pnl_processed flags in silver_whales to trigger recalculation
UPDATE silver.silver_whales 
SET 
    pnl_processed = false,
    pnl_processed_at = NULL
WHERE pnl_processed = true;

-- 3. Clear pipeline state entries for silver_wallet_pnl task
DELETE FROM pipeline.pipeline_state 
WHERE task_name = 'silver_wallet_pnl';

-- 4. Clear any gold layer data that depends on silver PnL (if exists)
-- Check if gold.smart_traders table exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'gold' AND table_name = 'smart_traders') THEN
        TRUNCATE TABLE gold.smart_traders;
        RAISE NOTICE 'Cleared gold.smart_traders table';
    ELSE
        RAISE NOTICE 'gold.smart_traders table does not exist';
    END IF;
END $$;

-- 5. Clear any other gold layer tables that might depend on silver PnL
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables 
               WHERE table_schema = 'gold' AND table_name = 'trader_performance_history') THEN
        TRUNCATE TABLE gold.trader_performance_history;
        RAISE NOTICE 'Cleared gold.trader_performance_history table';
    ELSE
        RAISE NOTICE 'gold.trader_performance_history table does not exist';
    END IF;
END $$;

-- 6. Reset gold_processed flags in silver_wallet_pnl (will be empty but for future records)
-- This is redundant since we truncated the table, but included for completeness

-- 7. Show summary of what was reset
SELECT 
    'silver.silver_wallet_pnl' as table_name,
    COUNT(*) as record_count,
    'All records cleared' as status
FROM silver.silver_wallet_pnl

UNION ALL

SELECT 
    'silver.silver_whales (pnl_processed=false)',
    COUNT(*),
    'Ready for PnL recalculation'
FROM silver.silver_whales 
WHERE pnl_processed = false

UNION ALL

SELECT 
    'pipeline.pipeline_state (silver_wallet_pnl)',
    COUNT(*),
    'State entries cleared'
FROM pipeline.pipeline_state 
WHERE task_name = 'silver_wallet_pnl';

COMMIT;

-- Display final status
\echo 'Silver PnL pipeline has been reset!'
\echo 'Next steps:'
\echo '1. Run the database migration: alembic upgrade head'
\echo '2. Trigger the silver_wallet_pnl task in Airflow'
\echo '3. All wallets will be recalculated with new cost basis matching logic'