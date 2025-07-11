-- Verify PnL Tracking Status
-- This script checks for mismatches between whale tracking and actual PnL records

-- 1. Overview of tracking vs actual records
SELECT 
    'Overview' as check_type,
    COUNT(DISTINCT sw.wallet_address) as total_whales,
    COUNT(DISTINCT CASE WHEN sw.pnl_processed = true THEN sw.wallet_address END) as marked_processed,
    COUNT(DISTINCT CASE WHEN sw.pnl_processed = false THEN sw.wallet_address END) as marked_unprocessed,
    COUNT(DISTINCT pnl.wallet_address) as actual_pnl_records,
    COUNT(DISTINCT CASE WHEN sw.pnl_processed = true AND pnl.wallet_address IS NULL THEN sw.wallet_address END) as marked_but_no_pnl,
    COUNT(DISTINCT CASE WHEN sw.pnl_processed = false AND pnl.wallet_address IS NOT NULL THEN sw.wallet_address END) as unmarked_but_has_pnl
FROM silver.silver_whales sw
LEFT JOIN silver.silver_wallet_pnl pnl ON sw.wallet_address = pnl.wallet_address;

-- 2. Detailed breakdown by processing status
SELECT 
    'Processing Status' as check_type,
    sw.pnl_processed,
    sw.transactions_processed,
    COUNT(DISTINCT sw.wallet_address) as whale_count,
    COUNT(DISTINCT pnl.wallet_address) as pnl_record_count
FROM silver.silver_whales sw
LEFT JOIN silver.silver_wallet_pnl pnl ON sw.wallet_address = pnl.wallet_address
GROUP BY sw.pnl_processed, sw.transactions_processed
ORDER BY sw.pnl_processed DESC, sw.transactions_processed DESC;

-- 3. Check pipeline state vs actual records
SELECT 
    'Pipeline State' as check_type,
    ps.status as pipeline_status,
    COUNT(DISTINCT ps.entity_id) as pipeline_count,
    COUNT(DISTINCT pnl.wallet_address) as pnl_exists
FROM pipeline.pipeline_state ps
LEFT JOIN silver.silver_wallet_pnl pnl ON ps.entity_id = pnl.wallet_address
WHERE ps.task_name = 'silver_wallet_pnl'
GROUP BY ps.status;

-- 4. Transaction counts for whales marked as processed but no PnL
WITH processed_no_pnl AS (
    SELECT sw.wallet_address
    FROM silver.silver_whales sw
    LEFT JOIN silver.silver_wallet_pnl pnl ON sw.wallet_address = pnl.wallet_address
    WHERE sw.pnl_processed = true AND pnl.wallet_address IS NULL
),
transaction_counts AS (
    SELECT 
        p.wallet_address,
        COUNT(t.transaction_hash) as tx_count
    FROM processed_no_pnl p
    LEFT JOIN bronze.bronze_transactions t ON p.wallet_address = t.wallet_address
    GROUP BY p.wallet_address
)
SELECT 
    'Mismatched Whales Transaction Analysis' as check_type,
    CASE 
        WHEN tx_count = 0 THEN '0 transactions'
        WHEN tx_count BETWEEN 1 AND 4 THEN '1-4 transactions'
        WHEN tx_count >= 5 THEN '5+ transactions'
    END as transaction_range,
    COUNT(*) as whale_count
FROM transaction_counts
GROUP BY 
    CASE 
        WHEN tx_count = 0 THEN '0 transactions'
        WHEN tx_count BETWEEN 1 AND 4 THEN '1-4 transactions'
        WHEN tx_count >= 5 THEN '5+ transactions'
    END;

-- 5. Sample of mismatched records
SELECT 
    'Sample Mismatched Records' as check_type,
    sw.wallet_address,
    sw.pnl_processed,
    sw.pnl_processed_at,
    sw.transactions_processed,
    CASE WHEN pnl.wallet_address IS NOT NULL THEN 'YES' ELSE 'NO' END as has_pnl_record
FROM silver.silver_whales sw
LEFT JOIN silver.silver_wallet_pnl pnl ON sw.wallet_address = pnl.wallet_address
WHERE (sw.pnl_processed = true AND pnl.wallet_address IS NULL)
   OR (sw.pnl_processed = false AND pnl.wallet_address IS NOT NULL)
LIMIT 10;