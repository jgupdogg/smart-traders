#!/bin/bash
# Check Silver PnL Pipeline Status
# This script shows the current status of the silver PnL pipeline

set -e

echo "📊 Silver PnL Pipeline Status Check"
echo "=================================="

# Check if docker-compose is available and services are running
if command -v docker-compose &> /dev/null; then
    if docker-compose ps postgres | grep -q "Up"; then
        PSQL_CMD="docker-compose exec -T postgres psql -U trader -d solana-smart-traders"
    else
        echo "❌ PostgreSQL container is not running. Please start services first:"
        echo "   docker-compose up -d"
        exit 1
    fi
else
    echo "❌ docker-compose not found. Please ensure Docker services are running."
    exit 1
fi

echo "🔍 Checking current status..."
echo ""

# Check silver_wallet_pnl table
echo "📈 Silver Wallet PnL Records:"
$PSQL_CMD -c "
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN smart_trader_eligible = true THEN 1 END) as smart_traders,
    ROUND(AVG(coverage_ratio_percent), 2) as avg_coverage_ratio,
    COUNT(CASE WHEN calculation_method LIKE '%cost_basis%' THEN 1 END) as new_method_records
FROM silver.silver_wallet_pnl;
" 2>/dev/null || echo "❌ silver.silver_wallet_pnl table not accessible"

echo ""

# Check silver_whales processing status
echo "🐋 Silver Whales Processing Status:"
$PSQL_CMD -c "
SELECT 
    COUNT(*) as total_whales,
    COUNT(CASE WHEN transactions_processed = true THEN 1 END) as transactions_done,
    COUNT(CASE WHEN pnl_processed = true THEN 1 END) as pnl_done,
    COUNT(CASE WHEN transactions_processed = true AND pnl_processed = false THEN 1 END) as ready_for_pnl
FROM silver.silver_whales;
" 2>/dev/null || echo "❌ silver.silver_whales table not accessible"

echo ""

# Check pipeline state
echo "🔄 Pipeline State for silver_wallet_pnl:"
$PSQL_CMD -c "
SELECT 
    state,
    COUNT(*) as count
FROM pipeline.pipeline_state 
WHERE task_name = 'silver_wallet_pnl'
GROUP BY state
ORDER BY state;
" 2>/dev/null || echo "❌ pipeline.pipeline_state table not accessible"

echo ""

# Check if new schema columns exist
echo "🏗️  Database Schema Status:"
$PSQL_CMD -c "
SELECT 
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'silver' 
AND table_name = 'silver_wallet_pnl'
AND column_name IN ('matched_trades', 'meaningful_trades', 'neutral_trades', 'coverage_ratio_percent')
ORDER BY column_name;
" 2>/dev/null && echo "✅ New columns found" || echo "❌ New columns missing - run migration first"

echo ""
echo "Status check complete! 📋"