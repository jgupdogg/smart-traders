#!/bin/bash
# Reset Silver PnL Pipeline Script
# This script safely resets all silver PnL data and related downstream tables

set -e  # Exit on any error

echo "🔄 Resetting Silver PnL Pipeline..."
echo "This will:"
echo "  ✨ Clear all existing silver_wallet_pnl records"
echo "  🔄 Reset pnl_processed flags in silver_whales"
echo "  🗑️  Clear related pipeline state entries"
echo "  🧹 Clear any gold layer data that depends on silver PnL"
echo ""

# Confirm with user
read -p "Are you sure you want to proceed? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Operation cancelled"
    exit 1
fi

echo "🚀 Starting reset process..."

# Check if docker-compose is available and services are running
if command -v docker-compose &> /dev/null; then
    if docker-compose ps postgres | grep -q "Up"; then
        echo "✅ Using docker-compose to access PostgreSQL"
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

# Run the reset SQL script
echo "📝 Executing reset SQL script..."
if $PSQL_CMD -f reset_silver_pnl.sql; then
    echo "✅ SQL script executed successfully"
else
    echo "❌ SQL script failed"
    exit 1
fi

# Apply database migrations to add new columns
echo "🔧 Applying database migrations..."
if docker-compose exec -T airflow-webserver alembic upgrade head; then
    echo "✅ Database migrations applied successfully"
else
    echo "❌ Database migration failed"
    exit 1
fi

echo ""
echo "🎉 Silver PnL pipeline reset completed successfully!"
echo ""
echo "📋 Summary:"
echo "  ✅ All silver_wallet_pnl records cleared"
echo "  ✅ All silver_whales.pnl_processed flags reset to false"
echo "  ✅ Pipeline state cleared for silver_wallet_pnl task"
echo "  ✅ Gold layer data cleared (if existed)"
echo "  ✅ Database schema updated with new columns"
echo ""
echo "🚀 Next steps:"
echo "  1. Go to Airflow UI: http://localhost:8080"
echo "  2. Trigger the 'solana_smart_traders_pipeline' DAG"
echo "  3. Or run specific task: silver_wallet_pnl"
echo ""
echo "📊 The new calculation will include:"
echo "  • Cost basis matching (only trades with known buy prices)"
echo "  • $50 minimum threshold for winning/losing trade classification"
echo "  • Enhanced coverage ratio metrics"
echo "  • More accurate smart trader identification"