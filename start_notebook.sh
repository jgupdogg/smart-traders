#!/bin/bash

# Start Jupyter notebook for database exploration
echo "Starting Jupyter notebook for Solana Smart Traders database exploration..."

# Check if notebook service is already running
if docker ps --format "table {{.Names}}" | grep -q "solana-jupyter"; then
    echo "Jupyter notebook is already running!"
    echo "Access it at: http://localhost:8888"
    echo "Token: solana-smart-traders"
    exit 0
fi

# Start the notebook service
echo "Starting Jupyter notebook container..."
docker compose -f docker-compose.notebook.yml up -d

# Wait for the service to start
echo "Waiting for Jupyter to start..."
sleep 10

# Show access information
echo ""
echo "✅ Jupyter notebook is now running!"
echo "🌐 Access URL: http://localhost:8888"
echo "🔑 Token: solana-smart-traders"
echo ""
echo "📁 Available notebooks:"
echo "  - database_explorer.ipynb (Main database exploration notebook)"
echo ""
echo "🗃️ Database connection details:"
echo "  - Host: postgres (from within container)"
echo "  - Database: solana-smart-traders"
echo "  - Username: trader"
echo "  - Password: trader_password"
echo ""
echo "To stop the notebook:"
echo "  docker compose -f docker-compose.notebook.yml down"
echo ""
echo "📊 The notebook includes pre-built queries for:"
echo "  - Bronze layer tables (raw data)"
echo "  - Silver layer tables (processed data)"
echo "  - Gold layer tables (smart traders)"
echo "  - Pipeline state tracking"
echo "  - Data quality validation"
echo "  - Custom query examples"