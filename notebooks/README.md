# Solana Smart Traders - Database Exploration Notebooks

This directory contains Jupyter notebooks for exploring and analyzing the Solana Smart Traders database.

## Quick Start

### Option 1: Using Docker (Recommended)
```bash
# From the project root directory
./start_notebook.sh
```

Then access: http://localhost:8888 with token: `solana-smart-traders`

### Option 2: Local Installation
```bash
# Install requirements
pip install -r notebooks/requirements.txt

# Start Jupyter
cd notebooks
jupyter notebook
```

## Available Notebooks

### 1. `quick_start.ipynb`
- 🚀 **Start here** - Tests database connectivity
- Shows basic table information and data overview
- Verifies that the pipeline is working

### 2. `database_explorer.ipynb`
- 📊 **Comprehensive exploration** of all database tables
- Pre-built queries for each medallion layer
- Data quality validation and flow analysis
- Custom query examples

## Database Connection Details

- **Host**: `localhost` (or `host.docker.internal` from Docker)
- **Port**: `5432`
- **Database**: `solana-smart-traders`
- **Username**: `trader`
- **Password**: `trader_password`

## Medallion Architecture Layers

### Bronze Layer (Raw Data)
- `bronze.bronze_tokens` - Token data from BirdEye API
- `bronze.bronze_whales` - Top token holders
- `bronze.bronze_transactions` - Transaction history

### Silver Layer (Processed Data)
- `silver.silver_tokens` - Selected tokens for tracking
- `silver.silver_whales` - Unique whale addresses
- `silver.silver_wallet_pnl` - Calculated trading performance

### Gold Layer (Analytics)
- `gold.smart_traders` - Final ranked smart traders

### Pipeline Layer (Monitoring)
- `pipeline.entity_processing_state` - Entity-level state tracking
- `pipeline.task_execution_log` - Task execution history
- `pipeline.data_quality_checks` - Data validation results

## Key Features

### State Tracking Analysis
- View processing state across all pipeline layers
- Analyze data flow integrity
- Monitor task execution performance

### Smart Trader Analytics
- Performance tier distribution
- Trading metrics analysis
- Portfolio characteristics

### Data Quality Validation
- Record count validation
- State tracking integrity
- Cross-layer consistency checks

## Example Queries

### Top Smart Traders
```sql
SELECT 
    performance_tier,
    wallet_address,
    total_realized_pnl_usd,
    win_rate_percent,
    total_trades
FROM gold.smart_traders
ORDER BY total_realized_pnl_usd DESC;
```

### Pipeline State Overview
```sql
SELECT 
    task_name,
    state,
    COUNT(*) as entity_count
FROM pipeline.entity_processing_state
GROUP BY task_name, state
ORDER BY task_name;
```

### Token Performance Analysis
```sql
SELECT 
    bt.symbol,
    bt.market_cap,
    bt.price_change_24h_percent,
    COUNT(bw.wallet_address) as whale_count
FROM bronze.bronze_tokens bt
LEFT JOIN bronze.bronze_whales bw ON bt.token_address = bw.token_address
GROUP BY bt.token_address, bt.symbol, bt.market_cap, bt.price_change_24h_percent
ORDER BY bt.market_cap DESC;
```

## Troubleshooting

### Connection Issues
1. Ensure PostgreSQL is running: `docker compose ps`
2. Check if database is accessible: `docker compose exec postgres psql -U trader -d solana-smart-traders -c "SELECT 1;"`
3. Verify network connectivity from notebook container

### Missing Data
1. Check if pipeline has run: Look at `pipeline.task_execution_log`
2. Verify state tracking: Check processing flags in tables
3. Run pipeline manually: `docker compose exec airflow-webserver airflow dags trigger solana_smart_traders_pipeline`

### Performance Issues
1. Large result sets: Use `LIMIT` in queries
2. Slow queries: Check indexes with `EXPLAIN ANALYZE`
3. Memory issues: Restart notebook kernel

## Contributing

To add new analysis notebooks:
1. Create new `.ipynb` file in this directory
2. Use the database connection pattern from existing notebooks
3. Add description to this README
4. Include data validation and error handling

## Security Notes

- Database credentials are for development only
- Don't commit notebooks with sensitive data
- Use environment variables for production connections