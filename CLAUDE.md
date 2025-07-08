# Solana Smart Traders Pipeline

## Overview
This project implements a medallion architecture pipeline for identifying profitable Solana traders using Airflow, PostgreSQL, and pandas. The pipeline processes token data from BirdEye API, tracks whale activities, calculates trading performance, and identifies smart traders based on profitability metrics.

## Architecture

### Medallion Architecture Layers
- **Bronze Layer**: Raw data from BirdEye API (tokens, whales, transactions)
- **Silver Layer**: Curated and filtered data (selected tokens, unique whales, calculated PnL)
- **Gold Layer**: Final smart traders table with performance rankings

### Task Flow
```
bronze_tokens → silver_tokens → bronze_whales → silver_whales → bronze_transactions → silver_wallet_pnl → smart_traders
```

### Current Pipeline Status
- ✅ **bronze_tokens**: Working - Fetches trending tokens from BirdEye API
- ✅ **silver_tokens**: Working - Selects top tokens based on scoring criteria
- ✅ **bronze_whales**: Working - Fetches top holders for selected tokens using BirdEye v3 API
- ✅ **silver_whales**: Working - Processes all unique whale addresses without filtering
- 🔄 **bronze_transactions**: Ready for implementation - Fetches transaction history for whales
- 🔄 **silver_wallet_pnl**: Ready for implementation - Calculates FIFO-based PnL metrics
- 🔄 **smart_traders**: Ready for implementation - Ranks traders by performance tiers

## Key Features

### Precise State Tracking
Every entity (token, whale, transaction) has individual state tracking in the `pipeline_state` table with states:
- `pending`: Ready for processing
- `processing`: Currently being processed
- `completed`: Successfully processed
- `failed`: Failed after max attempts
- `skipped`: Skipped due to business rules

### Type-Safe Database Models
All database operations use SQLModel for type safety and automatic schema generation:
- **Bronze Models**: Raw API data storage
- **Silver Models**: Processed and filtered data
- **Gold Models**: Final smart trader rankings
- **State Models**: Pipeline state and execution tracking

### Efficient Database Operations
- **UPSERT operations**: PostgreSQL `ON CONFLICT` for idempotent processing
- **Batch processing**: Configurable batch sizes for memory management
- **Connection pooling**: Optimized database connections with retry logic
- **NaN/NULL handling**: Robust data cleaning to handle missing values from API responses

### Recent Improvements
- **Enhanced Bronze Tokens Model**: Added flattened extension fields (coingecko_id, website, social links) for comprehensive token metadata
- **Robust NaN Handling**: Implemented multi-layer data cleaning to prevent "integer out of range" errors when processing missing data
- **Database Operations**: Enhanced UPSERT operations with comprehensive NaN value cleaning at the database level
- **BirdEye API v3 Integration**: Updated to use correct `/defi/v3/token/holder` endpoint with proper response normalization
- **SQLModel Column Access Fixes**: Implemented safe attribute access using `getattr()` with fallbacks to handle SQLModel mapping issues
- **Duplicate Wallet Handling**: Added deduplication logic for wallets with multiple token accounts for the same token
- **Raw SQL Fallback**: Added raw SQL queries as fallback for SQLModel tuple wrapping issues
- **Consistent Client Usage**: Standardized BirdEye API client initialization and usage patterns across all bronze tasks
- **Status Field Optimization**: Shortened status values to fit database varchar constraints
- **Silver Layer Processing**: Removed all filters from silver_whales to ensure complete data flow from bronze to silver layers

## Key Commands

### Development Setup
```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f airflow-webserver
```

### Database Management
```bash
# Run database migrations
docker-compose exec airflow-webserver alembic upgrade head

# Create new migration
docker-compose exec airflow-webserver alembic revision --autogenerate -m "description"

# Check database health
docker-compose exec postgres psql -U trader -d solana-smart-traders -c "SELECT 1;"
```

### Airflow Operations
```bash
# Access Airflow UI
# http://localhost:8080 (admin/admin)

# Trigger DAG manually
docker-compose exec airflow-webserver airflow dags trigger solana_smart_traders_pipeline

# Check DAG status
docker-compose exec airflow-webserver airflow dags list

# View task logs
docker-compose exec airflow-webserver airflow tasks logs smart_traders_pipeline bronze_tokens 2024-01-01
```

### Data Operations
```bash
# Check table statistics
docker-compose exec postgres psql -U trader -d solana-smart-traders -c "
SELECT schemaname, tablename, n_tup_ins, n_tup_upd, n_tup_del 
FROM pg_stat_user_tables;"

# View pipeline state
docker-compose exec postgres psql -U trader -d solana-smart-traders -c "
SELECT task_name, state, COUNT(*) 
FROM pipeline.pipeline_state 
GROUP BY task_name, state;"
```

### Configuration Management
```bash
# Reload configuration (requires restart)
docker-compose restart airflow-webserver airflow-scheduler airflow-worker

# View current configuration
docker-compose exec airflow-webserver python -c "
from src.config.settings import get_settings
print(get_settings().bronze_tokens.__dict__)
"
```

## Configuration

### Central Configuration
All pipeline parameters are configured in `airflow/config/pipeline_config.yaml`:
- API rate limits and batch sizes
- Selection criteria for tokens and whales
- PnL calculation parameters
- Performance thresholds for smart traders

### Environment Variables
Required environment variables in `.env`:
```bash
BIRDEYE_API_KEY=your_birdeye_api_key_here
POSTGRES_USER=trader
POSTGRES_PASSWORD=trader_password
POSTGRES_DB=solana_data
```

### Task-Specific Configuration
Each task has dedicated configuration sections:
- `bronze_tokens`: Token filtering and API parameters
- `silver_tokens`: Selection criteria and scoring weights
- `bronze_whales`: Whale identification parameters
- `silver_whales`: Unique whale selection logic
- `bronze_transactions`: Transaction fetching configuration
- `silver_wallet_pnl`: PnL calculation settings
- `smart_traders`: Performance tier definitions

## Database Schema

### Bronze Layer
- `bronze.bronze_tokens`: Raw token data from BirdEye API
- `bronze.bronze_whales`: Top holders per token
- `bronze.bronze_transactions`: Raw transaction data

### Silver Layer
- `silver.silver_tokens`: Selected tokens for tracking
- `silver.silver_whales`: Unique whale addresses
- `silver.silver_wallet_pnl`: Calculated trading performance

### Gold Layer
- `gold.smart_traders`: Final ranked profitable traders
- `gold.trader_performance_history`: Historical performance tracking

### Pipeline Layer
- `pipeline.pipeline_state`: Entity-level state tracking
- `pipeline.task_execution_log`: Task execution statistics
- `pipeline.data_quality_checks`: Data validation results

## Task Details

### 1. Bronze Tokens
- Fetches trending tokens from BirdEye API
- Filters by liquidity, volume, and price change criteria
- Processes in batches with rate limiting
- Tracks processing state for each token

### 2. Silver Tokens
- Selects top tokens based on composite scoring
- Considers liquidity, volume, price changes, and holder count
- Configurable selection ratio and maximum count
- Marks selected tokens for whale processing

### 3. Bronze Whales
- Fetches top holders for each selected token
- Stores holder rankings and amounts
- Tracks refetch intervals (72 hours default)
- Handles API rate limits and batch processing

### 4. Silver Whales
- Identifies unique whale addresses across tokens
- Excludes known CEX addresses
- Tracks whale metadata and holdings
- Marks whales for transaction processing

### 5. Bronze Transactions
- Fetches transaction history for each whale
- 30-day lookback period with configurable limits
- Normalizes transaction data for analysis
- Handles pagination and rate limits

### 6. Silver Wallet PnL
- Calculates FIFO-based profit/loss for each wallet
- Computes win rates, trade frequencies, and risk metrics
- Processes wallets in batches with memory management
- Only includes realized PnL (completed trades)

### 7. Smart Traders
- Ranks traders based on performance metrics
- Assigns performance tiers (ELITE, STRONG, PROMISING, QUALIFIED)
- Calculates composite scores and rankings
- Tracks historical performance changes

## Monitoring and Observability

### Health Checks
- Database connection health
- API connectivity status
- Memory and CPU usage tracking
- Task execution monitoring

### Data Quality
- Completeness checks for required fields
- Uniqueness validation for primary keys
- Range validation for numeric fields
- Referential integrity checks

### Performance Metrics
- Task execution times
- Record processing rates
- API call success rates
- Memory usage patterns

### Alerting
- Task failure notifications
- Performance threshold breaches
- Data quality violations
- Resource usage alerts

## Common Issues and Solutions

### Memory Management
- **Issue**: Out of memory during large batch processing
- **Solution**: Reduce batch sizes in configuration, enable memory monitoring

### API Rate Limits
- **Issue**: BirdEye API rate limit exceeded
- **Solution**: Increase delays in configuration, implement circuit breakers

### State Conflicts
- **Issue**: Concurrent task execution causing state conflicts
- **Solution**: Use UPSERT operations, implement proper locking

### Database Performance
- **Issue**: Slow query performance
- **Solution**: Run VACUUM ANALYZE, optimize indexes, check connection pool

### Data Consistency
- **Issue**: Missing or inconsistent data between layers
- **Solution**: Check state tracking, review error logs, re-run failed tasks

## Development Workflow

### Adding New Tasks
1. Create task function in `src/tasks/`
2. Add configuration section to `pipeline_config.yaml`
3. Update settings classes in `src/config/settings.py`
4. Add task to Airflow DAG
5. Include state tracking and error handling

### Modifying Database Schema
1. Update SQLModel classes in `src/models/`
2. Generate Alembic migration: `alembic revision --autogenerate`
3. Review and edit migration file
4. Apply migration: `alembic upgrade head`

### Configuration Changes
1. Update `airflow/config/pipeline_config.yaml`
2. Modify corresponding dataclass in `src/config/settings.py`
3. Restart Airflow services to apply changes
4. Test configuration loading

### Testing
```bash
# Run unit tests
docker-compose exec airflow-webserver python -m pytest tests/

# Run specific test file
docker-compose exec airflow-webserver python -m pytest tests/test_database_operations.py

# Run with coverage
docker-compose exec airflow-webserver python -m pytest --cov=src tests/
```

## Performance Optimization

### Database Optimization
- Regular VACUUM ANALYZE operations
- Index optimization for query patterns
- Connection pool tuning
- Partition large tables by date

### Memory Management
- Batch size tuning for available memory
- Memory usage monitoring and alerting
- Garbage collection between batches
- Process isolation for heavy tasks

### API Efficiency
- Request batching where possible
- Intelligent rate limiting
- Response caching for repeated calls
- Circuit breaker pattern for failures

## Security Considerations

### API Keys
- Store in environment variables only
- Rotate keys regularly
- Monitor API usage and quotas
- Implement key rotation procedures

### Database Security
- Use separate user accounts with minimal privileges
- Enable connection encryption
- Regular security updates
- Audit database access

### Network Security
- Firewall rules for database access
- VPN or private networks for production
- Regular security scanning
- Access logging and monitoring

## Backup and Recovery

### Database Backups
- Automated daily backups
- Point-in-time recovery capability
- Cross-region backup storage
- Regular restore testing

### Configuration Backups
- Version control for all configuration
- Backup of environment variables
- Documentation of manual configurations
- Change tracking and approval

### Disaster Recovery
- Recovery time objectives (RTO)
- Recovery point objectives (RPO)
- Failover procedures
- Regular disaster recovery testing