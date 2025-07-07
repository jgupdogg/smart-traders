# Solana Smart Traders Pipeline

A medallion architecture data pipeline for identifying and ranking profitable Solana traders by analyzing token holdings, whale behavior, and trading performance.

## Overview

This pipeline processes Solana blockchain data through three layers:
- **Bronze**: Raw data from BirdEye API (tokens, holders, transactions)
- **Silver**: Cleaned and enriched data with calculated metrics
- **Gold**: Final smart trader rankings and insights

## Architecture

```
BirdEye API → Bronze Layer → Silver Layer → Gold Layer → Smart Traders
```

### Key Components
- **Apache Airflow**: Orchestrates the data pipeline
- **PostgreSQL**: Stores data across all medallion layers
- **Docker**: Containerizes all services
- **SQLModel**: Provides type-safe database operations
- **Pandas**: Handles data transformations

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- BirdEye API key

### Setup

1. Clone the repository:
```bash
git clone https://github.com/jgupdogg/smart-traders.git
cd smart-traders
```

2. Create `.env` file:
```bash
cp .env.example .env
# Add your BIRDEYE_API_KEY to .env
```

3. Start the services:
```bash
docker compose up -d
```

4. Initialize the database:
```bash
docker compose exec airflow-webserver python -m src.database.init_tables
```

5. Access Airflow UI:
- URL: http://localhost:8080
- Username: airflow
- Password: airflow

## Pipeline Tasks

1. **bronze_tokens**: Fetches trending tokens from BirdEye API
2. **silver_tokens**: Processes all bronze tokens (simplified - no filtering)
3. **bronze_whales**: Fetches top holders for selected tokens
4. **silver_whales**: Identifies unique whale wallets
5. **bronze_transactions**: Fetches whale trading activity
6. **silver_wallet_pnl**: Calculates FIFO-based profit/loss
7. **gold_smart_traders**: Ranks traders by performance

## Configuration

Edit `config/pipeline_config.yaml` to adjust:
- API limits and filters
- Token selection criteria
- Whale thresholds
- Performance metrics

## Database Schema

The pipeline uses a medallion architecture with schemas:
- `bronze.*`: Raw data tables
- `silver.*`: Processed data tables
- `gold.*`: Final analytics tables
- `pipeline.*`: State tracking tables

## Development

### Running Tasks Manually

```bash
# Test bronze tokens task
docker compose exec airflow-webserver python -c "from src.tasks.bronze_tokens import process_bronze_tokens; print(process_bronze_tokens())"

# Test silver tokens task
docker compose exec airflow-webserver python -c "from src.tasks.silver_tokens import process_silver_tokens; print(process_silver_tokens())"
```

### Monitoring

View logs:
```bash
docker compose logs -f airflow-scheduler
```

Check database:
```bash
docker compose exec postgres psql -U airflow -d solana-smart-traders
```

## Documentation

For detailed documentation, see [CLAUDE.md](./CLAUDE.md)

## License

MIT License - see LICENSE file for details

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For issues and questions, please use the GitHub issue tracker.