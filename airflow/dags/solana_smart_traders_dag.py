"""
Solana Smart Traders DAG - Main pipeline orchestration.
Implements medallion architecture: Bronze → Silver → Gold data layers.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import sys
import os

# Add src path to Python path for imports
sys.path.append('/opt/airflow/src')
sys.path.append('/opt/airflow')

def run_bronze_tokens(**context):
    """Wrapper function to import and run bronze tokens task."""
    from smart_trader_tasks.bronze.tokens import process_bronze_tokens
    return process_bronze_tokens(**context)

def run_silver_tokens(**context):
    """Wrapper function to import and run silver tokens task."""
    from smart_trader_tasks.silver.tokens import process_silver_tokens
    return process_silver_tokens(**context)

def run_bronze_whales(**context):
    """Wrapper function to import and run bronze whales task."""
    from smart_trader_tasks.bronze.whales import process_bronze_whales
    return process_bronze_whales(**context)

def run_silver_whales(**context):
    """Wrapper function to import and run silver whales task."""
    from smart_trader_tasks.silver.whales import process_silver_whales
    return process_silver_whales(**context)

def run_bronze_transactions(**context):
    """Wrapper function to import and run bronze transactions task."""
    from smart_trader_tasks.bronze.transactions import process_bronze_transactions
    return process_bronze_transactions(**context)

def run_silver_wallet_pnl(**context):
    """Wrapper function to import and run silver wallet PnL task."""
    from smart_trader_tasks.silver.wallet_pnl import process_silver_wallet_pnl
    return process_silver_wallet_pnl(**context)

def run_smart_traders(**context):
    """Wrapper function to import and run smart traders task."""
    from smart_trader_tasks.gold.smart_traders import process_smart_traders
    return process_smart_traders(**context)

def run_helius_webhook_update(**context):
    """Wrapper function to import and run Helius webhook update task."""
    from src.services.webhook_updater import sync_update_webhook_after_dag_run
    return sync_update_webhook_after_dag_run()

def init_database(**context):
    """Initialize database tables."""
    from database.init_tables import init_database
    return init_database()

# Default arguments for all tasks
default_args = {
    'owner': 'solana-smart-traders',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'solana_smart_traders_pipeline',
    default_args=default_args,
    description='Solana Smart Traders medallion architecture pipeline',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
    max_active_runs=1,
    tags=['solana', 'crypto', 'trading', 'medallion-architecture']
)

# Start task
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Database initialization task
init_db_task = PythonOperator(
    task_id='init_database',
    python_callable=init_database,
    dag=dag,
    execution_timeout=timedelta(minutes=5)
)

# Bronze Layer Tasks
bronze_tokens_task = PythonOperator(
    task_id='bronze_tokens',
    python_callable=run_bronze_tokens,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=30)
)

# Silver Layer Tasks  
silver_tokens_task = PythonOperator(
    task_id='silver_tokens',
    python_callable=run_silver_tokens,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=15)
)

# Bronze Whales Task
bronze_whales_task = PythonOperator(
    task_id='bronze_whales',
    python_callable=run_bronze_whales,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=45)
)

# Silver Whales Task
silver_whales_task = PythonOperator(
    task_id='silver_whales',
    python_callable=run_silver_whales,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=20)
)

# Bronze Transactions Task
bronze_transactions_task = PythonOperator(
    task_id='bronze_transactions',
    python_callable=run_bronze_transactions,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=60)
)

# Silver Wallet PnL Task
silver_wallet_pnl_task = PythonOperator(
    task_id='silver_wallet_pnl',
    python_callable=run_silver_wallet_pnl,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=30)
)

# Smart Traders Task (Gold Layer)
smart_traders_task = PythonOperator(
    task_id='smart_traders',
    python_callable=run_smart_traders,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=15)
)

# Helius Webhook Update Task
helius_webhook_task = PythonOperator(
    task_id='update_helius_webhook',
    python_callable=run_helius_webhook_update,
    dag=dag,
    pool='default_pool',
    execution_timeout=timedelta(minutes=10)
)

# Pipeline completion
pipeline_complete = DummyOperator(
    task_id='pipeline_complete',
    dag=dag
)

# Task Dependencies - Complete pipeline flow
start_pipeline >> init_db_task >> bronze_tokens_task >> silver_tokens_task >> bronze_whales_task >> silver_whales_task >> bronze_transactions_task >> silver_wallet_pnl_task >> smart_traders_task >> helius_webhook_task >> pipeline_complete

# Additional task metadata
bronze_tokens_task.doc_md = """
## Bronze Tokens Task

Fetches trending tokens from BirdEye API and stores raw data in bronze_tokens table.

**What it does:**
- Fetches tokens using configured filters (liquidity, volume, price change)
- Normalizes API response data
- Performs batch UPSERT operations
- Tracks processing state

**Configuration:** Uses bronze_tokens section in pipeline_config.yaml
"""

silver_tokens_task.doc_md = """
## Silver Tokens Task

Selects best tokens from bronze layer using composite scoring algorithm.

**What it does:**
- Applies selection filters (min holders, volume)
- Calculates composite scores (liquidity, volume, price change, holders, trades)
- Selects top tokens based on score and ratio
- Stores selected tokens in silver_tokens table

**Configuration:** Uses silver_tokens section in pipeline_config.yaml
"""

bronze_whales_task.doc_md = """
## Bronze Whales Task

Fetches top holders for each selected token from BirdEye API.

**What it does:**
- Processes tokens from silver_tokens table
- Fetches top holders per token using BirdEye API
- Stores whale/holder data in bronze_whales table
- Updates processing state tracking

**Configuration:** Uses bronze_whales section in pipeline_config.yaml
"""

silver_whales_task.doc_md = """
## Silver Whales Task

Identifies unique whale addresses across all tracked tokens.

**What it does:**
- Aggregates unique wallet addresses from bronze_whales
- Filters by minimum token holdings and value thresholds
- Excludes known CEX addresses
- Stores unique whales in silver_whales table

**Configuration:** Uses silver_whales section in pipeline_config.yaml
"""

bronze_transactions_task.doc_md = """
## Bronze Transactions Task

Fetches transaction history for each whale from BirdEye API.

**What it does:**
- Processes whales from silver_whales table
- Fetches 30-day transaction history per whale
- Filters transactions by type and minimum value
- Stores transaction data in bronze_transactions table

**Configuration:** Uses bronze_transactions section in pipeline_config.yaml
"""

silver_wallet_pnl_task.doc_md = """
## Silver Wallet PnL Task

Calculates FIFO-based profit/loss for each wallet with detailed cost averaging.

**What it does:**
- Processes transaction data per wallet
- Implements FIFO cost basis calculation
- Calculates realized/unrealized PnL, win rates, trading metrics
- Stores comprehensive performance data in silver_wallet_pnl table

**Configuration:** Uses silver_wallet_pnl section in pipeline_config.yaml
"""

smart_traders_task.doc_md = """
## Smart Traders Task (Gold Layer)

Final ranking and tier assignment for profitable traders.

**What it does:**
- Analyzes wallet performance from silver_wallet_pnl
- Assigns performance tiers (ELITE, STRONG, PROMISING, QUALIFIED)
- Calculates composite scores and rankings
- Stores final smart trader rankings in gold.smart_traders table

**Configuration:** Uses smart_traders section in pipeline_config.yaml
"""