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

def run_bronze_tokens(**context):
    """Wrapper function to import and run bronze tokens task."""
    from tasks.bronze_tokens import process_bronze_tokens
    return process_bronze_tokens(**context)

def run_silver_tokens(**context):
    """Wrapper function to import and run silver tokens task."""
    from tasks.silver_tokens import process_silver_tokens
    return process_silver_tokens(**context)

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

# Pipeline completion
pipeline_complete = DummyOperator(
    task_id='pipeline_complete',
    dag=dag
)

# Task Dependencies - Initialize DB → Bronze → Silver flow
start_pipeline >> init_db_task >> bronze_tokens_task >> silver_tokens_task >> pipeline_complete

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