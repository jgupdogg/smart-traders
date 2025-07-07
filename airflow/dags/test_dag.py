"""
Simple test DAG to verify Airflow functionality.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

def test_function(**context):
    """Simple test function."""
    print("Test DAG is working!")
    return "success"

# Default arguments
default_args = {
    'owner': 'solana-smart-traders',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='Simple test DAG',
    schedule_interval=None,  # Manual trigger only
    tags=['test']
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

test_task = PythonOperator(
    task_id='test_task',
    python_callable=test_function,
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

start >> test_task >> end