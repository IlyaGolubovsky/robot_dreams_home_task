# airflow/dags/process_iris.py
"""
DAG for processing Iris dataset:
1. Run dbt transformation pipeline
2. Train ML model
3. Send email notification on success

Schedule: Daily at 1:00 AM Kyiv time (GMT+3)
Data range: 2025-04-22 to 2025-04-25
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

# Add python_scripts to path for importing train_model
sys.path.insert(0, os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'python_scripts'))

# Import the ML training function
from train_model import process_iris_data

# Configuration
ANALYTICS_DB = os.getenv('ANALYTICS_DB', 'analytics')
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
PROJECT_DIR = f"{AIRFLOW_HOME}/dags/dbt/homework"
PROFILES_DIR = f"{AIRFLOW_HOME}/dags/dbt"
PROFILE = 'homework'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['ilya.golubovsky@gmail.com'],
    'email_on_failure': False,  # Disabled - no SMTP configured
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    dag_id='process_iris',
    default_args=default_args,
    description='Process Iris dataset with dbt transformation and ML training',
    # Schedule at 1:00 AM Kyiv time (GMT+3) = 22:00 UTC previous day
    schedule_interval='0 22 * * *',  # Cron: minute=0, hour=22 UTC
    start_date=datetime(2025, 4, 22),
    end_date=datetime(2025, 4, 25),  # Process data for 3 days: 22, 23, 24, 25 April
    catchup=True,  # Enable catchup to process historical dates
    max_active_runs=1,
    tags=['iris', 'dbt', 'ml', 'homework'],
)

# Task 1: Run dbt transformation for Iris dataset using BashOperator
# This ensures proper profiles-dir is passed
DBT_PATH = '/home/airflow/.local/bin/dbt'

dbt_transform = BashOperator(
    task_id='dbt_transform_iris',
    dag=dag,
    bash_command=f'''
        cd {PROJECT_DIR} && \
        {DBT_PATH} run \
            --profiles-dir {PROFILES_DIR} \
            --project-dir {PROJECT_DIR} \
            --select stg_iris iris_processed \
            --vars '{{"data_date": "{{{{ ds }}}}"}}' 
    ''',
    env={
        'POSTGRES_ANALYTICS_HOST': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres_analytics'),
        'ETL_USER': os.getenv('ETL_USER', 'etl_user'),
        'ETL_PASSWORD': os.getenv('ETL_PASSWORD', 'etl_password'),
        'ANALYTICS_DB': ANALYTICS_DB,
    },
)

# Task 2: Train ML model
train_model = PythonOperator(
    task_id='train_ml_model',
    dag=dag,
    python_callable=process_iris_data,
    provide_context=True,
)

# Task 3: Send success notification (using BashOperator since SMTP not configured)
send_success_notification = BashOperator(
    task_id='send_success_notification',
    dag=dag,
    bash_command='echo "Pipeline completed successfully for {{ ds }}"',
)

# Define task dependencies
dbt_transform >> train_model >> send_success_notification