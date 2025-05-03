from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 27),
    'retries': 1,
}

# Define the DAG
with DAG(
    'books_etl_pipeline',
    default_args=default_args,
    description='Full Books ELT: Bronze → Silver → Gold → Load',
    schedule_interval='@daily',   # or '@hourly' or '0 1 * * *'
    catchup=False,
    tags=['books', 'ELT'],
) as dag:

    PROJECT = "/opt/project"          # same base used in volumes

    bronze_task = BashOperator(
        task_id='run_bronze',
        bash_command=f'python {PROJECT}/bronze.py',
    )

    silver_task = BashOperator(
        task_id='run_silver',
        bash_command=f'python {PROJECT}/silver.py',
    )

    gold_task = BashOperator(
        task_id='run_gold',
        bash_command=f'python {PROJECT}/gold.py',
    )

    load_task = BashOperator(
        task_id='run_load',
        bash_command=f'python {PROJECT}/load.py',
    )
