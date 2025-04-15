from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import producer function
import sys
import os
sys.path.append('/opt/airflow')
from tiki.tiki_producer import crawl_tiki_product_list

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'tiki_producer',
    default_args=default_args,
    description='Crawl and produce Tiki product URLs to RabbitMQ queue',
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['tiki', 'crawler', 'producer'],
)

# Define task
def run_tiki_producer():
    """Run the Tiki product crawler and send URLs to queue"""
    print("Starting Tiki product crawler...")
    crawl_tiki_product_list()
    print("Finished Tiki product crawler")

# Create PythonOperator task
crawl_task = PythonOperator(
    task_id='crawl_tiki_products',
    python_callable=run_tiki_producer,
    dag=dag,
)

# Task dependencies (not needed for single task)
# crawl_task