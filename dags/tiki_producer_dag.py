from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def run_tiki_producer():
    os.system('python /opt/airflow/scripts/tiki_producer.py')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'run_tiki_producer',
    default_args=default_args,
    schedule_interval='*/5 * * * *', # 5 phút chạy một lần
    catchup=False
) as dag:
    
    run_tiki_producer_task = PythonOperator(
        task_id='run_tiki_producer',
        python_callable=run_tiki_producer,
        dag=dag
    )