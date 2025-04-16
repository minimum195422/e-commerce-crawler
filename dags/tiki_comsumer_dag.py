from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

def run_tiki_consumer():
    os.system('python /opt/airflow/scripts/tiki_consumer.py')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hello_script_dag',
    default_args=default_args,
    schedule_interval='*/1 * * * *',  # Chạy mỗi 1 phút
    catchup=False
) as dag:
    
    run_hello_task = PythonOperator(
        task_id='run_tiki_consumer',
        python_callable=run_tiki_consumer,
        dag=dag
    )