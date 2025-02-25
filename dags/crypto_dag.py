from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta
import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.ingestion import *




default_args = {
    'owner': 'ali rahiqi',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('crypto', default_args=default_args, start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False) as dag:

    fetch=PythonOperator(task_id='fetch',python_callable=fetch,provide_context=True)
    ingestion=PythonOperator(task_id='ingestion',python_callable=ingestion,provide_context=True)





    fetch >> ingestion