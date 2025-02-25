import logging
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

logging.basicConfig(level=logging.debug)



with DAG('crypto',start_date=datetime(2023,2,28),schedule_interval='@daily',catchup=False) as dag:

    ingestion=PythonOperator(task_id='ingestion',python_callable=ingestion)

    ingestion