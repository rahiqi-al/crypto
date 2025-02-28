from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime , timedelta
from airflow.operators.bash import BashOperator
import os 
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.ingestion import *
from scripts.load_hbase import load




default_args = {
    'owner': 'ali rahiqi',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('crypto', default_args=default_args, start_date=datetime(2025,1,1), schedule_interval='@daily', catchup=False) as dag:

    fetch = PythonOperator(task_id='fetch',python_callable=fetch,provide_context=True)

    mapreduce = BashOperator(task_id='mapreduce',
        bash_command="""hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar -D mapreduce.job.num.map.tasks=1 -input /user/etudiant/crypto/raw/YYYY=2025/MM=02/DD=28/coingecko_raw.json -output /user/etudiant/crypto/processed/YYYY=2025/MM=02/DD=28 -mapper "/usr/bin/env python3 /home/ali/Desktop/crypto/scripts/mapper.py" -reducer "/usr/bin/env python3 /home/ali/Desktop/crypto/scripts/reducer.py" -file /home/ali/Desktop/crypto/scripts/mapper.py -file /home/ali/Desktop/crypto/scripts/reducer.py""")
  
    load_hbase = PythonOperator(task_id='load_hbase', python_callable=load, provide_context=True)




    fetch >> mapreduce >> load_hbase 