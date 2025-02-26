import sys ,os
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import config
import json
import subprocess
from hdfs import InsecureClient


def fetch(ti):
    try:
        response = requests.get(config.url,params=config.params)
        if response.status_code != 200 :
            raise ValueError(f'ingestion of data failed duo :{response.status_code}')
        
        data = response.json()
        ti.xcom_push(key='raw_data', value=data)

    except Exception as e :
        print(f'error{e}')


def ingestion(ti):
    data = ti.xcom_pull(key='raw_data')
    print(data)
    json_data = json.dumps(data)

    local_file = '/tmp/coingecko_raw.json'
    with open(local_file, 'w') as f:
        f.write(json_data)

    execution_date = ti.execution_date.strftime('%Y-%m-%d') 
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/coingecko_raw.json"

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file_path])



