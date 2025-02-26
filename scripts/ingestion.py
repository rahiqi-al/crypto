import sys ,os
import requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import config
import json
import subprocess
from hdfs import InsecureClient
import yfinance as yf
from datetime import datetime

def fetch():

    crypto_data = []

    for symbol in config.symbols:
        ticker = yf.Ticker(f"{symbol}-USD")
        history = ticker.history(period="1d", interval="1h")  

        if history.empty:
            continue

        for _, row in history.iterrows():
            record = {
                "id": symbol.lower(),
                "current_price": row["Close"],
                "total_volume": row["Volume"]
            }
            crypto_data.append(record)

    if not crypto_data:
        print("something went wrong in fetching data")
        return

    # Save data locally
    local_file = '/tmp/coingecko_raw.json'
    with open(local_file, 'w') as f:
        json.dump(crypto_data, f)

    # Upload to HDFS
    execution_date = datetime.now().strftime('%Y-%m-%d')
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/coingecko_raw.json"

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file_path])
    



