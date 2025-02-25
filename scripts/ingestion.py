import logging
import requests
from .. config.config import ali


ali=123
def ingestion():
    try:
        response = requests.get(url)
        if response.status_code != 200 :
            raise ValueError(f'ingestion of data failed duo :{response.status_code}')
        

        



    except Exception as e :
        logging.errerr
