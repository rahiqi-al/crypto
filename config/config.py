import yaml
from dotenv import load_dotenv
import os

load_dotenv()

class config:

    with open('/home/ali/Desktop/crypto/config/config.yml','r')as file:
        config_data=yaml.load(file,Loader=yaml.FullLoader)

    url = config_data['INGESTION']['URL']
    params = config_data['INGESTION']['PARAMS']



    api_key =os.getenv('')



config=config()


#print(config.params)

