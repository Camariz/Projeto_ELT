import requests
import json
import os
from datetime import datetime
import pandas as pd
import pyarrow

API_URL = 'https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/'
API_KEY = "df128a7998d37d92f42212c87516a8ffdc4a69e9"

DATA_LAKE_PATH = "dataset"
RAW_PATH = os.path.join(DATA_LAKE_PATH, "raw")
BRONZE_PATH = os.path.join(DATA_LAKE_PATH, "bronze")

def extract(page:int):
    url = API_URL + f'?page={page}'
    resp = requests.get(url, headers = {
        "Authorization": f'Token {API_KEY}' 
    })
    data = resp.json()
    if 'message' in data:
        return False
    with open (f'{RAW_PATH}/gastos{page}.json', 'w', encoding = 'utf-8') as file:
        json.dump(data['results'], file, indent = 4)
    return True

def bronze(page: int =1):
	with open(f'{RAW_PATH}/gastos{page}.json', 'r') as file:
		gastos = json.load(file)
	dframe = pd.DataFrame(gastos)
	dframe.to_parquet(BRONZE_PATH,
		engine = 'pyarrow',
		partition_cols = [ 'ano', 'mes' ],
		index = True,
	)

def bronze_csv():
	dframe = pd.read_csv(f'{RAW_PATH}/gastos.csv')
	dframe.to_parquet(BRONZE_PATH,
		engine = 'pyarrow',
		partition_cols = [ 'ano', 'mes' ],
		index = True,
	)

extract(1)
bronze(1)


