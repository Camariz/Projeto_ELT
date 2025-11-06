import requests
import json
import os
from datetime import datetime
import pandas as pd
import pyarrow

# Configurações de Acesso à API

# Define a URL base do endpoint da API de onde os dados serão extraídos (Origem de Dados).
API_URL = 'https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/'
# Define a chave de autenticação (Token) necessária para acessar a API, garantindo a segurança.
API_KEY = "df128a7998d37d92f42212c87516a8ffdc4a69e9"

# Configurações do Data Lake
# Define o diretório raiz do Data Lake (camada de armazenamento central).
DATA_LAKE_PATH = "dataset"
# Define o caminho para a camada 'Raw' (Bruta) do Data Lake.
# Nesta camada, os dados são armazenados exatamente como vieram da fonte (sem modificações).
RAW_PATH = os.path.join(DATA_LAKE_PATH, "raw")
# Define o caminho para a camada 'Bronze' do Data Lake.
# Nesta camada, os dados brutos são convertidos para um formato otimizado (e.g., Parquet), mas a estrutura é mantida.
BRONZE_PATH = os.path.join(DATA_LAKE_PATH, "bronze")

# Funções do Pipeline de Dados

# Função de Extração (E) dos dados da API.
# O parâmetro 'page' permite a paginação da API para extrair grandes volumes de dados.
def extract(page:int):
	# Constrói a URL completa para a requisição, incluindo o número da página.
    url = API_URL + f'?page={page}'
	# Realiza a requisição GET à API, incluindo o cabeçalho de autorização com a chave (API_KEY).
    resp = requests.get(url, headers = {
        "Authorization": f'Token {API_KEY}' 
    })
	# Converte a resposta HTTP (JSON) em um dicionário Python.
    data = resp.json()
	# Checa se a resposta contém uma chave 'message', indicando um erro ou fim de dados.
    if 'message' in data:
		# Retorna False para sinalizar que a extração falhou ou não há mais páginas.
        return False
	# Abre o arquivo na camada 'Raw' para escrita. O nome do arquivo inclui o número da página.
    # O modo 'w' (write) sobrescreve o arquivo se ele já existir.
    with open (f'{RAW_PATH}/gastos{page}.json', 'w', encoding = 'utf-8') as file:
		# Salva apenas a lista de resultados ('results') do JSON da API no arquivo.
        # Usa 'indent = 4' para formatar o JSON, tornando-o legível (útil para inspeção na camada Raw).
        json.dump(data['results'], file, indent = 4)
	# Retorna True para sinalizar que a extração foi bem-sucedida.
    return True
# Função de Carga/Processamento para a Camada Bronze (L/T - Load/Transformação Leve).
# Esta função converte os dados JSON brutos em um formato otimizado (Parquet).
def bronze(page: int =1):
	# Abre o arquivo JSON extraído da camada 'Raw' correspondente à página.
	with open(f'{RAW_PATH}/gastos{page}.json', 'r') as file:
		# Carrega o conteúdo do arquivo JSON para uma variável Python.
		gastos = json.load(file)
	# Cria um DataFrame Pandas a partir dos dados carregados.
	dframe = pd.DataFrame(gastos)
	# Escreve o DataFrame para o formato Parquet na camada 'Bronze'.
	dframe.to_parquet(BRONZE_PATH,
		# Especifica 'pyarrow' como o motor de escrita (otimizado para Parquet).
		engine = 'pyarrow',
		# Define a partição dos dados pelos campos 'ano' e 'mes'.
		# Particionamento é crucial em Data Lakes para otimizar consultas e reduzir custos,
		# pois permite que ferramentas de query (como Spark ou Athena) leiam apenas os arquivos relevantes.
		partition_cols = [ 'ano', 'mes' ],
		# Mantém o índice do DataFrame como parte dos dados escritos (opcional, dependendo do caso de uso).
		index = True,
	)
# Função alternativa de Carga/Processamento para a Camada Bronze, mas partindo de um arquivo CSV.
# Esta função seria usada se a fonte fosse um arquivo estático em vez de uma API paginada.
def bronze_csv():
	# Lê o arquivo CSV bruto (pressupõe-se que 'gastos.csv' esteja na camada Raw).
	dframe = pd.read_csv(f'{RAW_PATH}/gastos.csv')
	# Escreve o DataFrame para o formato Parquet na camada 'Bronze'.
	dframe.to_parquet(BRONZE_PATH,
		# Usa o mesmo motor e estratégia de particionamento.
		engine = 'pyarrow',
		partition_cols = [ 'ano', 'mes' ],
		index = True,
	)

# Execução do Pipeline

# Chama a função de extração para a primeira página.
# Este é o início do pipeline ELT/ETL.
extract(1)
# Chama a função de conversão/carga para a camada Bronze para os dados da primeira página.
# Otimiza o formato para armazenamento eficiente.
bronze(1)


