from datetime import datetime
import pandas as pd
import requests, json, os
import pyarrow

API_URL = 'https://brasil.io/api/v1/dataset/gastos-diretos/gastos/data/'
API_KEY = "df128a7998d37d92f42212c87516a8ffdc4a69e9"

DATA_LAKE_PATH = "dataset"
RAW_PATH = os.path.join(DATA_LAKE_PATH, "raw")
BRONZE_PATH = os.path.join(DATA_LAKE_PATH, "bronze")
# Define o caminho para a camada 'Silver' do Data Lake.
# Dados limpos, padronizados e com tipos corrigidos.
SILVER_PATH = os.path.join(DATA_LAKE_PATH, "silver")

# Define o caminho para a camada 'Gold' do Data Lake.
# Dados agregados e prontos para consumo.
GOLD_PATH = os.path.join(DATA_LAKE_PATH, "gold")

def extract(page: int):
	url = API_URL + f'?page={page}'
	if os.path.exists(f'{RAW_PATH}/gastos{page}.json'):
		return True
	resp = requests.get(url, headers = {
		"Authorization": f'Token {API_KEY}'
	})
	data = resp.json()
	if 'message' in data:
		return False
	with open (f'{RAW_PATH}/gastos{page}.json', 'w', encoding = 'utf-8') as file:
		json.dump(data['results'], file, indent = 4)
	return True

def bronze(page: int=1):
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

# Funções de Configuração do Data Lake (Atualizada)
def setup_data_lake():
	"""Cria os diretórios 'raw', 'bronze', 'silver' e 'gold' se eles não existirem."""
	os.makedirs(RAW_PATH, exist_ok=True)
	os.makedirs(BRONZE_PATH, exist_ok=True)
	os.makedirs(SILVER_PATH, exist_ok=True) # NOVO
	os.makedirs(GOLD_PATH, exist_ok=True)   # NOVO
	print(f"Estrutura completa do Data Lake em '{DATA_LAKE_PATH}' garantida.")

# ----------------------------------------------------
# 1. FUNÇÕES DE QUALIDADE E WRANGLING
# ----------------------------------------------------

def run_data_quality_checks(dframe: pd.DataFrame, source_path: str):
	"""Executa testes de qualidade simples no DataFrame."""
	print("-> Executando Testes de Qualidade...")

	# Teste 1: Checagem de Nulos Críticos
	critical_nulls = dframe['valor'].isnull().sum()
	if critical_nulls > 0:
		print(f"   [ALERTA] {critical_nulls} valores nulos encontrados na coluna 'valor'.")

	# Teste 2: Checagem de Tipo de Dado Correto
	if dframe['valor'].dtype != np.float64:
		raise TypeError(f"   [ERRO FATAL] Coluna 'valor' não foi convertida para float64.")

	print("   Testes básicos de qualidade concluídos com sucesso.")

def clean_and_transform_silver():
	"""Lê dados do Bronze, aplica Data Wrangling e salva no Silver."""
	print("\n--- Processamento para Camada Silver (Limpeza e Padronização) ---")

	# 1. Leitura dos Dados Particionados (Toda a Camada Bronze)
	# O Pandas consegue ler o dataset particionado como um único DataFrame.
	print(f"-> Lendo dados particionados de: {BRONZE_PATH}")
	try: df_bronze = pd.read_parquet(BRONZE_PATH)
	except Exception as e:
		print(f"ERRO ao ler a camada Bronze. Certifique-se de que os arquivos existem. Erro: {e}")
		return False

	# 2. Data Wrangling (Limpeza e Transformação)

	# a) Análise Exploratória Simples (Identificação de Valor de Negócio)
	# Colunas-chave:
	# 'valor' (Quantia gasta) -> Métrica principal.
	# 'ano', 'mes', 'data_extracao' -> Dimensões temporais.
	# 'orgao', 'municipio' -> Dimensões geográficas/organizacionais.
	print("-> Aplicando Regras de Wrangling (Tipo e Nulos)...")

	# b) Tratamento de Nulos
	# Para simplicidade, vamos remover linhas onde a métrica principal ('valor') é nula.
	# Alternativa: preencher com 0 ou a média, mas remover é mais seguro para esta métrica.
	df_silver = df_bronze.dropna(subset=['valor'])

	# c) Conversão de Tipos de Dados
	# O Parquet geralmente infere bem, mas vamos garantir:
	df_silver['valor'] = df_silver['valor'].astype(np.float64)
	# A coluna 'data_extracao' é crucial para rastreabilidade (será convertida para datetime se necessário, mas não é a data do gasto)

	# d) Padronização e Enriquecimento Simples
	# Garante que as colunas 'ano' e 'mes' sejam inteiros (melhor para agregação)
	df_silver['ano'] = df_silver['ano'].astype(int)
	df_silver['mes'] = df_silver['mes'].astype(int)

	print(df_silver.columns)

	# 3. Testes de Qualidade (Validação)
	run_data_quality_checks(df_silver, BRONZE_PATH)

	# 4. Salvando no Silver (Mantendo o Particionamento)
	print(f"-> Salvando dados limpos no Silver: {SILVER_PATH}")
	df_silver.to_parquet(SILVER_PATH,
		engine='pyarrow', index=True,
		partition_cols=['ano', 'mes'],
	)
	print("--- Camada Silver concluída. Dados limpos e prontos para agregação. ---")

	return True


# Assume-se que SILVER_PATH e GOLD_PATH estão importados do arquivo de configurações principal

# ----------------------------------------------------
# 2. FUNÇÃO DE AGREGAÇÃO E MODELAGEM
# ----------------------------------------------------

def aggregate_gold():
	"""Lê dados do Silver, aplica agregação e salva no Gold."""
	print("\n--- Processamento para Camada Gold (Agregação e Modelagem) ---")

	# 1. Leitura dos Dados Limpos da Camada Silver
	print(f"-> Lendo dados limpos de: {SILVER_PATH}")
	try: df_silver = pd.read_parquet(SILVER_PATH, engine='pyarrow')
	except Exception as e:
		print(f"ERRO ao ler a camada Silver. Certifique-se de que os arquivos existem. Erro: {e}")
		return
	# 2. Agregação (Criação do Data Product)
	# Objetivo: Criar uma tabela mestre de Gastos Consolidados por Mês/Município/Órgão.
	print("-> Aplicando Agregação (Gasto Total Mensal por Órgão/Município)...")
	df_gold = df_silver

	# 3. Salvando no Gold (Pronto para Consumo)
	print(f"-> Salvando Data Mart agregado no Gold: {GOLD_PATH}")

	# Mantemos o particionamento em ano/mes para permitir consultas rápidas por período.
	df_gold.to_parquet(GOLD_PATH,
		engine='pyarrow', index=True,
		partition_cols=['ano', 'mes'],
	)
	print(f"--- Camada Gold concluída. Arquivos {len(df_gold)} linhas prontos para BI/ML. ---")
	return True

# ... (Funções extract, bronze, setup_data_lake) ...
# Importar as novas funções:
# from silver_transformer import clean_and_transform_silver
# from gold_aggregator import aggregate_gold


def main():
	"""Orquestra as etapas Raw (E), Bronze (L/T), Silver (T) e Gold (T)."""

	# 1. Garante que a estrutura do Data Lake existe (incluindo Silver e Gold)
	setup_data_lake()

	# 2. ETAPA RAW & BRONZE (Extração e Conversão, como feito antes)
	npages = 10
	print("\n--- 1. Início da Extração e Carga (RAW/BRONZE) ---")
	for p in range(1, npages+1):
		if extract(p): bronze(p)
		else: return

	# 3. ETAPA SILVER (Limpeza e Qualidade)
	# Lógica: Lê TUDO do Bronze, Limpa e Salva TUDO no Silver.
	if not clean_and_transform_silver(): return

	# 4. ETAPA GOLD (Agregação e Modelagem)
	# Lógica: Lê TUDO do Silver, Agrega e Salva no Gold.
	if not aggregate_gold(): return

	print("\n PIPELINE COMPLETO EXECUTADO COM SUCESSO!")

# Chama a função principal para iniciar o pipeline
# run_pipeline()

__name__ == '__main__' and main()
