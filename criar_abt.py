import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Recupera as credenciais
user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
db = os.getenv("POSTGRES_DB")

# Caminho para salvar o banco de dados SQLite
destino_db = "abt.db"

# Configurações de conexão ao banco de dados PostgreSQL
postgres_url = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"

# Conectar ao banco de dados PostgreSQL (origem)
engine_origem = create_engine(postgres_url)

# Criar conexão com o banco de dados SQLite (destino)
engine_destino = create_engine(f"sqlite:///{destino_db}")

# Consultar dados das tabelas com JOIN usando o ClientID
query = """
SELECT t1.*, t2."DataUltimaTransacao" 
FROM db_churn.dim_clientes AS t1
JOIN db_churn.fato_churn AS t2 ON t1."ClientId" = t2."ClientId"
"""

# Carregar o resultado da consulta em um DataFrame
df = pd.read_sql(query, engine_origem)

# Salvar o DataFrame no banco de dados SQLite local com o nome 'abt'
df.to_sql('abt', engine_destino, index=False, if_exists='replace')

print("Dados salvos no banco de dados SQLite local com sucesso.")