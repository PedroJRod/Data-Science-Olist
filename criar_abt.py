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
select t2.seller_id,
sum(t2.price) as receita_total,
count(distinct t2.order_id) as qtde_vendas,
sum(t2.price)/count(distinct t2.order_id) as avg_vl_venda,
count(t2.product_id) as qtde_produto,
count(distinct t2.product_id) as qtde_prod_distinto,
sum(t2.price)/count(t2.product_id) as avg_vl_produto 
from db_olist.orders as t1
left join db_olist.order_items as t2
on t1.order_id = t2.order_id 
where t1.order_approved_at between '2016-10-01'
and '2017-04-01'
and t1.order_status ='delivered'
group by t2.seller_id
"""

# Carregar o resultado da consulta em um DataFrame
df = pd.read_sql(query, engine_origem)

# Salvar o DataFrame no banco de dados SQLite local com o nome 'abt'
df.to_sql('abt', engine_destino, index=False, if_exists='replace')

print("Dados salvos no banco de dados SQLite local com sucesso.")