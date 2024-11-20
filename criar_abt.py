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
destino_db = "data/abt.db"

# Configurações de conexão ao banco de dados PostgreSQL
postgres_url = f"postgresql+psycopg2://{user}:{password}@{host}/{db}"

# Conectar ao banco de dados PostgreSQL (origem)
engine_origem = create_engine(postgres_url)

# Criar conexão com o banco de dados SQLite (destino)
engine_destino = create_engine(f"sqlite:///{destino_db}")

# Função para obter o valor de dtref e substituí-lo na query
def get_query_with_dtref():
    dtref = input("Digite a data de referência (YYYY-MM-DD): ")
    
    # Valida o formato da data
    from datetime import datetime
    try:
        datetime.strptime(dtref, "%Y-%m-%d")
    except ValueError:
        print("Formato inválido! Tente novamente com o formato YYYY-MM-DD.")
        return None
    
    # Query SQL com a data inserida
    query = f"""
    SELECT {dtref} as dtref,
    t3.seller_city AS cidade,
    t3.seller_state AS estado,
    t1.*,
    CASE WHEN t2.seller_id IS NULL THEN 1 ELSE 0 END AS flag_model
    FROM (
        SELECT t2.seller_id,
        MAX(t1.order_approved_at) AS dt_ult_venda,
        SUM(t2.price) AS receita_total,
        COUNT(DISTINCT t2.order_id) AS qtde_vendas,
        SUM(t2.price) / COUNT(DISTINCT t2.order_id) AS avg_vl_venda,
        COUNT(t2.product_id) AS qtde_produto,
        COUNT(DISTINCT t2.product_id) AS qtde_prod_distinto,
        SUM(t2.price) / COUNT(t2.product_id) AS avg_vl_produto
        FROM db_olist.orders AS t1
        LEFT JOIN db_olist.order_items AS t2
        ON t1.order_id = t2.order_id
        WHERE t1.order_approved_at::TIMESTAMP BETWEEN '{dtref}'::TIMESTAMP
        AND ('{dtref}'::TIMESTAMP + INTERVAL '6 MONTHS')
        AND t1.order_status = 'delivered'
        GROUP BY t2.seller_id
    ) AS t1
    LEFT JOIN (
        SELECT DISTINCT t2.seller_id
        FROM db_olist.orders AS t1
        LEFT JOIN db_olist.order_items AS t2
        ON t1.order_id = t2.order_id
        WHERE t1.order_approved_at::TIMESTAMP BETWEEN ('{dtref}'::TIMESTAMP + INTERVAL '6 MONTHS')
        AND ('{dtref}'::TIMESTAMP + INTERVAL '9 MONTHS')
        AND t1.order_status = 'delivered'
    ) AS t2
    ON t1.seller_id = t2.seller_id
    LEFT JOIN db_olist.sellers AS t3
    ON t1.seller_id = t3.seller_id;
    """
    return query

# Uso
query = get_query_with_dtref()

# Executar consulta para o mês atual e carregar os dados no DataFrame
df = pd.read_sql(query, engine_origem)

# Salvar os dados no banco de dados SQLite, consolidando na tabela 'abt'
df.to_sql('abt', engine_destino, index=False, if_exists='append')  # Alterado para 'append'

print("Consolidação concluída com sucesso.")