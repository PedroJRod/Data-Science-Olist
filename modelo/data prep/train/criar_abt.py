import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta  # Importar timedelta

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


def get_date_range() -> list[datetime]:
    """
    Solicita ao usuário um intervalo de datas e retorna uma lista de datas no formato datetime.
    """
    try:
        start_date = datetime.strptime(input("Digite a data inicial (YYYY-MM-DD): "), "%Y-%m-%d")
        end_date = datetime.strptime(input("Digite a data final (YYYY-MM-DD): "), "%Y-%m-%d")
        
        if start_date > end_date:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")
        
        # Gera a lista de meses no intervalo
        dates = [start_date]
        while dates[-1] < end_date:
            next_month = dates[-1].replace(day=1) + timedelta(days=31)
            dates.append(next_month.replace(day=1))
        
        return dates

    except ValueError as e:
        print(f"Erro: {e}")
        return []


def generate_query(dtref: str) -> str:
    """
    Gera a consulta SQL para um dado `dtref` no formato 'YYYY-MM-DD'.
    """
    return f"""
    SELECT TO_CHAR('{dtref}'::TIMESTAMP, 'YYYY-MM') AS dtref,
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


# Fluxo principal
def main():
    dates = get_date_range()
    if not dates:
        return

    for dt in dates:
        dtref = dt.strftime("%Y-%m-%d")
        query = generate_query(dtref)
        
        try:
            # Executar consulta e carregar os dados no DataFrame
            df = pd.read_sql(query, engine_origem)
            
            # Salvar os dados no banco de dados SQLite, consolidando na tabela 'abt'
            df.to_sql('abt', engine_destino, index=False, if_exists='append')
            print(f"Dados consolidados com sucesso para {dtref}.")

        except Exception as e:
            print(f"Erro ao processar {dtref}: {e}")


if __name__ == "__main__":
    main()