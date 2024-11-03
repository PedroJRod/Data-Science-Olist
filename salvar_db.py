import os
import pandas as pd
from sqlalchemy import create_engine

# Configurações de conexão ao banco de dados PostgreSQL
postgres_url = "postgresql+psycopg2://aluno:PreditivaAlunoGVD@preditiva-postgres-1.cmaonsvyciyn.us-east-1.rds.amazonaws.com/alunos"

# Conectar ao banco de dados PostgreSQL (origem)
engine_origem = create_engine(postgres_url)

# Criar conexão com o banco de dados SQLite (destino)
engine_destino = create_engine(f"sqlite:///{destino_db}")

# Lista de tabelas para exportar
tabelas = ["db_churn.dim_clientes", "db_churn.fato_churn"]

# Caminho do banco de dados SQLite de destino
destino_db = r"C:\Users\Acer\Desktop\TESTE\meu_banco.db"

# Função para copiar a tabela para o banco de dados SQLite de destino
def copiar_tabela(table_name):
    # Ler a tabela do banco de dados PostgreSQL
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine_origem)
    # Exportar para o banco de dados SQLite de destino
    df.to_sql(table_name, engine_destino, index=False, if_exists="replace")
    print(f"Tabela '{table_name}' exportada com sucesso para {destino_db}")

# Exportar cada tabela
for tabela in tabelas:
    copiar_tabela(tabela)