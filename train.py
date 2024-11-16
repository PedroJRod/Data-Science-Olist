import pandas as pd
import numpy as np
import sqlalchemy
import os
from sklearn import tree
from sklearn import metrics

np.random.seed(144)

PAI_DIR = os.path.dirname( os.path.abspath( __file__ ) )
DATA_DIR = os.path.join(PAI_DIR, 'data')

# Caminho do banco de dados
db_path = os.path.join(DATA_DIR, 'abt.db')

# Conexão com o banco de dados
engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")

# Nome da tabela que deseja carregar
table_name = "abt"

# Carregando a tabela em um DataFrame
df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

# Exibindo as primeiras linhas para verificar
print(df.head())

columns = df.columns.tolist()

# Variáveis para serem removidas
to_remove = ['seller_id', 'cidade','estado']

# Variável alvo, target, resposta
target = 'flag_model'

# Remove de fato as variáveis
for i in to_remove + [target]:
    columns.remove(i)

# Defini tipos de variáveis
cat_features = df[ columns ].dtypes[ df[ columns ].dtypes == 'object'].index.tolist()
num_features = list( set( columns ) - set( cat_features) )

# Treinando o algoritmo de arvore de decisao
clf = tree.DecisionTreeClassifier(max_depth=10)
clf.fit( df[num_features], df[target] )

y_pred = clf.predict( df[num_features] )
y_prob = clf.predict_proba( df[num_features] )

cf = metrics.confusion_matrix( df[target], y_pred )
print(cf)

features_importance = pd.Series( clf.feature_importances_, index=num_features)
features_importance.sort_values( ascending=False )[:20]

print(features_importance)