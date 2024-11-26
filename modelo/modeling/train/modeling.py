import os
import sqlalchemy
from sklearn import tree
from sklearn import model_selection
from sklearn import metrics
from sklearn import preprocessing
import pandas as pd
from sqlalchemy import create_engine

MOD_TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
MODELING_DIR = os.path.dirname(MOD_TRAIN_DIR)
BASE_DIR = os.path.dirname(MODELING_DIR)
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR),'data')

MODEL_DIR = os.path.join( BASE_DIR, 'models')


engine = sqlalchemy.create_engine("sqlite:///" + os.path.join(DATA_DIR, 'abt.db'))

abt = pd.read_sql_table('abt', engine)

df_oot = abt[abt['dtref']==abt['dtref'].max()].copy()
df_oot.reset_index( drop=True, inplace=True )

df_abt = abt[abt['dtref']<abt['dtref'].max()].copy()

target = 'flag_model'
to_remove = ['dtref','cidade', 'estado', 'seller_id', 'dt_ult_venda', target]

features = df_abt.columns.tolist()

for f in to_remove:
    features.remove(f)
    
cat_features = df_abt[features].dtypes[ df_abt[features].dtypes == 'object' ].index.tolist()
num_features = list( set(features) - set(cat_features) )

# Separando entre treino e teste
X = df_abt[features] # matriz de features ou variáveis
y = df_abt[target] # Vetor da resposta ou target

X_train, X_test, y_train, y_test = model_selection.train_test_split(X,
                                                                     y,
                                                                     test_size=0.2,
                                                                     random_state=1992)


# Modelo
clf = tree.DecisionTreeClassifier()
clf.fit( X_train, y_train )

# Importancia das vairáveis
print(pd.Series( clf.feature_importances_, index = X_train.columns).sort_values(ascending=False)[:10])

# Analise na base de treino
y_train_pred = clf.predict( X_train )
acc_train = metrics.accuracy_score(y_train, y_train_pred)
print("Base Treino:", acc_train)

# Analise na base de teste
y_test_pred = clf.predict( X_test )
acc_test = metrics.accuracy_score(y_test, y_test_pred)
print("Base Teste:", acc_test )

# Analise na base de out of time
oot_pred = clf.predict( df_oot[features] )
acc_oot = metrics.accuracy_score(df_oot[target], oot_pred)
print("Base out of time:", acc_oot)

probs = clf.predict_proba( abt[features] )
abt['score_churn'] = clf.predict_proba( abt[features] )[:,1]
abt_score = abt[[ 'dtref','seller_id', 'score_churn']]


# Caminho para salvar o banco de dados SQLite
destino_db = "data/tb_churn_score.db"
# Criar conexão com o banco de dados SQLite (destino)
engine_destino = create_engine(f"sqlite:///{destino_db}")

abt_score.to_sql( 'tb_churn_score', engine_destino, index=False, if_exists='replace' )
