import os
import sqlalchemy
from sklearn import tree
from sklearn import model_selection
from sklearn import metrics
from sklearn import preprocessing
import pandas as pd

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
to_remove = ['dtref', 'seller_id', 'dt_ult_venda', target]
features = df_abt.columns.tolist()
for f in to_remove:
    features.remove(f)
    
cat_features = df_abt[features].dtypes[ df_abt[features].dtypes == 'object' ].index.tolist()
num_features = list( set(features) - set(cat_features) )

# Separando entre treino e teste
X = df_abt[features] # matriz de features ou variáveis
y = df_abt[target] # Vetor da resposta ou target

X_train, X_test, y_train, y_teste = model_selection.train_test_split(X,
                                                                     y,
                                                                     test_size=0.2,
                                                                     random_state=1992)


X_train.reset_index(drop=True, inplace=True)
X_test.reset_index(drop=True, inplace=True)

onehot = preprocessing.OneHotEncoder(sparse_output=False, handle_unknown='ignore')
onehot.fit( X_train[cat_features] ) # Treinou o onehot!

onehot_df = pd.DataFrame( onehot.transform( X_train[cat_features] ),
                          columns=onehot.get_feature_names_out(cat_features) )

df_train = pd.concat([X_train[num_features], onehot_df], axis=1)
features_fit = df_train.columns.tolist()

# Modelo
clf = tree.DecisionTreeClassifier()
clf.fit( df_train[features_fit], y_train )

# Importancia das vairáveis
pd.Series( clf.feature_importances_, index = df_train.columns).sort_values(ascending=False)[:10]

# Analise na base de treino
y_train_pred = clf.predict( df_train )
acc_train = metrics.accuracy_score(y_train, y_train_pred)
print("Base Treino:", acc_train)

# Análise na base de teste
onehot_df_test = pd.DataFrame( onehot.transform( X_test[cat_features] ),
                               columns=onehot.get_feature_names_out(cat_features) )
df_predict = pd.concat( [X_test[num_features], onehot_df_test], axis=1 )
y_test_pred = clf.predict( df_predict )
acc_test = metrics.accuracy_score(y_test, y_test_pred)
print("Base Teste:", acc_test )

# Análise na base de oot
onehot_df_oot = pd.DataFrame( onehot.transform( df_oot[cat_features] ),
                               columns=onehot.get_feature_names_out(cat_features) )
df_oot_predict = pd.concat( [df_oot[num_features], onehot_df_oot], axis=1 )
oot_pred = clf.predict( df_oot_predict )
acc_oot = metrics.accuracy_score(df_oot[target], oot_pred)
print("Base out of time:", acc_oot)

# Fazendo o predict
df_abt_onehot = pd.DataFrame( onehot.transform( abt[cat_features] ),
                              columns=onehot.get_feature_names_out(cat_features) )

df_abt_predict = pd.concat( [abt[num_features], df_abt_onehot], axis=1 )

probs = clf.predict_proba( df_abt_predict )
abt['score_churn'] = clf.predict_proba( df_abt_predict )[:,1]
abt_score = abt[[ 'dt_ref','seller_id', 'score_churn']]
# abt_score.to_sql( 'tb_churn_score', engine, index=False, if_exists='replace' )
