import pandas as pd
import os
import sqlalchemy
from sklearn import tree
from sklearn import model_selection
from sklearn import metrics
from sklearn import preprocessing

MOD_TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
MODELING_DIR = os.path.dirname(MOD_TRAIN_DIR)
BASE_DIR = os.path.dirname(MODELING_DIR)
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR),'data')

engine = sqlalchemy.create_engine("sqlite:///" + os.path.join(DATA_DIR, 'abt.db'))

abt = pd.read_sql_table('abt', engine)

df_oot = abt[abt['dtref']==abt['dtref'].max()].copy()

df_abt = abt[abt['dtref']<abt['dtref'].max()].copy()

target = 'flag_model'
to_remove = ['dtref', 'cidade', 'estado', 'seller_id', 'dt_ult_venda', target]
features = df_abt.columns.tolist()
for f in to_remove:
    features.remove(f)

X_train, X_test, y_train, y_teste = model_selection.train_test_split(df_abt[features],
                                                                     df_abt[target],
                                                                     test_size=0.2,
                                                                     random_state=1992)


clf = tree.DecisionTreeClassifier()
clf.fit(X_train,y_train)

y_train_pred = clf.predict(X_train)
y_train_prob = clf.predict_proba(X_train)  
print('Acurácia Train: ', metrics.accuracy_score(y_train, y_train_pred)) 

y_test_pred = clf.predict(X_test)
y_test_prob = clf.predict_proba(X_test) 
print('Acurácia Teste: ', metrics.accuracy_score(y_teste, y_test_pred)) 

y_oot_pred = clf.predict(df_oot[features])
y_oot_prob = clf.predict_proba(df_oot[features])
print('Acurácia Out of Time: ', metrics.accuracy_score(df_oot[target], y_oot_pred))

print(df_oot[features])
