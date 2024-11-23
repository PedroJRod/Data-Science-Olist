import pandas as pd
import os
import sqlalchemy

MOD_TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
MODELING_DIR = os.path.dirname(MOD_TRAIN_DIR)
BASE_DIR = os.path.dirname(MODELING_DIR)
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR),'data')

engine = sqlalchemy.create_engine("sqlite:///" + os.path.join(DATA_DIR, 'abt.db'))

abt = pd.read_sql_table('abt', engine)

print(abt.head())

print(abt.groupby(['estado'])['flag_model'].mean())
