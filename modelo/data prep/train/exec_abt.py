import os
import sys

TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PREP_DIR = os.path.dirname(TRAIN_DIR)
BASE_DIR = os.path.dirname(DATA_PREP_DIR)
DATA_DIR = os.path.join(os.path.dirname(BASE_DIR),'data')


#print(TRAIN_DIR)
#print(DATA_PREP_DIR)
#print(BASE_DIR)
#print(DATA_DIR)

# Adiciona DATA_PREP_DIR ao sys.path para permitir a importação
sys.path.append(DATA_PREP_DIR)

# Importa e executa criar_abt.py
try:
    import criar_abt
    criar_abt.main()
    print("Execução do arquivo criar_abt.py concluída com sucesso!")
except Exception as e:
    print(f"Erro ao executar criar_abt.py: {e}")