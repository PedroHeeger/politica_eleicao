# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando variável de ambiente
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando a configuração do spark
sys.path.insert(0, f'{path_eleicao_18}')
from con_spark import *

# Deleta uma tabela determinada
session.execute("drop table resultado_eleicao")