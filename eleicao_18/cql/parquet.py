from pyspark.sql import SparkSession

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

# Criando as configurações do Spark Session
# spark = SparkSession.builder.appName("etl_politica").config("spark.executor.memory", '8g').config('spark.executor.cores', '3').config('spark.cores.max', '3').config("spark.driver.memory",'8g').config("spark.driver.maxResultSize",'8g').getOrCreate()

# Lendo os dados dos arquivos CSVs
df_parquet = session_spark.read.option("inferSchema", "true").option("encoding", "ISO-8859-1").option("sep", ";").option("header", "true").csv("../dataset/csv/*.csv")

# Transformando os dados em parquet
df_parquet.write.parquet("../dataset/parquet/tabela.parquet")