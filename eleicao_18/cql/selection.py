# Selecionando os dados da tabela geral do Cassandra e construindo um Dataframe para ser usado nos Inserts da pasta cql-sql

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando variável de ambiente
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando a variável que contem o caminho das pastas
sys.path.insert(0, f'{path_eleicao_18}')
from con_spark import *

# Criando um Dataframe a partir da tabela geral do Cassandra
df_geral = sqlContext.read.format("org.apache.spark.sql.cassandra").options(table="resultado_eleicao_18", keyspace="politica").load()

# spark.jars
# spark.driver.extraClassPath

# Verificar total de colunas e linhas do DF
# print(df.shape)

# Exibir os dados do DF
# print(df.head(2))

# Verificar a quantidade de linhas por coluna
# print(df.count())
# print(df.columns)

# Verificar os tipos de cada coluna
# print(df.dtypes)