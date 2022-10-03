# Insert Coligação

# Importações sistema
import sys
import os
from dotenv import load_dotenv

# Carregando as variáveis de ambiente
load_dotenv()

# Buscando cada variável
path_eleicao_18 = os.environ['path_eleicao_18']

# Importando o Dataframe Spark com os dado do banco Cassandra
sys.path.insert(0, f'{end}cql')
from selection import df_geral

# Importando as conexões
sys.path.insert(0, f'{path_eleicao_18}')
# from con_spark import session_spark
from con_sqlalch import *

# Outras Importações
import pandas as pd


# SPARK
# Selecionando as colunas necessárias
df_coligacao = df_geral.select("sq_coligacao", "tp_agremiacao", "nm_coligacao", "ds_composicao_coligacao").distinct()
# df_coligacao.show(5)

# Inserindo no banco MySQL com JDBC
df_coligacao.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "coligacao").save()








# PANDAS
# Transformando o Dataframe Spark em Pandas
# df_coligacao = df_coligacao.toPandas()
# print(df_coligacao.head(5))

# # Tratando os dados iguais de uma coluna para não influenciar na chave primária
# df_coligacao = df_coligacao.mask(df_coligacao['ds_composicao_coligacao']=='MDB / PP / PRB / PR / PSC / PTB / PHS / PMB / PODE..',"MDB / PP / PRB / PR / PSC / PTB / PHS / PODE / PMB..")

# # Inserindo o Dataframe na tabela criada.
# df_coligacao.to_sql(name='coligacao', con=conection, if_exists="append", index=False)