# Insert DS_Eleicao

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
df_ds_eleicao = df_geral.select("cd_eleicao", "ds_eleicao", "dt_eleicao").distinct()
# df_eleicao.show(20)

# Inserindo no banco MySQL com JDBC
df_ds_eleicao.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "ds_eleicao").save()






# PANDAS
# Transformando o Dataframe Spark em Pandas
# df_eleicao = df_eleicao.toPandas()
# print(df_eleicao.head(5))

# # Inserindo o Dataframe na tabela criada.
# df_eleicao.to_sql(name='ds_eleicao', con=conection, if_exists="append", index=False)