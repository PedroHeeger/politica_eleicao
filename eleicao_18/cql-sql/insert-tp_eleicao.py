# Insert TP_Eleicao

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
df_tp_eleicao = df_geral.select("cd_tipo_eleicao", "nm_tipo_eleicao").distinct()
# df_tp_eleicao.show(20)

# Inserindo no banco MySQL com JDBC
df_tp_eleicao.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "tp_eleicao").save()



# # PANDAS
# # Transformando o Dataframe Spark em Pandas
# df_tp_eleicao = df_tp_eleicao.toPandas()
# print(df_tp_eleicao.head(5))

# # Inserindo o Dataframe na tabela criada.
# df_tp_eleicao.to_sql(name='tp_eleicao', con=conection, if_exists="append", index=False)