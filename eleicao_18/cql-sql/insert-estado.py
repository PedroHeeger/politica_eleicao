# Insert Estado

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
df_estado = df_geral.select("sg_uf", "sg_ue", "nm_ue").distinct()
# df_estado.show(5)

# Inserindo no banco MySQL com JDBC
df_estado.write.format("jdbc") \
    .option("url", f"jdbc:mysql://mysql-app/{db}?user=root&password=root") \
    .mode("append") \
	.option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "estado").save()





# # PANDAS
# # Transformando o Dataframe Spark em Pandas
# df_estado = df_estado.toPandas()
# print(df_estado.head(5))

# # Inserindo o Dataframe na tabela criada.
# df_estado.to_sql(name='estado', con=conection, if_exists="append", index=False)